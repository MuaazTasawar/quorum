//! Shared harness for multi-node integration tests.
//!
//! Every node is a real `Cluster` task over a real on-disk `Persistence`
//! directory. Only the network is simulated: in-process channels stand in for
//! TCP (the transport crate has its own tests for framing and sockets), which
//! is what makes it possible to cut and restore links, and to crash and restart
//! a node over the same storage directory.
//!
//! Each test binary uses a different subset of this module.
#![allow(dead_code)]

use node::cluster::{
    ClientCommand, ClientCommandResult, ClientQuery, Cluster, ClusterHandle, ClusterSnapshot,
    QueryResult,
};
use node::config::{NodeConfig, ReadMode};
use node::metrics_api::{router, AppState};
use raft_core::{NodeId, RpcMessage};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage::{Command, Persistence};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

type Inbox = mpsc::UnboundedSender<(NodeId, RpcMessage)>;

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// The simulated network: which links are cut, and where each node's inbox is.
/// Inboxes are looked up at delivery time so a restarted node, which gets a
/// brand-new inbox, receives messages sent by links created before it restarted.
#[derive(Default)]
pub struct Network {
    blocked: Mutex<HashSet<(NodeId, NodeId)>>,
    inboxes: Mutex<HashMap<NodeId, Inbox>>,
}

impl Network {
    fn is_blocked(&self, from: NodeId, to: NodeId) -> bool {
        self.blocked.lock().unwrap().contains(&(from, to))
    }

    fn inbox(&self, to: NodeId) -> Option<Inbox> {
        self.inboxes.lock().unwrap().get(&to).cloned()
    }

    fn register(&self, id: NodeId, inbox: Inbox) {
        self.inboxes.lock().unwrap().insert(id, inbox);
    }

    fn unregister(&self, id: NodeId) {
        self.inboxes.lock().unwrap().remove(&id);
    }
}

/// One direction of one link. Messages to a blocked or dead destination are
/// silently dropped, which is what a real partition or crash looks like.
fn spawn_link(
    network: Arc<Network>,
    from: NodeId,
    to: NodeId,
) -> mpsc::UnboundedSender<RpcMessage> {
    let (tx, mut rx) = mpsc::unbounded_channel::<RpcMessage>();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if network.is_blocked(from, to) {
                continue;
            }
            if let Some(inbox) = network.inbox(to) {
                let _ = inbox.send((from, msg));
            }
        }
    });
    tx
}

struct TestNode {
    handle: ClusterHandle,
    join: JoinHandle<()>,
}

pub struct TestCluster {
    ids: Vec<NodeId>,
    network: Arc<Network>,
    nodes: BTreeMap<NodeId, TestNode>,
    base_dir: PathBuf,
    read_mode: ReadMode,
}

impl TestCluster {
    /// Starts every node in `ids`, fully meshed, each over its own fresh
    /// storage directory. Must be called from inside a tokio runtime.
    pub fn start(ids: &[NodeId]) -> Self {
        Self::start_with_read_mode(ids, ReadMode::Linearizable)
    }

    /// Like `start`, but with an explicit read mode for every node.
    pub fn start_with_read_mode(ids: &[NodeId], read_mode: ReadMode) -> Self {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let n = DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
        let base_dir = std::env::temp_dir().join(format!("quorum-itest-{nanos}-{n}"));
        std::fs::create_dir_all(&base_dir).expect("test dir should be creatable");

        let mut cluster = Self {
            ids: ids.to_vec(),
            network: Arc::new(Network::default()),
            nodes: BTreeMap::new(),
            base_dir,
            read_mode,
        };
        for &id in ids {
            cluster.launch(id);
        }
        cluster
    }

    fn node_dir(&self, id: NodeId) -> PathBuf {
        self.base_dir.join(format!("node{id}"))
    }

    /// Brings node `id` up over its storage directory, recovering whatever is
    /// already there. Used for both first start and restart.
    fn launch(&mut self, id: NodeId) {
        let (inbox_tx, inbox_rx) = mpsc::unbounded_channel();
        self.network.register(id, inbox_tx);

        let mut peer_outboxes = HashMap::new();
        for &peer in &self.ids {
            if peer != id {
                peer_outboxes.insert(peer, spawn_link(self.network.clone(), id, peer));
            }
        }

        let config = NodeConfig {
            id,
            listen_addr: "127.0.0.1:0".to_string(),
            metrics_addr: "127.0.0.1:0".to_string(),
            peers: peer_outboxes.keys().map(|&pid| (pid, String::new())).collect(),
            storage_dir: self.node_dir(id),
            read_mode: self.read_mode,
        };

        let recovered = Persistence::open(self.node_dir(id)).expect("storage should open");
        let (cluster, handle) = Cluster::new(config, recovered, peer_outboxes, inbox_rx);
        let join = tokio::spawn(cluster.run());
        self.nodes.insert(id, TestNode { handle, join });
    }

    // ---- failures -----------------------------------------------------------

    /// Hard-kills a node: no shutdown, no goodbye. Waits for the task to be fully
    /// gone so its files are closed before a restart reopens them.
    pub async fn crash(&mut self, id: NodeId) {
        let node = self.nodes.remove(&id).expect("node should be running");
        node.join.abort();
        let _ = node.join.await;
        self.network.unregister(id);
    }

    /// Restarts a crashed node over the same storage directory.
    pub async fn restart(&mut self, id: NodeId) {
        assert!(!self.nodes.contains_key(&id), "node {id} is still running");
        self.launch(id);
    }

    /// Cuts every link between `id` and every other node, in both directions.
    pub fn isolate(&self, id: NodeId) {
        let mut blocked = self.network.blocked.lock().unwrap();
        for &other in &self.ids {
            if other != id {
                blocked.insert((id, other));
                blocked.insert((other, id));
            }
        }
    }

    /// Cuts every link between the two groups, in both directions.
    pub fn partition(&self, group_a: &[NodeId], group_b: &[NodeId]) {
        let mut blocked = self.network.blocked.lock().unwrap();
        for &a in group_a {
            for &b in group_b {
                blocked.insert((a, b));
                blocked.insert((b, a));
            }
        }
    }

    /// Cuts ONE direction of one link: messages from `from` to `to` are dropped,
    /// while `to` can still reach `from`. Models the awkward real-world failure
    /// where a follower stops hearing its leader but can still talk to everyone.
    pub fn cut_link(&self, from: NodeId, to: NodeId) {
        self.network.blocked.lock().unwrap().insert((from, to));
    }

    /// Restores every cut link.
    pub fn heal(&self) {
        self.network.blocked.lock().unwrap().clear();
    }

    // ---- observation --------------------------------------------------------

    pub fn live_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn snapshot(&self, id: NodeId) -> ClusterSnapshot {
        self.nodes[&id].handle.snapshot_rx.borrow().clone()
    }

    pub fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.handle.snapshot_rx.borrow().role == "Leader")
            .map(|(&id, _)| id)
            .collect()
    }

    /// Polls until `condition` holds, or panics with `what` after `timeout`.
    pub async fn wait_until(
        &self,
        timeout: Duration,
        what: &str,
        condition: impl Fn(&TestCluster) -> bool,
    ) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if condition(self) {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("timed out after {timeout:?} waiting for: {what}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits until one of `candidates` reports itself Leader and returns it.
    pub async fn wait_for_leader_among(&self, candidates: &[NodeId], timeout: Duration) -> NodeId {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for &id in candidates {
                if let Some(node) = self.nodes.get(&id) {
                    if node.handle.snapshot_rx.borrow().role == "Leader" {
                        return id;
                    }
                }
            }
            if tokio::time::Instant::now() >= deadline {
                panic!("no leader among {candidates:?} within {timeout:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn wait_for_leader(&self, timeout: Duration) -> NodeId {
        let live = self.live_ids();
        self.wait_for_leader_among(&live, timeout).await
    }

    // ---- client operations --------------------------------------------------

    /// Serves node `id`'s real HTTP API (the same router the node binary uses) on
    /// a free local port and returns its address. Lets a test exercise the whole
    /// path: HTTP request -> cluster -> consensus -> HTTP response.
    pub async fn serve_http(&self, id: NodeId, request_timeout: Duration) -> SocketAddr {
        let handle = &self.nodes[&id].handle;
        let state = AppState {
            command_tx: handle.command_tx.clone(),
            query_tx: handle.query_tx.clone(),
            snapshot_rx: handle.snapshot_rx.clone(),
            request_timeout,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("local port should be bindable");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router(state)).await;
        });
        addr
    }

    pub async fn put(&self, id: NodeId, key: &str, value: &str) -> ClientCommandResult {
        let (respond_to, receiver) = oneshot::channel();
        self.nodes[&id]
            .handle
            .command_tx
            .send(ClientCommand {
                command: Command::Put { key: key.to_string(), value: value.as_bytes().to_vec() },
                respond_to,
            })
            .expect("command channel should be open");
        receiver.await.expect("cluster should respond")
    }

    pub async fn get(&self, id: NodeId, key: &str) -> QueryResult {
        let (respond_to, receiver) = oneshot::channel();
        self.nodes[&id]
            .handle
            .query_tx
            .send(ClientQuery { key: key.to_string(), respond_to })
            .expect("query channel should be open");
        receiver.await.expect("cluster should respond")
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in self.nodes.values() {
            node.join.abort();
        }
        // Best effort: on Windows an aborted task may still hold its files open
        // for a moment, in which case the directory is simply left in %TEMP%.
        let _ = std::fs::remove_dir_all(&self.base_dir);
    }
}
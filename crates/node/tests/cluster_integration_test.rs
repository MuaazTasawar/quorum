//! Proves the actual point of this project: a real multi-node Raft cluster,
//! wired with in-process channels standing in for the TCP transport layer
//! (the transport crate's framing/socket logic is already covered by its
//! own tests in Phase 5 - this test is about the CONSENSUS behavior, not
//! re-testing byte framing). Each node here is a full, real `Cluster` task
//! running the actual election/replication/commit logic against a real
//! temp-file WAL - nothing about raft-core or storage is mocked.

use node::cluster::{
    ClientCommand, ClientCommandResult, ClientQuery, Cluster, ClusterHandle, QueryResult,
};
use node::config::NodeConfig;
use raft_core::{Log, NodeId, PersistentState, RpcMessage};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use storage::{Command, KvStore};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Forwards every message sent on the returned channel into `to_inbox`,
/// tagged with `from_id`. Stands in for `network::spawn_outbound` without
/// touching real sockets - this is deliberately NOT re-testing transport
/// (that's Phase 5's job), only exercising Cluster's own logic.
fn spawn_link(
    from_id: NodeId,
    to_inbox: mpsc::UnboundedSender<(NodeId, RpcMessage)>,
) -> mpsc::UnboundedSender<RpcMessage> {
    let (tx, mut rx) = mpsc::unbounded_channel::<RpcMessage>();
    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let _ = to_inbox.send((from_id, msg));
        }
    });
    tx
}

fn temp_wal_path(node_id: NodeId) -> std::path::PathBuf {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("quorum-itest-node{node_id}-{nanos}.wal"))
}

/// Builds a fully-wired N-node cluster (every node connected to every other
/// node, matching a real deployment's full mesh) and spawns each node's
/// `Cluster::run()` as its own tokio task. Returns each node's handle
/// (for submitting client commands/queries) and its JoinHandle (so a test
/// can `.abort()` it to simulate that node's process dying outright).
async fn spin_up_cluster(node_ids: &[NodeId]) -> HashMap<NodeId, (ClusterHandle, JoinHandle<()>)> {
    let mut inboxes = HashMap::new();
    let mut inbox_rxs = HashMap::new();
    for &id in node_ids {
        let (tx, rx) = mpsc::unbounded_channel();
        inboxes.insert(id, tx);
        inbox_rxs.insert(id, rx);
    }

    let mut result = HashMap::new();
    for &id in node_ids {
        let mut peer_outboxes = HashMap::new();
        for &peer_id in node_ids {
            if peer_id != id {
                peer_outboxes.insert(peer_id, spawn_link(id, inboxes[&peer_id].clone()));
            }
        }

        let config = NodeConfig {
            id,
            listen_addr: "0.0.0.0:0".to_string(), // unused - no real listener in this test
            metrics_addr: "0.0.0.0:0".to_string(),
            peers: peer_outboxes.keys().map(|&pid| (pid, String::new())).collect(),
            storage_dir: std::env::temp_dir(),
        };

        let wal = storage::Wal::open(temp_wal_path(id)).expect("temp WAL should open");
        let inbox_rx = inbox_rxs.remove(&id).unwrap();

        let (cluster, handle) = Cluster::new(
            config,
            wal,
            Log::new(),
            KvStore::new(),
            PersistentState::default(),
            peer_outboxes,
            inbox_rx,
        );
        let join_handle = tokio::spawn(cluster.run());
        result.insert(id, (handle, join_handle));
    }
    result
}

/// Polls every node's snapshot until exactly one reports itself as Leader,
/// or panics after `timeout` - a real election should settle well within a
/// couple hundred ms given the default 150-300ms randomized timeout range.
async fn wait_for_leader(
    handles: &HashMap<NodeId, (ClusterHandle, JoinHandle<()>)>,
    timeout: Duration,
) -> NodeId {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        for (&id, (handle, _)) in handles {
            if handle.snapshot_rx.borrow().role == "Leader" {
                return id;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("no leader elected within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn put(handle: &ClusterHandle, key: &str, value: &str) -> ClientCommandResult {
    let (respond_to, receiver) = oneshot::channel();
    handle
        .command_tx
        .send(ClientCommand {
            command: Command::Put { key: key.to_string(), value: value.as_bytes().to_vec() },
            respond_to,
        })
        .expect("command channel should be open");
    receiver.await.expect("cluster should respond")
}

async fn get(handle: &ClusterHandle, key: &str) -> QueryResult {
    let (respond_to, receiver) = oneshot::channel();
    handle
        .query_tx
        .send(ClientQuery { key: key.to_string(), respond_to })
        .expect("query channel should be open");
    receiver.await.expect("cluster should respond")
}

#[tokio::test]
async fn three_node_cluster_elects_exactly_one_leader() {
    let handles = spin_up_cluster(&[1, 2, 3]).await;
    let leader_id = wait_for_leader(&handles, Duration::from_secs(2)).await;

    // Give the rest of the cluster a moment to settle on the same leader
    // (heartbeats propagate) before asserting uniqueness.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let leader_count = handles
        .values()
        .filter(|(handle, _)| handle.snapshot_rx.borrow().role == "Leader")
        .count();
    assert_eq!(leader_count, 1, "exactly one node must be leader, found {leader_count}");
    assert!(handles.contains_key(&leader_id));
}

/// THE test: proves the entire premise of this project. A write is
/// committed by the leader, the leader's process is then killed outright
/// (task abort - no clean shutdown, no final heartbeat, matching a real
/// crash), and the surviving majority elects a new leader that still has
/// the committed write. Zero data loss despite losing the leader.
#[tokio::test]
async fn cluster_survives_leader_crash_with_zero_data_loss() {
    let mut handles = spin_up_cluster(&[1, 2, 3, 4, 5]).await;

    let leader_id = wait_for_leader(&handles, Duration::from_secs(2)).await;
    let leader_handle = &handles[&leader_id].0;

    let write_result = put(leader_handle, "critical-key", "must-survive").await;
    assert!(
        matches!(write_result, ClientCommandResult::Applied(_)),
        "write to the leader should be applied, got {write_result:?}"
    );

    // Simulate a hard crash: abort the task outright. No graceful shutdown,
    // no chance for the leader to notify anyone - exactly what a `kill -9`
    // or power loss looks like from every other node's point of view.
    let (_, leader_join) = handles.remove(&leader_id).unwrap();
    leader_join.abort();

    // Give the abort a moment to actually land before we start polling the
    // survivors - otherwise we might observe the about-to-die leader still
    // reporting itself as leader in the same tick.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_leader_id = wait_for_leader(&handles, Duration::from_secs(3)).await;
    assert_ne!(
        new_leader_id, leader_id,
        "a NEW leader must be elected - the old one is dead, not just unresponsive"
    );

    let new_leader_handle = &handles[&new_leader_id].0;
    // Give the new leader a moment to finish applying anything it inherited
    // in the log (should already be committed, but apply_committed_entries
    // runs on the event loop's own cadence).
    tokio::time::sleep(Duration::from_millis(100)).await;

    let read_result = get(new_leader_handle, "critical-key").await;
    match read_result {
        QueryResult::Value(Some(bytes)) => {
            assert_eq!(bytes, b"must-survive".to_vec(), "value must match what was written before the crash");
        }
        other => panic!("expected the committed write to survive the leader crash, got {other:?}"),
    }
}

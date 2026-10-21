use raft_core::{
    advance_commit_index, build_append_entries, handle_append_entries,
    handle_append_entries_response, handle_request_vote, start_election, ElectionTimeoutConfig,
    LeaderState, Log, NodeId, PersistentState, Role, RpcMessage, Term, VoteTally, VolatileState,
};
use std::collections::HashMap;
use storage::{Command, KvStore, Persistence, Recovered};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};

use crate::config::NodeConfig;

/// A write command submitted by a client. `respond_to` is resolved once the
/// command's log entry is actually committed and applied - not when it's
/// merely appended.
pub struct ClientCommand {
    pub command: Command,
    pub respond_to: oneshot::Sender<ClientCommandResult>,
}

#[derive(Debug, Clone)]
pub enum ClientCommandResult {
    Applied(Option<Vec<u8>>),
    NotLeader { leader_hint: Option<NodeId> },
}

/// A read request. Deliberately NOT a `Command` / NOT routed through the
/// replicated log - a read doesn't change state, so there's nothing to
/// replicate. Answered directly from the leader's locally-applied KvStore.
///
/// Known simplification: this serves from `last_applied` state without the
/// read-index/lease-read protocol from the Raft paper (§8), so it's not
/// *strictly* linearizable under the rare case of a stale leader that hasn't
/// yet learned it lost an election. Fine for an MVP/portfolio scope; a
/// production system would add read-index confirmation before answering.
pub struct ClientQuery {
    pub key: String,
    pub respond_to: oneshot::Sender<QueryResult>,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Value(Option<Vec<u8>>),
    NotLeader { leader_hint: Option<NodeId> },
}

/// Read-only view of cluster state, published after every event-loop
/// iteration for the dashboard to stream over WebSocket.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ClusterSnapshot {
    pub node_id: NodeId,
    pub role: String,
    pub current_term: Term,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_len: u64,
    pub leader_hint: Option<NodeId>,
}

pub struct ClusterHandle {
    pub command_tx: mpsc::UnboundedSender<ClientCommand>,
    pub query_tx: mpsc::UnboundedSender<ClientQuery>,
    pub snapshot_rx: watch::Receiver<ClusterSnapshot>,
}

/// Durable storage is the guarantee behind every acknowledgement and vote this
/// node sends. If a write to it fails we can no longer honour that guarantee, and
/// in-memory state has already moved ahead of the disk, so the only safe response
/// is fail-stop: crash and let the node recover from what is really on disk.
/// (Raft tolerates crashed nodes; it does not tolerate a node that acknowledges
/// entries, or grants votes, that it has not durably recorded.)
fn persist_must<T, E: std::fmt::Display>(result: Result<T, E>, what: &str) -> T {
    match result {
        Ok(value) => value,
        Err(e) => {
            error!(error = %e, operation = what, "durable write failed - halting node");
            panic!("fatal: durable write ({what}) failed: {e}");
        }
    }
}

pub struct Cluster {
    config: NodeConfig,
    persistent: PersistentState,
    volatile: VolatileState,
    leader_state: LeaderState,
    vote_tally: Option<VoteTally>,
    log: Log,
    persistence: Persistence,
    kv_store: KvStore,
    election_timeout: ElectionTimeoutConfig,
    election_deadline: Instant,
    known_leader: Option<NodeId>,
    peer_outboxes: HashMap<NodeId, mpsc::UnboundedSender<RpcMessage>>,
    inbox_rx: mpsc::UnboundedReceiver<(NodeId, RpcMessage)>,
    command_rx: mpsc::UnboundedReceiver<ClientCommand>,
    query_rx: mpsc::UnboundedReceiver<ClientQuery>,
    pending_commands: HashMap<u64, oneshot::Sender<ClientCommandResult>>,
    snapshot_tx: watch::Sender<ClusterSnapshot>,
}

impl Cluster {
    /// Builds a node from what was recovered off disk: the log, plus the
    /// `current_term` / `voted_for` it had before it last stopped. Starting from
    /// the recovered term and vote (rather than from zero) is what stops a
    /// restarted node from voting twice in a term it already voted in.
    pub fn new(
        config: NodeConfig,
        recovered: Recovered,
        peer_outboxes: HashMap<NodeId, mpsc::UnboundedSender<RpcMessage>>,
        inbox_rx: mpsc::UnboundedReceiver<(NodeId, RpcMessage)>,
    ) -> (Self, ClusterHandle) {
        let Recovered { persistence, hard_state: persistent, entries } = recovered;
        let log = Log::from_entries(entries);
        let kv_store = KvStore::new();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let (query_tx, query_rx) = mpsc::unbounded_channel();
        let election_timeout = ElectionTimeoutConfig::default();
        let election_deadline = Instant::now() + election_timeout.random_timeout();

        let (snapshot_tx, snapshot_rx) = watch::channel(ClusterSnapshot {
            node_id: config.id,
            role: "Follower".to_string(),
            current_term: persistent.current_term,
            commit_index: 0,
            last_applied: 0,
            log_len: log.last_index(),
            leader_hint: None,
        });

        let cluster = Self {
            config,
            persistent,
            volatile: VolatileState::new(),
            leader_state: LeaderState::default(),
            vote_tally: None,
            log,
            persistence,
            kv_store,
            election_timeout,
            election_deadline,
            known_leader: None,
            peer_outboxes,
            inbox_rx,
            command_rx,
            query_rx,
            pending_commands: HashMap::new(),
            snapshot_tx,
        };

        (cluster, ClusterHandle { command_tx, query_tx, snapshot_rx })
    }

    /// Heartbeat interval must sit well below the minimum election timeout -
    /// otherwise followers time out and start needless elections even while
    /// a healthy leader is alive. A quarter of the minimum is a conservative margin.
    fn heartbeat_interval(&self) -> Duration {
        self.election_timeout.min / 4
    }

    pub async fn run(mut self) {
        let mut heartbeat_ticker = tokio::time::interval(self.heartbeat_interval());

        loop {
            let election_sleep = tokio::time::sleep_until(self.election_deadline);

            tokio::select! {
                Some((from, msg)) = self.inbox_rx.recv() => {
                    self.handle_rpc(from, msg);
                }
                Some(cmd) = self.command_rx.recv() => {
                    self.handle_client_command(cmd);
                }
                Some(query) = self.query_rx.recv() => {
                    self.handle_client_query(query);
                }
                _ = heartbeat_ticker.tick(), if self.volatile.role == Role::Leader => {
                    self.broadcast_append_entries();
                }
                _ = election_sleep, if self.volatile.role != Role::Leader => {
                    self.begin_election();
                }
            }

            self.apply_committed_entries();
            self.publish_snapshot();
        }
    }

    fn handle_rpc(&mut self, from: NodeId, msg: RpcMessage) {
        match msg {
            RpcMessage::Hello(_) => {} // handshake only matters at connect time

            RpcMessage::RequestVote(req) => {
                let resp =
                    handle_request_vote(&mut self.persistent, &mut self.volatile, &self.log, &req);
                // The vote (and any term bump) must be on disk BEFORE the reply
                // leaves this node, or a crash could make us forget it.
                self.persist_hard_state();
                self.reset_election_deadline_if_follower();
                self.send_to(from, RpcMessage::RequestVoteResponse(resp));
            }

            RpcMessage::RequestVoteResponse(resp) => {
                if resp.term > self.persistent.current_term {
                    self.step_down(resp.term);
                    return;
                }
                if self.volatile.role != Role::Candidate {
                    return; // stale response from a completed/abandoned election
                }
                if resp.vote_granted {
                    let became_leader = self
                        .vote_tally
                        .as_mut()
                        .map(|tally| {
                            tally.record_vote(from);
                            tally.has_majority()
                        })
                        .unwrap_or(false);
                    if became_leader {
                        self.become_leader();
                    }
                }
            }

            RpcMessage::AppendEntries(req) => {
                let term_before = self.persistent.current_term;
                let is_new_term = req.term > term_before;
                let leader_id = req.leader_id;
                let (resp, effect) = handle_append_entries(
                    &mut self.persistent,
                    &mut self.volatile,
                    &mut self.log,
                    &req,
                );

                // Everything below must be durable BEFORE we acknowledge. First the
                // term (an AppendEntries from a newer term bumps it)...
                self.persist_hard_state();
                // ...then mirror exactly what happened to the in-memory log onto
                // disk. A conflict truncates the WAL suffix too; entries we
                // already had are never re-appended.
                if let Some(from_index) = effect.truncated_from {
                    persist_must(self.persistence.truncate_from(from_index), "log truncate");
                }
                for entry in &effect.appended {
                    persist_must(self.persistence.append(entry), "log append");
                }

                // Any request from a current-or-newer term comes from the
                // legitimate leader, even if its log didn't match ours - so
                // clients hitting this follower get a useful leader_hint.
                if req.term >= term_before {
                    self.known_leader = Some(leader_id);
                }
                if is_new_term {
                    self.vote_tally = None;
                }
                self.reset_election_deadline_if_follower();
                self.send_to(from, RpcMessage::AppendEntriesResponse(resp));
            }

            RpcMessage::AppendEntriesResponse(resp) => {
                if resp.term > self.persistent.current_term {
                    self.step_down(resp.term);
                    return;
                }
                // A response stamped with an older term answers a request from
                // a previous leadership of this node. The log may have changed
                // since, so what it reports no longer says anything reliable.
                if resp.term < self.persistent.current_term {
                    return;
                }
                if self.volatile.role != Role::Leader {
                    return;
                }
                handle_append_entries_response(&mut self.leader_state, from, &resp);
                self.advance_leader_commit();
            }
        }
    }

    /// Recomputes commit_index from the followers' reported match_index values
    /// (plus the leader's own log tail) and advances it if a majority now holds
    /// a current-term entry.
    fn advance_leader_commit(&mut self) {
        if let Some(new_commit) = advance_commit_index(
            &self.leader_state,
            &self.log,
            self.persistent.current_term,
            self.config.cluster_size(),
            self.log.last_index(),
        ) {
            self.volatile.commit_index = new_commit;
        }
    }

    /// Durably appends the entry at `index` (already in the in-memory log) to
    /// the WAL. Used for entries this node creates itself as leader.
    fn append_to_wal(&mut self, index: u64) {
        if let Some(entry) = self.log.get(index) {
            persist_must(self.persistence.append(entry), "log append");
        }
    }

    /// Writes `current_term` / `voted_for` to disk if they changed. Called after
    /// every code path that can change them and before anything is sent that
    /// depends on them. A no-op (no fsync) when nothing changed, so steady-state
    /// heartbeats cost nothing.
    fn persist_hard_state(&mut self) {
        persist_must(
            self.persistence.save_hard_state_if_changed(&self.persistent),
            "hard state",
        );
    }

    fn reset_election_deadline_if_follower(&mut self) {
        if self.volatile.role != Role::Leader {
            self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
        }
    }

    fn step_down(&mut self, new_term: Term) {
        self.persistent.current_term = new_term;
        self.persistent.voted_for = None;
        self.persist_hard_state();
        self.volatile.role = Role::Follower;
        self.vote_tally = None;
        self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
    }

    fn begin_election(&mut self) {
        let req = start_election(&mut self.persistent, &mut self.volatile, &self.log, self.config.id);
        // The new term and our vote for ourselves must be durable before any
        // RequestVote leaves this node.
        self.persist_hard_state();
        self.known_leader = None;
        self.vote_tally = Some(VoteTally::new(self.config.id, self.config.cluster_size()));
        self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
        info!(term = self.persistent.current_term, "starting election");

        let peer_ids: Vec<NodeId> = self.peer_outboxes.keys().copied().collect();
        for peer_id in peer_ids {
            self.send_to(peer_id, RpcMessage::RequestVote(req.clone()));
        }

        if self.vote_tally.as_ref().map(|t| t.has_majority()).unwrap_or(false) {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        info!(term = self.persistent.current_term, "became leader");
        self.volatile.role = Role::Leader;
        self.known_leader = Some(self.config.id);
        self.vote_tally = None;

        let next = self.log.last_index() + 1;
        self.leader_state = LeaderState::default();
        for &peer_id in self.peer_outboxes.keys() {
            self.leader_state.next_index.insert(peer_id, next);
            self.leader_state.match_index.insert(peer_id, 0);
        }

        // Raft paper section 8: immediately append a no-op entry in this
        // leader's OWN current term. Entries the previous leader replicated
        // to a majority but never got to mark committed (e.g. it crashed
        // before sending the next heartbeat) sit safely in the log but
        // can't be committed directly by this leader - advance_commit_index
        // only ever commits current-term entries directly. Once this no-op
        // commits, the log-matching property means everything before it
        // (including those stranded entries) becomes committed too.
        let noop = Command::NoOp.encode().expect("NoOp always encodes");
        let noop_index = self.log.append(self.persistent.current_term, noop);
        self.append_to_wal(noop_index);

        self.broadcast_append_entries();

        // Covers the single-node-cluster case (majority of one, satisfied
        // immediately) where no AppendEntriesResponse will ever arrive to
        // trigger this otherwise.
        self.advance_leader_commit();
    }

    fn broadcast_append_entries(&mut self) {
        let peer_ids: Vec<NodeId> = self.peer_outboxes.keys().copied().collect();
        for peer_id in peer_ids {
            let next_index = *self.leader_state.next_index.get(&peer_id).unwrap_or(&1);
            let req = build_append_entries(
                &self.persistent,
                &self.log,
                self.config.id,
                next_index,
                self.volatile.commit_index,
            );
            self.send_to(peer_id, RpcMessage::AppendEntries(req));
        }
    }

    fn handle_client_command(&mut self, cmd: ClientCommand) {
        if self.volatile.role != Role::Leader {
            let _ = cmd
                .respond_to
                .send(ClientCommandResult::NotLeader { leader_hint: self.known_leader });
            return;
        }

        let encoded = match cmd.command.encode() {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!(error = %e, "failed to encode client command");
                return;
            }
        };

        let index = self.log.append(self.persistent.current_term, encoded);
        self.append_to_wal(index);
        self.pending_commands.insert(index, cmd.respond_to);
        self.broadcast_append_entries();

        // A single-node cluster (or a leader whose own append already satisfies
        // a majority) never receives an AppendEntriesResponse to trigger this -
        // the commit check must also run right after the leader's own append.
        self.advance_leader_commit();
    }

    /// Answers a read directly from local applied state - no log append, no
    /// replication round-trip. Only the (believed) leader answers; followers
    /// redirect, same as writes.
    fn handle_client_query(&mut self, query: ClientQuery) {
        if self.volatile.role != Role::Leader {
            let _ = query
                .respond_to
                .send(QueryResult::NotLeader { leader_hint: self.known_leader });
            return;
        }
        let value = self.kv_store.get(&query.key).cloned();
        let _ = query.respond_to.send(QueryResult::Value(value));
    }

    fn apply_committed_entries(&mut self) {
        while self.volatile.last_applied < self.volatile.commit_index {
            let next_index = self.volatile.last_applied + 1;
            let Some(entry) = self.log.get(next_index) else { break };
            let result = match Command::decode(&entry.command) {
                Ok(command) => self.kv_store.apply(&command),
                Err(e) => {
                    warn!(error = %e, "failed to decode committed command");
                    None
                }
            };
            self.volatile.last_applied = next_index;

            if let Some(respond_to) = self.pending_commands.remove(&next_index) {
                let _ = respond_to.send(ClientCommandResult::Applied(result));
            }
        }
    }

    fn send_to(&self, peer_id: NodeId, msg: RpcMessage) {
        if let Some(tx) = self.peer_outboxes.get(&peer_id) {
            let _ = tx.send(msg);
        }
    }

    fn publish_snapshot(&self) {
        let _ = self.snapshot_tx.send(ClusterSnapshot {
            node_id: self.config.id,
            role: format!("{:?}", self.volatile.role),
            current_term: self.persistent.current_term,
            commit_index: self.volatile.commit_index,
            last_applied: self.volatile.last_applied,
            log_len: self.log.last_index(),
            leader_hint: self.known_leader,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_core::{
        AppendEntriesRequest, AppendEntriesResponse, LogEntry, RequestVoteRequest,
        RequestVoteResponse,
    };
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use storage::{Wal, WAL_FILE};

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    /// A scratch storage directory, removed on drop. Declare it BEFORE the
    /// cluster that uses it so the cluster (and its open files) is dropped first.
    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let n = DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
            Self(std::env::temp_dir().join(format!("quorum-unit-{nanos}-{n}")))
        }
        fn path(&self) -> &Path {
            &self.0
        }
        fn wal_path(&self) -> PathBuf {
            self.0.join(WAL_FILE)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    type Outputs = HashMap<NodeId, mpsc::UnboundedReceiver<RpcMessage>>;

    /// Builds node 1 over `dir` with the given peers, recovering whatever is
    /// already stored there (so calling it twice on one directory is a restart).
    /// Returns the receivers for each peer's outbound channel so a test can read
    /// exactly what this node replied.
    fn make_cluster_in(dir: &Path, peer_ids: &[NodeId]) -> (Cluster, Outputs) {
        let peers = peer_ids.iter().map(|&id| (id, String::new())).collect();
        let config = NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            metrics_addr: "127.0.0.1:0".to_string(),
            peers,
            storage_dir: dir.to_path_buf(),
        };
        let mut outboxes = HashMap::new();
        let mut outputs = HashMap::new();
        for &id in peer_ids {
            let (tx, rx) = mpsc::unbounded_channel();
            outboxes.insert(id, tx);
            outputs.insert(id, rx);
        }
        let (_inbox_tx, inbox_rx) = mpsc::unbounded_channel();
        let recovered = Persistence::open(dir).expect("storage should open");
        let (cluster, _handle) = Cluster::new(config, recovered, outboxes, inbox_rx);
        (cluster, outputs)
    }

    /// For tests that never look at what the node sends: the peers' receivers
    /// are dropped, so sends are silently discarded.
    fn build_cluster(dir: &TestDir, peer_ids: &[NodeId]) -> Cluster {
        make_cluster_in(dir.path(), peer_ids).0
    }

    fn entry(term: Term, index: u64, command: &str) -> LogEntry {
        LogEntry { term, index, command: command.as_bytes().to_vec() }
    }

    fn append_request(
        term: Term,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
    ) -> RpcMessage {
        RpcMessage::AppendEntries(AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: 0,
        })
    }

    fn vote_request(term: Term, candidate_id: NodeId) -> RpcMessage {
        RpcMessage::RequestVote(RequestVoteRequest {
            term,
            candidate_id,
            last_log_index: 0,
            last_log_term: 0,
        })
    }

    fn put_command(key: &str, value: &str) -> ClientCommand {
        let (respond_to, _receiver) = oneshot::channel();
        ClientCommand {
            command: Command::Put { key: key.into(), value: value.as_bytes().to_vec() },
            respond_to,
        }
    }

    /// The vote reply this node sent to `peer`, if any.
    fn vote_reply(outputs: &mut Outputs, peer: NodeId) -> RequestVoteResponse {
        match outputs.get_mut(&peer).unwrap().try_recv() {
            Ok(RpcMessage::RequestVoteResponse(resp)) => resp,
            other => panic!("expected a vote reply to node {peer}, got {other:?}"),
        }
    }

    // ---- existing behaviour -------------------------------------------------

    #[test]
    fn single_node_cluster_becomes_leader_on_election() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        assert_eq!(cluster.volatile.role, Role::Follower);
        cluster.begin_election();
        assert_eq!(cluster.volatile.role, Role::Leader);
    }

    #[tokio::test]
    async fn client_command_rejected_when_not_leader() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        let (respond_to, receiver) = oneshot::channel();
        cluster.handle_client_command(ClientCommand {
            command: Command::Put { key: "k".into(), value: b"v".to_vec() },
            respond_to,
        });
        let result = receiver.await.unwrap();
        assert!(matches!(result, ClientCommandResult::NotLeader { .. }));
    }

    #[tokio::test]
    async fn leader_appends_client_command_to_log() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        cluster.begin_election();
        assert_eq!(cluster.volatile.role, Role::Leader);

        // become_leader() appends a no-op at index 1 (Raft section 8) before
        // any client command can land - so the client command below is
        // expected at index 2, not 1.
        assert_eq!(cluster.log.last_index(), 1);

        let (respond_to, _receiver) = oneshot::channel();
        cluster.handle_client_command(ClientCommand {
            command: Command::Put { key: "k".into(), value: b"v".to_vec() },
            respond_to,
        });
        assert_eq!(cluster.log.last_index(), 2);
    }

    #[tokio::test]
    async fn query_rejected_when_not_leader() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        let (respond_to, receiver) = oneshot::channel();
        cluster.handle_client_query(ClientQuery { key: "k".into(), respond_to });
        let result = receiver.await.unwrap();
        assert!(matches!(result, QueryResult::NotLeader { .. }));
    }

    #[tokio::test]
    async fn leader_answers_query_from_applied_state() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        cluster.begin_election();

        // Directly apply, bypassing the full commit pipeline - this test is
        // only checking the query path reads from kv_store correctly, not
        // re-testing commit/apply (already covered by raft-core's own tests).
        cluster.kv_store.apply(&Command::Put { key: "k".into(), value: b"v".to_vec() });

        let (respond_to, receiver) = oneshot::channel();
        cluster.handle_client_query(ClientQuery { key: "k".into(), respond_to });
        let result = receiver.await.unwrap();
        assert!(matches!(result, QueryResult::Value(Some(v)) if v == b"v".to_vec()));
    }

    #[test]
    fn follower_wal_mirrors_truncation_after_conflicting_append() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);

        // Term-1 leader replicates entries 1 and 2.
        cluster.handle_rpc(
            2,
            append_request(1, 2, 0, 0, vec![entry(1, 1, "a"), entry(1, 2, "b")]),
        );
        // A term-2 leader disagrees about entry 2.
        cluster.handle_rpc(3, append_request(2, 3, 1, 1, vec![entry(2, 2, "c")]));

        assert_eq!(cluster.log.last_index(), 2);
        assert_eq!(cluster.log.term_at(2), Some(2));

        // What a restart would recover must equal the in-memory log.
        let on_disk = Wal::replay(dir.wal_path()).unwrap();
        assert_eq!(on_disk.len(), 2);
        assert_eq!(on_disk[0].command, b"a".to_vec());
        assert_eq!(on_disk[1].term, 2);
        assert_eq!(on_disk[1].command, b"c".to_vec());
    }

    #[test]
    fn duplicate_append_entries_does_not_duplicate_wal_records() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);

        let request = append_request(1, 2, 0, 0, vec![entry(1, 1, "a")]);
        cluster.handle_rpc(2, request.clone());
        cluster.handle_rpc(2, request);

        assert_eq!(cluster.log.last_index(), 1);
        assert_eq!(Wal::replay(dir.wal_path()).unwrap().len(), 1);
    }

    #[test]
    fn follower_learns_leader_hint_even_when_its_log_does_not_match() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        // prev_log_index 5 can't match our empty log, so this is rejected...
        cluster.handle_rpc(2, append_request(1, 2, 5, 1, vec![]));
        // ...but it still came from the legitimate leader of term 1.
        assert_eq!(cluster.known_leader, Some(2));
    }

    #[test]
    fn leader_commit_uses_follower_reported_match_index_not_its_own_log_tail() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader(); // no-op at index 1
        cluster.handle_client_command(put_command("a", "1")); // index 2
        cluster.handle_client_command(put_command("b", "2")); // index 3
        assert_eq!(cluster.log.last_index(), 3);

        // Follower 2 acknowledges only index 1, even though the leader's log
        // has already grown to 3 since that request was sent.
        cluster.handle_rpc(
            2,
            RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: 1,
                success: true,
                conflict_index: None,
                match_index: 1,
            }),
        );

        assert_eq!(cluster.leader_state.match_index.get(&2), Some(&1));
        assert_eq!(
            cluster.volatile.commit_index, 1,
            "indices 2 and 3 are on only one node and must not be committed"
        );
    }

    #[test]
    fn leader_ignores_append_response_from_an_earlier_term() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 2;
        cluster.become_leader();

        cluster.handle_rpc(
            2,
            RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: 1,
                success: true,
                conflict_index: None,
                match_index: 1,
            }),
        );

        assert_eq!(cluster.leader_state.match_index.get(&2), Some(&0));
    }

    // ---- durable hard state -------------------------------------------------

    #[test]
    fn restarted_node_remembers_its_vote_and_refuses_a_second_one_in_that_term() {
        let dir = TestDir::new();

        // First life: grant node 2 a vote in term 5, then "crash".
        {
            let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
            cluster.handle_rpc(2, vote_request(5, 2));
            assert!(vote_reply(&mut outputs, 2).vote_granted);
        }

        // Second life, same storage directory.
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        assert_eq!(cluster.persistent.current_term, 5, "term must survive the restart");
        assert_eq!(cluster.persistent.voted_for, Some(2), "vote must survive the restart");

        // Node 3 now asks for a vote in the same term. Without a persisted vote
        // this restarted node would grant it: two votes in one term.
        cluster.handle_rpc(3, vote_request(5, 3));
        assert!(
            !vote_reply(&mut outputs, 3).vote_granted,
            "must not vote twice in term 5 just because the process restarted"
        );
    }

    #[test]
    fn a_term_learned_from_a_leader_survives_a_restart() {
        let dir = TestDir::new();
        {
            let mut cluster = build_cluster(&dir, &[]);
            cluster.handle_rpc(2, append_request(3, 2, 0, 0, vec![]));
            assert_eq!(cluster.persistent.current_term, 3);
        }
        let cluster = build_cluster(&dir, &[]);
        assert_eq!(cluster.persistent.current_term, 3);
    }

    #[test]
    fn a_candidates_own_vote_is_durable_before_it_asks_anyone_else() {
        let dir = TestDir::new();
        {
            let mut cluster = build_cluster(&dir, &[2, 3]);
            cluster.begin_election();
            assert_eq!(cluster.persistent.current_term, 1);
            assert_eq!(cluster.persistent.voted_for, Some(1));
        }
        let cluster = build_cluster(&dir, &[2, 3]);
        assert_eq!(cluster.persistent.current_term, 1);
        assert_eq!(cluster.persistent.voted_for, Some(1), "must remember it voted for itself");
    }

    #[test]
    fn stepping_down_to_a_higher_term_is_durable() {
        let dir = TestDir::new();
        {
            let mut cluster = build_cluster(&dir, &[2, 3]);
            cluster.persistent.current_term = 1;
            cluster.become_leader();
            cluster.handle_rpc(
                2,
                RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
                    term: 4,
                    success: false,
                    conflict_index: None,
                    match_index: 0,
                }),
            );
            assert_eq!(cluster.volatile.role, Role::Follower);
        }
        let cluster = build_cluster(&dir, &[2, 3]);
        assert_eq!(cluster.persistent.current_term, 4);
        assert_eq!(cluster.persistent.voted_for, None);
    }

    #[test]
    fn restart_recovers_the_replicated_log() {
        let dir = TestDir::new();
        {
            let mut cluster = build_cluster(&dir, &[]);
            cluster.handle_rpc(
                2,
                append_request(1, 2, 0, 0, vec![entry(1, 1, "a"), entry(1, 2, "b")]),
            );
        }
        let cluster = build_cluster(&dir, &[]);
        assert_eq!(cluster.log.last_index(), 2);
        assert_eq!(cluster.log.term_at(2), Some(1));
    }
}
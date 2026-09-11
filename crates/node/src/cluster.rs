use raft_core::{
    advance_commit_index, build_append_entries, handle_append_entries,
    handle_append_entries_response, handle_request_vote, start_election, ElectionTimeoutConfig,
    LeaderState, Log, NodeId, PersistentState, Role, RpcMessage, Term, VoteTally, VolatileState,
};
use std::collections::HashMap;
use storage::{Command, KvStore, Wal};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

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

pub struct Cluster {
    config: NodeConfig,
    persistent: PersistentState,
    volatile: VolatileState,
    leader_state: LeaderState,
    vote_tally: Option<VoteTally>,
    log: Log,
    wal: Wal,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: NodeConfig,
        wal: Wal,
        log: Log,
        kv_store: KvStore,
        persistent: PersistentState,
        peer_outboxes: HashMap<NodeId, mpsc::UnboundedSender<RpcMessage>>,
        inbox_rx: mpsc::UnboundedReceiver<(NodeId, RpcMessage)>,
    ) -> (Self, ClusterHandle) {
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
            wal,
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
                let is_new_term = req.term > self.persistent.current_term;
                let leader_id = req.leader_id;
                let resp = handle_append_entries(
                    &mut self.persistent,
                    &mut self.volatile,
                    &mut self.log,
                    &req,
                );
                if resp.success {
                    self.known_leader = Some(leader_id);
                    for entry in &req.entries {
                        if let Err(e) = self.wal.append(entry) {
                            warn!(error = %e, "WAL append failed");
                        }
                    }
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
                if self.volatile.role != Role::Leader {
                    return;
                }
                let next_index = *self.leader_state.next_index.get(&from).unwrap_or(&1);
                let sent_prev_log_index = next_index.saturating_sub(1);
                let sent_entries_count = self.log.last_index().saturating_sub(sent_prev_log_index);
                handle_append_entries_response(
                    &mut self.leader_state,
                    from,
                    sent_prev_log_index,
                    sent_entries_count,
                    &resp,
                );

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
        }
    }

    fn reset_election_deadline_if_follower(&mut self) {
        if self.volatile.role != Role::Leader {
            self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
        }
    }

    fn step_down(&mut self, new_term: Term) {
        self.persistent.current_term = new_term;
        self.persistent.voted_for = None;
        self.volatile.role = Role::Follower;
        self.vote_tally = None;
        self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
    }

    fn begin_election(&mut self) {
        let req = start_election(&mut self.persistent, &mut self.volatile, &self.log, self.config.id);
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

        self.broadcast_append_entries();
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
        if let Some(entry) = self.log.get(index) {
            if let Err(e) = self.wal.append(entry) {
                warn!(error = %e, "WAL append failed for leader-local entry");
            }
        }
        self.pending_commands.insert(index, cmd.respond_to);
        self.broadcast_append_entries();

        // A single-node cluster (or a leader whose own append already satisfies
        // a majority) never receives an AppendEntriesResponse to trigger this -
        // advance_commit_index must also be checked right after the leader's
        // own log append, using log.last_index() as the leader's own match_index.
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_wal() -> Wal {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        Wal::open(std::env::temp_dir().join(format!("quorum-test-{nanos}.wal"))).unwrap()
    }

    fn build_cluster() -> Cluster {
        let config = NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            metrics_addr: "127.0.0.1:0".to_string(),
            peers: HashMap::new(),
            storage_dir: std::env::temp_dir(),
        };
        let (_inbox_tx, inbox_rx) = mpsc::unbounded_channel();
        let (cluster, _handle) = Cluster::new(
            config,
            temp_wal(),
            Log::new(),
            KvStore::new(),
            PersistentState::default(),
            HashMap::new(),
            inbox_rx,
        );
        cluster
    }

    #[test]
    fn single_node_cluster_becomes_leader_on_election() {
        let mut cluster = build_cluster();
        assert_eq!(cluster.volatile.role, Role::Follower);
        cluster.begin_election();
        assert_eq!(cluster.volatile.role, Role::Leader);
    }

    #[tokio::test]
    async fn client_command_rejected_when_not_leader() {
        let mut cluster = build_cluster();
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
        let mut cluster = build_cluster();
        cluster.begin_election();
        assert_eq!(cluster.volatile.role, Role::Leader);

        let (respond_to, _receiver) = oneshot::channel();
        cluster.handle_client_command(ClientCommand {
            command: Command::Put { key: "k".into(), value: b"v".to_vec() },
            respond_to,
        });
        assert_eq!(cluster.log.last_index(), 1);
    }

    #[tokio::test]
    async fn query_rejected_when_not_leader() {
        let mut cluster = build_cluster();
        let (respond_to, receiver) = oneshot::channel();
        cluster.handle_client_query(ClientQuery { key: "k".into(), respond_to });
        let result = receiver.await.unwrap();
        assert!(matches!(result, QueryResult::NotLeader { .. }));
    }

    #[tokio::test]
    async fn leader_answers_query_from_applied_state() {
        let mut cluster = build_cluster();
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
}

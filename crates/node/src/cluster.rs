use raft_core::{
    advance_commit_index, build_append_entries, handle_append_entries,
    handle_append_entries_response, handle_pre_vote, handle_request_vote, start_election,
    ConfigChange, ConfigHistory, ElectionTimeoutConfig, LeaderState, Log, LogEntry, Membership,
    NodeId, PersistentState, PreVoteRequest, PreVoteResponse, RequestVoteResponse, Role,
    RpcMessage, Term, VoteTally, VolatileState,
};
use std::collections::{BTreeMap, HashMap};
use storage::{Command, KvStore, Persistence, Recovered, RecoveredSnapshot};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

use crate::config::{NodeConfig, ReadMode};

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
    /// This node was the leader when it accepted the write but lost leadership
    /// before the write was confirmed. The outcome is UNKNOWN, not "failed": the
    /// entry may already be on a majority, in which case the next leader will
    /// commit it, or it may be discarded. Callers must not assume either.
    LeadershipLost,
    /// The leader already holds as many unfinished writes as it will carry, so
    /// this one was NOT accepted: nothing was logged or replicated. Retrying is
    /// safe, unlike `LeadershipLost`.
    Overloaded,
}

/// A read request. Deliberately NOT a `Command` / NOT routed through the
/// replicated log - a read doesn't change state, so there's nothing to
/// replicate.
///
/// By default reads are linearizable (ReadIndex, Raft dissertation §6.4): the
/// leader records its commit index, proves with a heartbeat round that a majority
/// still recognises it as leader, waits until its state machine has applied up to
/// the recorded index, and only then answers from its local state. A leader that
/// has been cut off therefore cannot return a stale value; it answers "not the
/// leader" instead. `ReadMode::Local` skips the proof for speed.
pub struct ClientQuery {
    pub key: String,
    pub respond_to: oneshot::Sender<QueryResult>,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Value(Option<Vec<u8>>),
    NotLeader { leader_hint: Option<NodeId> },
    /// The leader already holds as many waiting reads as it will carry. Retry.
    Overloaded,
}

/// How a node opens a connection to a peer it has just learned about (a node
/// being added to the cluster). Given the peer's id and address it returns the
/// queue that messages for that peer are written to; the connection behind it is
/// the connector's business, and may be a real TCP link or, in tests, a channel
/// into a simulated network. The cluster itself never touches a socket.
pub type PeerConnector = Box<dyn Fn(NodeId, &str) -> mpsc::Sender<RpcMessage> + Send + 'static>;

/// A change to the cluster's membership, one server at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipChange {
    /// Add `id`, reachable at `addr`, as a voting member. The node first joins as
    /// a non-voting learner and is only promoted once it has caught up.
    Add { id: NodeId, addr: String },
    /// Remove `id` from the voting members.
    Remove { id: NodeId },
}

pub struct ClientAdmin {
    pub change: MembershipChange,
    /// Resolved once the change is COMMITTED, not merely accepted.
    pub respond_to: oneshot::Sender<AdminResult>,
}

#[derive(Debug, Clone)]
pub enum AdminResult {
    /// The change was committed and is in effect cluster-wide.
    Applied { members: Vec<NodeId> },
    NotLeader { leader_hint: Option<NodeId> },
    /// The change was refused and NOTHING was logged, so retrying (once the reason
    /// has gone) is always safe.
    Rejected(AdminRejection),
    /// This node lost leadership while the change was in progress. The outcome is
    /// UNKNOWN: the entry may already be on a majority and be committed by the next
    /// leader. Check the membership before retrying.
    LeadershipLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminRejection {
    /// This leader has not yet committed an entry of its own term, so it cannot be
    /// sure what the current configuration is. Retry in a moment.
    NotReady,
    /// Another membership change has not finished. Only one may be in flight, which
    /// is exactly what makes single-server changes safe.
    ChangeInProgress,
    AlreadyMember,
    NotMember,
    /// The cluster must keep at least one voting member.
    LastMember,
    /// A node to add needs an address the others can dial.
    BadAddress,
    /// This leader's log no longer starts at the beginning, so a new node cannot be
    /// brought up to date by replication alone and would need the snapshot sent to
    /// it, which is not supported yet. Refused up front rather than left to time out.
    LogCompacted,
    /// The joining node did not catch up with the log in time, or the caller gave
    /// up waiting. It was not added.
    CatchUpFailed,
}

impl std::fmt::Display for AdminRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AdminRejection::NotReady => {
                "this leader has not committed an entry of its own term yet; retry shortly"
            }
            AdminRejection::ChangeInProgress => {
                "another membership change is still in progress; only one at a time is allowed"
            }
            AdminRejection::AlreadyMember => "that node is already a member of the cluster",
            AdminRejection::NotMember => "that node is not a member of the cluster",
            AdminRejection::LastMember => "the last remaining member cannot be removed",
            AdminRejection::BadAddress => "a node to add needs a non-empty address (host:port)",
            AdminRejection::LogCompacted => {
                "this cluster's log has been compacted into a snapshot, and a new node cannot \
                 be brought up to date from a compacted log yet"
            }
            AdminRejection::CatchUpFailed => {
                "the new node did not catch up with the log in time and was not added"
            }
        })
    }
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
    /// Times the log has been forced to disk. With group commit this grows much
    /// more slowly than the number of writes under load.
    pub wal_syncs: u64,
    /// Outbound peer messages discarded because that peer's queue was full.
    pub dropped_messages: u64,
    /// The voting members this node currently believes in, in id order. A change
    /// shows here as soon as the entry reaches this node's log, before it commits.
    pub members: Vec<NodeId>,
    /// Nodes the leader is bringing up to date before admitting them as voters.
    /// Only ever non-empty on a leader with an add in progress.
    pub learners: Vec<NodeId>,
    /// Index the newest snapshot on disk reaches (0 if none has been taken).
    pub snapshot_index: u64,
    /// Index of the last log entry that has been discarded; the log starts after it.
    pub log_base: u64,
    /// Snapshots this process has taken since it started.
    pub snapshots_taken: u64,
}

pub struct ClusterHandle {
    pub command_tx: mpsc::Sender<ClientCommand>,
    pub query_tx: mpsc::Sender<ClientQuery>,
    pub admin_tx: mpsc::Sender<ClientAdmin>,
    pub snapshot_rx: watch::Receiver<ClusterSnapshot>,
}

/// Takes `first` plus whatever else is already waiting in `queue`, up to `max`
/// items in all, without ever waiting for more. Requests that arrive together are
/// handled together, so they share one disk sync and one round of replication.
/// Under light load the batch is a single item and nothing is delayed; under heavy
/// load batches grow by themselves.
fn collect_batch<T>(first: T, queue: &mut mpsc::Receiver<T>, max: usize) -> Vec<T> {
    let mut batch = Vec::with_capacity(max.min(16));
    batch.push(first);
    while batch.len() < max {
        match queue.try_recv() {
            Ok(item) => batch.push(item),
            Err(_) => break,
        }
    }
    batch
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

/// A read that is waiting for the leader to prove it is still the leader.
struct PendingRead {
    key: String,
    respond_to: oneshot::Sender<QueryResult>,
    /// The log index the state machine must have applied before this read may
    /// be answered: the commit index when the read arrived, but never lower than
    /// this leader's own first (no-op) entry, because until that entry commits
    /// the leader cannot be sure its commit index reflects everything committed
    /// under earlier leaders.
    read_index: u64,
    /// The first heartbeat round that began after the read arrived. A majority
    /// answering this round (or a later one) proves leadership at a moment no
    /// earlier than the read's arrival.
    required_seq: u64,
}

/// The one membership change this leader has in flight.
struct PendingAdmin {
    respond_to: oneshot::Sender<AdminResult>,
    stage: AdminStage,
}

enum AdminStage {
    /// The node being added is a learner: it receives the log, but nothing counts
    /// its acknowledgements. It is promoted once its match index reaches the
    /// leader's commit index, so that adding it can never leave the cluster one
    /// slow disk away from losing its majority.
    CatchingUp { id: NodeId, addr: String, deadline: Instant },
    /// The configuration entry is in the log and in effect; waiting for it to be
    /// committed and applied.
    Committing { index: u64, removes_self: bool },
}

/// The change a log entry carries, if it carries one.
fn config_change_of(command: &[u8]) -> Option<ConfigChange> {
    Command::decode(command).ok().and_then(|c| c.config_change())
}

/// An in-progress pre-vote round: the term this node WOULD use, and the answers
/// collected so far. Nothing here is persisted or visible to peers as a term
/// change; if the round fails it simply evaporates.
struct PreVoteRound {
    term: Term,
    tally: VoteTally,
}

pub struct Cluster {
    config: NodeConfig,
    persistent: PersistentState,
    volatile: VolatileState,
    leader_state: LeaderState,
    vote_tally: Option<VoteTally>,
    pre_vote: Option<PreVoteRound>,
    log: Log,
    persistence: Persistence,
    kv_store: KvStore,
    election_timeout: ElectionTimeoutConfig,
    election_deadline: Instant,
    known_leader: Option<NodeId>,
    peer_outboxes: HashMap<NodeId, mpsc::Sender<RpcMessage>>,
    inbox_rx: mpsc::Receiver<(NodeId, RpcMessage)>,
    command_rx: mpsc::Receiver<ClientCommand>,
    query_rx: mpsc::Receiver<ClientQuery>,
    admin_rx: mpsc::Receiver<ClientAdmin>,
    /// Every configuration entry in the log, on top of the bootstrap membership.
    /// The log is the source of truth for who is in the cluster.
    history: ConfigHistory,
    /// `history.current()`, cached: who votes right now. A change is in effect from
    /// the moment its entry is appended, long before it commits.
    membership: Membership,
    /// Leader only: nodes being caught up before they are admitted.
    learners: BTreeMap<NodeId, String>,
    /// The membership change this leader is working through, if any.
    pending_admin: Option<PendingAdmin>,
    connector: PeerConnector,
    /// Snapshots taken since this process started.
    snapshots_taken: u64,
    /// After a failed snapshot, do not try again until this many entries are applied,
    /// so a full disk is not retried (and logged) on every event.
    snapshot_retry_after: u64,
    /// Outbound messages discarded because a peer's queue was full.
    dropped_messages: u64,
    pending_commands: HashMap<u64, oneshot::Sender<ClientCommandResult>>,
    pending_reads: Vec<PendingRead>,
    /// Numbers every AppendEntries broadcast this node sends. Never reset, so a
    /// round number is never reused across leaderships of the same node.
    heartbeat_seq: u64,
    /// Index of the no-op this node appended when it last became leader.
    noop_index: u64,
    snapshot_tx: watch::Sender<ClusterSnapshot>,
}

impl Cluster {
    /// Builds a node from what was recovered off disk: the log, plus the
    /// `current_term` / `voted_for` it had before it last stopped. Starting from
    /// the recovered term and vote (rather than from zero) is what stops a
    /// restarted node from voting twice in a term it already voted in.
    ///
    /// The cluster's membership is rebuilt from the log (on top of the bootstrap
    /// membership in `config`), and `connector` is how the node dials any member
    /// it learns about that it has no connection to yet.
    pub fn new(
        config: NodeConfig,
        recovered: Recovered,
        peer_outboxes: HashMap<NodeId, mpsc::Sender<RpcMessage>>,
        inbox_rx: mpsc::Receiver<(NodeId, RpcMessage)>,
        connector: PeerConnector,
    ) -> (Self, ClusterHandle) {
        let Recovered { persistence, hard_state: persistent, snapshot, entries, .. } = recovered;

        // A node that has a snapshot starts from it: the state machine as it was,
        // the log's starting point, and the membership at that point. The latter
        // replaces the bootstrap membership from the environment, because the log
        // entries that produced it are gone. Only the entries AFTER the snapshot
        // are replayed on top.
        let (base_index, base_term, kv_store, base_membership) = match snapshot {
            Some(RecoveredSnapshot { metadata, state }) => (
                metadata.last_included_index,
                metadata.last_included_term,
                state,
                metadata.membership,
            ),
            None => (0, 0, KvStore::new(), config.base_membership()),
        };
        let mut history = ConfigHistory::new(base_membership);
        for entry in &entries {
            if let Some(change) = config_change_of(&entry.command) {
                history.record(entry.index, change);
            }
        }
        let membership = history.current();
        let log = Log::from_parts(base_index, base_term, entries);
        let mut volatile = VolatileState::new();
        // Everything a snapshot covers was committed and applied before it was taken.
        volatile.commit_index = base_index;
        volatile.last_applied = base_index;
        let capacity = config.limits.channel_capacity;
        let (command_tx, command_rx) = mpsc::channel(capacity);
        let (query_tx, query_rx) = mpsc::channel(capacity);
        let (admin_tx, admin_rx) = mpsc::channel(capacity);
        let election_timeout = ElectionTimeoutConfig::default();
        let election_deadline = Instant::now() + election_timeout.random_timeout();

        let (snapshot_tx, snapshot_rx) = watch::channel(ClusterSnapshot {
            node_id: config.id,
            role: "Follower".to_string(),
            current_term: persistent.current_term,
            commit_index: base_index,
            last_applied: base_index,
            log_len: log.last_index(),
            leader_hint: None,
            wal_syncs: persistence.wal_sync_count(),
            dropped_messages: 0,
            members: membership.ids().collect(),
            learners: Vec::new(),
            snapshot_index: base_index,
            log_base: base_index,
            snapshots_taken: 0,
        });

        let mut cluster = Self {
            config,
            persistent,
            volatile,
            leader_state: LeaderState::default(),
            vote_tally: None,
            pre_vote: None,
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
            admin_rx,
            history,
            membership,
            learners: BTreeMap::new(),
            pending_admin: None,
            connector,
            snapshots_taken: 0,
            snapshot_retry_after: 0,
            dropped_messages: 0,
            pending_commands: HashMap::new(),
            pending_reads: Vec::new(),
            heartbeat_seq: 0,
            noop_index: 0,
            snapshot_tx,
        };

        // The log may name members this process was not told about on startup, and
        // the peers it WAS told about may since have been removed.
        cluster.connect_missing_peers();
        cluster.drop_peers_outside_membership();

        (cluster, ClusterHandle { command_tx, query_tx, admin_tx, snapshot_rx })
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
                    let batch = collect_batch(cmd, &mut self.command_rx, self.config.limits.max_batch);
                    self.handle_client_commands(batch);
                }
                Some(query) = self.query_rx.recv() => {
                    let batch = collect_batch(query, &mut self.query_rx, self.config.limits.max_batch);
                    self.handle_client_queries(batch);
                }
                Some(admin) = self.admin_rx.recv() => {
                    self.handle_client_admin(admin);
                }
                _ = heartbeat_ticker.tick(), if self.volatile.role == Role::Leader => {
                    self.check_quorum();
                    if self.volatile.role == Role::Leader {
                        self.prune_abandoned_requests();
                        self.broadcast_append_entries();
                    }
                }
                _ = election_sleep, if self.volatile.role != Role::Leader => {
                    self.begin_pre_vote();
                }
            }

            self.apply_committed_entries();
            self.maybe_snapshot();
            self.drive_admin();
            self.complete_ready_reads();
            self.publish_snapshot();
        }
    }

    /// Handles one inbound RPC. Leadership can be lost inside almost any handler
    /// (a higher term arriving in a vote request, an append, or either response),
    /// so rather than patching every path this wrapper checks for the transition
    /// once, afterwards, and fails the writes that were waiting on this node.
    fn handle_rpc(&mut self, from: NodeId, msg: RpcMessage) {
        let was_leader = self.volatile.role == Role::Leader;
        self.dispatch_rpc(from, msg);
        if was_leader && self.volatile.role != Role::Leader {
            // Whoever we were told about by the message that deposed us (an append
            // from the new leader) has already been recorded as `known_leader`. If
            // nothing named a new leader, the only name we hold is our own, and
            // pointing clients at ourselves would just bounce them straight back.
            if self.known_leader == Some(self.config.id) {
                self.known_leader = None;
            }
            self.abort_pending_requests();
        }
    }

    /// Answers everything still waiting on this node once it is no longer leader.
    ///
    /// Writes get `LeadershipLost`: their outcome is genuinely unknown, because the
    /// entry may yet be committed by the next leader. Reads get `NotLeader`: a read
    /// has no side effect to be unsure about, and "I am not the leader" is simply
    /// true now, so the client is redirected like any other request to a follower.
    /// Without this, a deposed leader would leave its clients waiting forever.
    fn abort_pending_requests(&mut self) {
        if !self.pending_commands.is_empty() {
            info!(
                pending = self.pending_commands.len(),
                "lost leadership - failing writes that were waiting on this node"
            );
            for (_, respond_to) in self.pending_commands.drain() {
                let _ = respond_to.send(ClientCommandResult::LeadershipLost);
            }
        }
        // A membership change in flight is answered like a write: the entry (if it
        // was logged) may still be committed by the next leader, so the outcome is
        // unknown. Learners are this leader's alone; the next leader starts afresh.
        if let Some(pending) = self.pending_admin.take() {
            info!("lost leadership - failing the membership change that was in progress");
            let _ = pending.respond_to.send(AdminResult::LeadershipLost);
        }
        self.drop_learners();
        if !self.pending_reads.is_empty() {
            info!(
                pending = self.pending_reads.len(),
                "lost leadership - redirecting reads that were waiting on this node"
            );
            let leader_hint = self.known_leader;
            for read in self.pending_reads.drain(..) {
                let _ = read.respond_to.send(QueryResult::NotLeader { leader_hint });
            }
        }
    }

    /// Drops bookkeeping for requests whose caller has already gone away (an HTTP
    /// request that timed out, or a client that disconnected). A write's log entry
    /// stays and is committed or discarded as usual; only the reply slot is
    /// released, so an unreachable majority cannot make these queues grow forever.
    fn prune_abandoned_requests(&mut self) {
        self.pending_commands.retain(|_, respond_to| !respond_to.is_closed());
        self.pending_reads.retain(|read| !read.respond_to.is_closed());
    }

    fn dispatch_rpc(&mut self, from: NodeId, msg: RpcMessage) {
        match msg {
            RpcMessage::Hello(_) => {} // handshake only matters at connect time

            RpcMessage::RequestVote(req) if !self.membership.contains(req.candidate_id) => {
                // A node that is not a voting member (a removed server that never
                // heard, or one not yet admitted) has no business starting an
                // election. Refuse WITHOUT looking at its term, or a stale server
                // could depose a healthy leader just by timing out (dissertation §4.2.3).
                self.send_to(
                    from,
                    RpcMessage::RequestVoteResponse(RequestVoteResponse {
                        term: self.persistent.current_term,
                        vote_granted: false,
                    }),
                );
            }

            RpcMessage::PreVote(req) if !self.membership.contains(req.candidate_id) => {
                self.send_to(
                    from,
                    RpcMessage::PreVoteResponse(PreVoteResponse {
                        term: self.persistent.current_term,
                        vote_granted: false,
                        request_term: req.term,
                    }),
                );
            }

            RpcMessage::RequestVote(req) => {
                let resp =
                    handle_request_vote(&mut self.persistent, &mut self.volatile, &self.log, &req);
                // The vote (and any term bump) must be on disk BEFORE the reply
                // leaves this node, or a crash could make us forget it.
                self.persist_hard_state();
                if resp.vote_granted {
                    // Granting a vote counts as contact (it is what stops us
                    // granting pre-votes to a rival right afterwards). Stamp it
                    // only now that the vote is durable: if the disk was slow, the
                    // time spent writing must not make the contact look stale.
                    self.volatile.last_heartbeat = std::time::Instant::now();
                }
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

            RpcMessage::PreVote(req) => {
                // Read-only by construction: no term change, no vote recorded, no
                // timer reset, nothing to persist. Answering must never have side
                // effects, or a disruptive node could use it to suppress elections.
                let resp = handle_pre_vote(&self.persistent, &self.log, &req, self.leader_is_active());
                self.send_to(from, RpcMessage::PreVoteResponse(resp));
            }

            RpcMessage::PreVoteResponse(resp) => {
                if resp.term > self.persistent.current_term {
                    // Someone is already in a newer term: we are the stale one.
                    self.step_down(resp.term);
                    return;
                }
                let won = match self.pre_vote.as_mut() {
                    // Ignore a denial, or a late answer to an earlier round.
                    Some(round) if resp.vote_granted && resp.request_term == round.term => {
                        round.tally.record_vote(from);
                        round.tally.has_majority()
                    }
                    _ => false,
                };
                if won {
                    self.begin_election();
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
                // One fsync for the whole batch, however many entries it carries.
                persist_must(self.persistence.append_batch(&effect.appended), "log append");

                // A configuration entry is in effect as soon as it is in the log,
                // and one that was truncated away must stop counting.
                self.track_config_entries(effect.truncated_from, &effect.appended);

                // Any request from a current-or-newer term comes from the
                // legitimate leader, even if its log didn't match ours - so
                // clients hitting this follower get a useful leader_hint.
                if req.term >= term_before {
                    self.known_leader = Some(leader_id);
                    // A live leader exists, so there is nothing to elect.
                    self.pre_vote = None;
                    // Record the contact NOW, after the durable writes above. The
                    // raft-core handler stamped it before they ran, so a slow disk
                    // (persisting the term alone costs two fsyncs and a rename)
                    // could make a contact from this very moment look older than
                    // the minimum election timeout, and this node would then grant
                    // pre-votes against a leader that is working fine.
                    self.volatile.last_heartbeat = std::time::Instant::now();
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
                // Any same-term answer, even a rejection, proves this follower is
                // reachable and still recognises us as its leader.
                self.leader_state.record_ack(from, std::time::Instant::now());
                // And it names the heartbeat round it answered, which is what lets
                // a waiting read prove leadership (see `complete_ready_reads`).
                self.leader_state.record_ack_seq(from, resp.seq);
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
            self.config.id,
            &self.membership,
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
        self.pre_vote = None;
        self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
    }

    /// True if this node has reason to believe a working leader exists: it IS the
    /// leader, or it heard from one within the minimum election timeout. Used to
    /// refuse pre-votes, so a node returning from a partition cannot force an
    /// election against a leader that never stopped.
    fn leader_is_active(&self) -> bool {
        self.volatile.role == Role::Leader
            || self.volatile.last_heartbeat.elapsed() < self.election_timeout.min
    }

    /// The election timer fired. Instead of immediately bumping the term and
    /// asking for real votes, first ask whether a majority WOULD vote for us. Only
    /// if they would does a real election start. A node that cannot reach anyone
    /// therefore never changes its term (or writes to disk) no matter how long it
    /// stays cut off.
    fn begin_pre_vote(&mut self) {
        // A node that is not a voting member never campaigns: a node waiting to be
        // admitted would otherwise elect itself leader of a cluster of one, and a
        // removed node would keep pestering the cluster it has left. It just keeps
        // waiting, in case the log later says it is a member after all.
        if !self.membership.contains(self.config.id) {
            self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
            return;
        }

        // With no one else to ask, a one-member cluster just elects itself.
        if self.membership.len() == 1 {
            self.begin_election();
            return;
        }

        let proposed_term = self.persistent.current_term + 1;
        self.pre_vote = Some(PreVoteRound {
            term: proposed_term,
            tally: VoteTally::new(self.config.id, &self.membership),
        });
        // If this round gets no majority, the timer fires again and we ask again.
        self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
        info!(proposed_term, "election timeout - asking peers whether they would vote");

        let req = PreVoteRequest {
            term: proposed_term,
            candidate_id: self.config.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        let peer_ids: Vec<NodeId> =
            self.membership.ids().filter(|&id| id != self.config.id).collect();
        for peer_id in peer_ids {
            self.send_to(peer_id, RpcMessage::PreVote(req.clone()));
        }
    }

    /// Check-quorum: a leader that no majority has answered within an election
    /// timeout cannot commit anything and may already have been replaced, so it
    /// steps down instead of accepting writes it can never finish. The wait is the
    /// MAXIMUM election timeout, so a leader never steps down earlier than the
    /// followers could have given up on it.
    fn check_quorum(&mut self) {
        if self.membership.len() == 1 && self.membership.contains(self.config.id) {
            return;
        }
        let has_quorum = self.leader_state.has_recent_quorum(
            self.config.id,
            &self.membership,
            std::time::Instant::now(),
            self.election_timeout.max,
        );
        if !has_quorum {
            warn!(
                term = self.persistent.current_term,
                "no majority has answered this leader within an election timeout - stepping down"
            );
            self.lose_leadership();
        }
    }

    /// Gives up leadership without any change of term (the term is only ever
    /// changed by hearing about a newer one), and answers every write that was
    /// waiting on this node.
    fn lose_leadership(&mut self) {
        self.volatile.role = Role::Follower;
        self.vote_tally = None;
        self.pre_vote = None;
        self.known_leader = None;
        self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
        self.abort_pending_requests();
    }

    fn begin_election(&mut self) {
        self.pre_vote = None;
        let req = start_election(&mut self.persistent, &mut self.volatile, &self.log, self.config.id);
        // The new term and our vote for ourselves must be durable before any
        // RequestVote leaves this node.
        self.persist_hard_state();
        self.known_leader = None;
        self.vote_tally = Some(VoteTally::new(self.config.id, &self.membership));
        self.election_deadline = Instant::now() + self.election_timeout.random_timeout();
        info!(term = self.persistent.current_term, "starting election");

        let peer_ids: Vec<NodeId> =
            self.membership.ids().filter(|&id| id != self.config.id).collect();
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

        self.pre_vote = None;

        let next = self.log.last_index() + 1;
        self.leader_state = LeaderState::default();
        let elected_at = std::time::Instant::now();
        for &peer_id in self.peer_outboxes.keys() {
            self.leader_state.next_index.insert(peer_id, next);
            self.leader_state.match_index.insert(peer_id, 0);
            // A majority just voted for us, so every peer starts with a fresh
            // acknowledgement; check-quorum only fires if that goes stale.
            self.leader_state.record_ack(peer_id, elected_at);
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
        self.noop_index = noop_index;
        self.append_to_wal(noop_index);

        self.broadcast_append_entries();

        // Covers the single-node-cluster case (majority of one, satisfied
        // immediately) where no AppendEntriesResponse will ever arrive to
        // trigger this otherwise.
        self.advance_leader_commit();
    }

    /// Sends AppendEntries (which doubles as a heartbeat) to every peer. Each call
    /// is one numbered "round"; followers echo the number back.
    fn broadcast_append_entries(&mut self) {
        self.heartbeat_seq += 1;
        let seq = self.heartbeat_seq;
        let peer_ids: Vec<NodeId> = self.peer_outboxes.keys().copied().collect();
        for peer_id in peer_ids {
            let next_index = *self.leader_state.next_index.get(&peer_id).unwrap_or(&1);
            // A follower that is further behind than the start of this node's log
            // needs the snapshot, not entries this node no longer has. Sending the
            // remainder with a gap before it would only be rejected, so send nothing.
            // (Sending the snapshot itself is InstallSnapshot, a later phase.)
            if !self.log.can_send_after(next_index.saturating_sub(1)) {
                debug!(
                    peer_id,
                    next_index,
                    log_starts_at = self.log.first_index(),
                    "peer needs entries that have been compacted away - cannot replicate to it yet"
                );
                continue;
            }
            let req = build_append_entries(
                &self.persistent,
                &self.log,
                self.config.id,
                next_index,
                self.volatile.commit_index,
                seq,
            );
            self.send_to(peer_id, RpcMessage::AppendEntries(req));
        }
    }

    /// Most unfinished writes (and, separately, most waiting reads) this node will
    /// hold at once. Past this the node says so immediately instead of queueing
    /// more: if the majority is unreachable, an ever-growing pile of requests
    /// that can never complete helps no one.
    fn max_pending(&self) -> usize {
        self.config.limits.channel_capacity
    }

    /// One-at-a-time form of `handle_client_commands`, for tests.
    #[cfg(test)]
    fn handle_client_command(&mut self, cmd: ClientCommand) {
        self.handle_client_commands(vec![cmd]);
    }

    /// Handles a batch of client writes as one unit: every accepted command is
    /// appended to the log, the whole batch is made durable with a SINGLE fsync,
    /// and one round of replication carries all of it. Nothing is acknowledged
    /// until the entries are durable and committed, exactly as before; batching
    /// only lets requests that arrived together share the cost of the disk sync.
    fn handle_client_commands(&mut self, commands: Vec<ClientCommand>) {
        if self.volatile.role != Role::Leader {
            for cmd in commands {
                let _ = cmd
                    .respond_to
                    .send(ClientCommandResult::NotLeader { leader_hint: self.known_leader });
            }
            return;
        }

        let mut first_new_index: Option<u64> = None;
        for cmd in commands {
            if self.pending_commands.len() >= self.max_pending() {
                let _ = cmd.respond_to.send(ClientCommandResult::Overloaded);
                continue;
            }
            let encoded = match cmd.command.encode() {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(error = %e, "failed to encode client command");
                    continue;
                }
            };
            let index = self.log.append(self.persistent.current_term, encoded);
            first_new_index.get_or_insert(index);
            self.pending_commands.insert(index, cmd.respond_to);
        }
        let Some(first) = first_new_index else { return };

        // Durable BEFORE anything is replicated or counted toward a majority.
        let new_entries = self.log.entries_after(first - 1);
        persist_must(self.persistence.append_batch(&new_entries), "log append");

        self.broadcast_append_entries();

        // A single-node cluster (or a leader whose own append already satisfies
        // a majority) never receives an AppendEntriesResponse to trigger this -
        // the commit check must also run right after the leader's own append.
        self.advance_leader_commit();
    }

    /// One-at-a-time form of `handle_client_queries`, for tests.
    #[cfg(test)]
    fn handle_client_query(&mut self, query: ClientQuery) {
        self.handle_client_queries(vec![query]);
    }

    /// Handles a batch of reads. Only the (believed) leader answers; followers
    /// redirect, same as writes.
    ///
    /// In `Linearizable` mode the reads are parked until `complete_ready_reads` can
    /// prove two things: that a majority still recognised this node as leader
    /// after they arrived, and that this node has applied everything that was
    /// committed when they arrived. The whole batch shares ONE heartbeat round,
    /// which is sound because every read in it arrived before that round began. In
    /// `Local` mode each is answered at once from local state.
    fn handle_client_queries(&mut self, queries: Vec<ClientQuery>) {
        if self.volatile.role != Role::Leader {
            for query in queries {
                let _ = query
                    .respond_to
                    .send(QueryResult::NotLeader { leader_hint: self.known_leader });
            }
            return;
        }

        if self.config.read_mode == ReadMode::Local {
            for query in queries {
                let value = self.kv_store.get(&query.key).cloned();
                let _ = query.respond_to.send(QueryResult::Value(value));
            }
            return;
        }

        // The index these reads must see. Never below this leader's own no-op:
        // until that entry commits, our commit index may lag what earlier leaders
        // committed, and only the no-op committing settles that.
        let read_index = self.volatile.commit_index.max(self.noop_index);

        let mut accepted = Vec::new();
        for query in queries {
            if self.pending_reads.len() + accepted.len() >= self.max_pending() {
                let _ = query.respond_to.send(QueryResult::Overloaded);
            } else {
                accepted.push(query);
            }
        }
        if accepted.is_empty() {
            return;
        }

        // Start a heartbeat round NOW, after every one of these reads arrived. A
        // majority answering this round (or any later one) proves we were still
        // the leader at a moment no earlier than their arrival. An older round
        // would prove nothing: it could have been answered before a newer leader
        // existed.
        self.broadcast_append_entries();
        let required_seq = self.heartbeat_seq;

        for query in accepted {
            self.pending_reads.push(PendingRead {
                key: query.key,
                respond_to: query.respond_to,
                read_index,
                required_seq,
            });
        }
    }

    /// Answers every waiting read that is now safe to answer. A read is safe once
    /// (1) a majority has answered a heartbeat round that began after it arrived,
    /// so leadership is proven, and (2) the state machine has applied up to the
    /// read's index, so the value it reads includes everything committed before
    /// the read began. Called at the end of every event-loop pass, after
    /// committed entries are applied.
    fn complete_ready_reads(&mut self) {
        if self.pending_reads.is_empty() || self.volatile.role != Role::Leader {
            return;
        }
        let last_applied = self.volatile.last_applied;

        let mut still_waiting = Vec::new();
        for read in self.pending_reads.drain(..) {
            let leadership_proven = self.leader_state.has_quorum_acked(
                read.required_seq,
                self.config.id,
                &self.membership,
            );
            if leadership_proven && last_applied >= read.read_index {
                let value = self.kv_store.get(&read.key).cloned();
                let _ = read.respond_to.send(QueryResult::Value(value));
            } else {
                still_waiting.push(read);
            }
        }
        self.pending_reads = still_waiting;
    }

    // ---- snapshots and log compaction ----------------------------------------

    /// Takes a snapshot once enough entries have been applied since the last one,
    /// then trims whatever part of the log is safe to discard.
    ///
    /// A snapshot is optional: it only saves disk and replay time. So a failure to
    /// write one is logged and retried later, never fatal, unlike a failed write of
    /// the log itself.
    fn maybe_snapshot(&mut self) {
        let threshold = self.config.snapshot_threshold;
        if threshold == 0 {
            return;
        }
        let applied = self.volatile.last_applied;
        let last_snapshot = self.persistence.snapshot_base();
        if applied >= last_snapshot + threshold && applied >= self.snapshot_retry_after {
            self.take_snapshot(applied);
        }
        self.compact_log();
    }

    /// Snapshots the state machine as of `index`, which must be what it has applied.
    fn take_snapshot(&mut self, index: u64) {
        let Some(term) = self.log.term_at(index) else {
            return;
        };
        let metadata = storage::SnapshotMetadata {
            last_included_index: index,
            last_included_term: term,
            membership: self.history.membership_at(index),
        };
        let started = Instant::now();
        match self.persistence.save_snapshot_only(metadata, &self.kv_store) {
            Ok(()) => {
                self.snapshots_taken += 1;
                info!(index, keys = self.kv_store.len(), took_ms = started.elapsed().as_millis() as u64, "snapshot taken");
            }
            Err(e) => {
                error!(error = %e, index, "could not write a snapshot - carrying on without one");
                self.snapshot_retry_after = index + self.config.snapshot_threshold;
            }
        }
    }

    /// Discards the part of the log that the newest snapshot covers and nobody
    /// needs, keeping `snapshot_retain` entries behind the snapshot.
    ///
    /// A leader additionally keeps everything its slowest peer, learners included,
    /// has not yet confirmed: that peer is caught up by sending it log entries, and
    /// entries thrown away cannot be sent.
    fn compact_log(&mut self) {
        let snapshot_index = self.persistence.snapshot_base();
        if snapshot_index <= self.log.base_index() {
            return;
        }
        let mut upto = snapshot_index.saturating_sub(self.config.snapshot_retain);
        if self.volatile.role == Role::Leader {
            upto = upto.min(self.slowest_peer_match());
        }
        if upto <= self.log.base_index() {
            return;
        }
        match self.persistence.compact_log_through(upto) {
            Ok(()) => {
                self.log.compact_through(upto).expect("only entries that exist are compacted");
                self.history.compact_through(upto);
                debug!(upto, "log compacted");
            }
            Err(e) => {
                error!(error = %e, upto, "could not trim the log - it will be retried");
            }
        }
    }

    /// The lowest index any other node (voter or learner) is known to hold; entries
    /// at or below it have been replicated everywhere. `u64::MAX` if there are no
    /// other nodes.
    fn slowest_peer_match(&self) -> u64 {
        self.membership
            .ids()
            .chain(self.learners.keys().copied())
            .filter(|&id| id != self.config.id)
            .map(|id| self.leader_state.match_index.get(&id).copied().unwrap_or(0))
            .min()
            .unwrap_or(u64::MAX)
    }

    // ---- membership ---------------------------------------------------------

    /// Why a membership change cannot be started right now, if it cannot.
    ///
    /// Two rules make single-server changes safe. Only ONE change may be in flight,
    /// so any majority of the old configuration overlaps any majority of the new
    /// one. And a new leader must first commit an entry of its own term (its no-op),
    /// because until then it cannot know whether the configuration it sees in its log
    /// has itself been committed.
    fn admin_rejection(&self, change: &MembershipChange) -> Option<AdminRejection> {
        let change_in_log_uncommitted = self
            .history
            .last_change_index()
            .map_or(false, |index| index > self.volatile.commit_index);
        if self.pending_admin.is_some() || change_in_log_uncommitted {
            return Some(AdminRejection::ChangeInProgress);
        }
        if self.volatile.commit_index < self.noop_index {
            return Some(AdminRejection::NotReady);
        }
        match change {
            MembershipChange::Add { id, addr } => {
                if addr.trim().is_empty() {
                    Some(AdminRejection::BadAddress)
                } else if self.membership.contains(*id) {
                    Some(AdminRejection::AlreadyMember)
                } else if self.log.base_index() > 0 {
                    Some(AdminRejection::LogCompacted)
                } else {
                    None
                }
            }
            MembershipChange::Remove { id } => {
                if !self.membership.contains(*id) {
                    Some(AdminRejection::NotMember)
                } else if self.membership.len() == 1 {
                    Some(AdminRejection::LastMember)
                } else {
                    None
                }
            }
        }
    }

    fn handle_client_admin(&mut self, admin: ClientAdmin) {
        if self.volatile.role != Role::Leader {
            let _ = admin.respond_to.send(AdminResult::NotLeader { leader_hint: self.known_leader });
            return;
        }
        if let Some(rejection) = self.admin_rejection(&admin.change) {
            let _ = admin.respond_to.send(AdminResult::Rejected(rejection));
            return;
        }

        match admin.change {
            MembershipChange::Add { id, addr } => {
                info!(id, %addr, "adding a member: bringing it up to date as a learner first");
                // Open the connection and start replicating to it, but do NOT let it
                // vote or count toward any majority yet.
                let outbox = (self.connector)(id, &addr);
                self.peer_outboxes.insert(id, outbox);
                self.leader_state.next_index.insert(id, self.log.last_index() + 1);
                self.leader_state.match_index.insert(id, 0);
                self.leader_state.last_ack.remove(&id);
                self.leader_state.ack_seq.remove(&id);
                self.learners.insert(id, addr.clone());
                self.pending_admin = Some(PendingAdmin {
                    respond_to: admin.respond_to,
                    stage: AdminStage::CatchingUp {
                        id,
                        addr,
                        deadline: Instant::now() + self.config.catch_up_timeout,
                    },
                });
                self.broadcast_append_entries();
            }
            MembershipChange::Remove { id } => {
                info!(id, "removing a member");
                let index = self.append_config_entry(Command::RemoveNode { id });
                self.pending_admin = Some(PendingAdmin {
                    respond_to: admin.respond_to,
                    stage: AdminStage::Committing { index, removes_self: id == self.config.id },
                });
                self.broadcast_append_entries();
                // The new, smaller configuration is already in effect, and it may
                // be satisfied by what is already replicated.
                self.advance_leader_commit();
            }
        }
    }

    /// Moves the in-flight membership change along. Run after every event, because
    /// what it waits for (a learner catching up, an entry committing) is changed by
    /// ordinary replication traffic.
    fn drive_admin(&mut self) {
        let Some(pending) = self.pending_admin.take() else { return };
        if self.volatile.role != Role::Leader {
            // Leadership was lost; `abort_pending_requests` answers the caller.
            self.pending_admin = Some(pending);
            return;
        }
        let PendingAdmin { respond_to, stage } = pending;

        match stage {
            AdminStage::CatchingUp { id, addr, deadline } => {
                if respond_to.is_closed() || Instant::now() >= deadline {
                    // The caller gave up, or the node is not catching up. Nothing was
                    // logged, so there is nothing to undo but the learner itself.
                    warn!(id, "new node did not catch up in time - abandoning the add");
                    self.drop_learner(id);
                    let _ = respond_to.send(AdminResult::Rejected(AdminRejection::CatchUpFailed));
                    return;
                }
                let matched = self.leader_state.match_index.get(&id).copied().unwrap_or(0);
                if matched >= self.volatile.commit_index {
                    info!(id, matched, "learner has caught up - admitting it as a voting member");
                    let index = self.append_config_entry(Command::AddNode { id, addr });
                    self.pending_admin = Some(PendingAdmin {
                        respond_to,
                        stage: AdminStage::Committing { index, removes_self: false },
                    });
                    self.broadcast_append_entries();
                    self.advance_leader_commit();
                } else {
                    self.pending_admin = Some(PendingAdmin {
                        respond_to,
                        stage: AdminStage::CatchingUp { id, addr, deadline },
                    });
                }
            }
            AdminStage::Committing { index, removes_self } => {
                if self.volatile.last_applied >= index {
                    let members = self.membership.ids().collect();
                    let _ = respond_to.send(AdminResult::Applied { members });
                    if removes_self {
                        // The change that took this node out of the cluster is now
                        // committed. It led the cluster through its own removal; now it
                        // hands over. It is not a member, so it will not campaign again.
                        info!("this node's removal is committed - stepping down");
                        self.lose_leadership();
                    }
                } else {
                    self.pending_admin = Some(PendingAdmin {
                        respond_to,
                        stage: AdminStage::Committing { index, removes_self },
                    });
                }
            }
        }
    }

    /// Appends a configuration entry to this leader's log, makes it durable, and
    /// puts it into effect. Returns its index.
    fn append_config_entry(&mut self, command: Command) -> u64 {
        let change = command.config_change().expect("only membership commands come through here");
        let encoded = command.encode().expect("membership commands always encode");
        let index = self.log.append(self.persistent.current_term, encoded);
        self.append_to_wal(index);
        self.history.record(index, change);
        self.refresh_membership();
        index
    }

    /// Keeps the configuration in step with what just happened to the log: entries
    /// that were truncated away stop counting, entries that were appended start.
    fn track_config_entries(&mut self, truncated_from: Option<u64>, appended: &[LogEntry]) {
        let mut changed = false;
        if let Some(from_index) = truncated_from {
            let before = self.history.len();
            self.history.truncate_from(from_index);
            changed |= self.history.len() != before;
        }
        for entry in appended {
            if let Some(change) = config_change_of(&entry.command) {
                self.history.record(entry.index, change);
                changed = true;
            }
        }
        if changed {
            self.refresh_membership();
        }
    }

    /// Recomputes who votes, and makes sure there is a connection to every member.
    fn refresh_membership(&mut self) {
        self.membership = self.history.current();
        let membership = &self.membership;
        // A learner that has just become a member is a learner no more.
        self.learners.retain(|id, _| !membership.contains(*id));
        self.connect_missing_peers();
    }

    /// Opens a connection to every member this node is not yet connected to. The
    /// address comes from the configuration entry that added the member.
    fn connect_missing_peers(&mut self) {
        let missing: Vec<(NodeId, String)> = self
            .membership
            .iter()
            .filter(|(id, _)| *id != self.config.id && !self.peer_outboxes.contains_key(id))
            .map(|(id, addr)| (id, addr.to_string()))
            .collect();
        for (id, addr) in missing {
            if addr.trim().is_empty() {
                warn!(id, "member has no known address - cannot connect to it");
                continue;
            }
            info!(id, %addr, "connecting to a new member");
            let outbox = (self.connector)(id, &addr);
            self.peer_outboxes.insert(id, outbox);
            // Only meaningful on a leader, harmless elsewhere (reset on election).
            self.leader_state.next_index.entry(id).or_insert(self.log.last_index() + 1);
            self.leader_state.match_index.entry(id).or_insert(0);
        }
    }

    /// At startup: stop talking to peers the log says are no longer in the cluster.
    fn drop_peers_outside_membership(&mut self) {
        let membership = &self.membership;
        let learners = &self.learners;
        self.peer_outboxes.retain(|id, _| membership.contains(*id) || learners.contains_key(id));
    }

    /// A removal has committed. Stop sending to the node (it is not a member, and
    /// the leader must not keep its bookkeeping for it forever), unless it has been
    /// added back since.
    fn forget_removed_peer(&mut self, id: NodeId) {
        if self.membership.contains(id) || self.learners.contains_key(&id) {
            return;
        }
        self.peer_outboxes.remove(&id);
        self.leader_state.next_index.remove(&id);
        self.leader_state.match_index.remove(&id);
        self.leader_state.last_ack.remove(&id);
        self.leader_state.ack_seq.remove(&id);
    }

    fn drop_learner(&mut self, id: NodeId) {
        if self.learners.remove(&id).is_some() && !self.membership.contains(id) {
            self.peer_outboxes.remove(&id);
            self.leader_state.next_index.remove(&id);
            self.leader_state.match_index.remove(&id);
            self.leader_state.last_ack.remove(&id);
            self.leader_state.ack_seq.remove(&id);
        }
    }

    fn drop_learners(&mut self) {
        let ids: Vec<NodeId> = self.learners.keys().copied().collect();
        for id in ids {
            self.drop_learner(id);
        }
    }

    fn apply_committed_entries(&mut self) {
        while self.volatile.last_applied < self.volatile.commit_index {
            let next_index = self.volatile.last_applied + 1;
            let Some(entry) = self.log.get(next_index) else { break };
            let result = match Command::decode(&entry.command) {
                Ok(command) => {
                    let removed = match command.config_change() {
                        Some(ConfigChange::Remove { id }) => Some(id),
                        _ => None,
                    };
                    let result = self.kv_store.apply(&command);
                    if let Some(id) = removed {
                        self.forget_removed_peer(id);
                    }
                    result
                }
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

    /// Queues a message for a peer without ever waiting. If that peer's queue is
    /// full (it is down, or too slow to keep up) the message is dropped. That is
    /// safe: Raft is built to tolerate lost messages, and the leader resends what
    /// matters on its next heartbeat. What must never happen is the event loop
    /// stalling on one slow peer while the rest of the cluster waits.
    fn send_to(&mut self, peer_id: NodeId, msg: RpcMessage) {
        let Some(tx) = self.peer_outboxes.get(&peer_id) else { return };
        match tx.try_send(msg) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped_messages += 1;
                // Log on the 1st, 2nd, 4th, 8th... drop: visible, but not a flood.
                if self.dropped_messages.is_power_of_two() {
                    warn!(
                        peer_id,
                        dropped_total = self.dropped_messages,
                        "peer queue full - dropping messages to it"
                    );
                }
            }
            // The peer's connection task is gone (node shutting down).
            Err(mpsc::error::TrySendError::Closed(_)) => {}
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
            wal_syncs: self.persistence.wal_sync_count(),
            dropped_messages: self.dropped_messages,
            members: self.membership.ids().collect(),
            learners: self.learners.keys().copied().collect(),
            snapshot_index: self.persistence.snapshot_base(),
            log_base: self.log.base_index(),
            snapshots_taken: self.snapshots_taken,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Limits;
    use raft_core::{
        AppendEntriesRequest, AppendEntriesResponse, LogEntry, PreVoteResponse,
        RequestVoteRequest, RequestVoteResponse,
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

    type Outputs = HashMap<NodeId, mpsc::Receiver<RpcMessage>>;

    /// Builds node 1 over `dir` with the given peers, recovering whatever is
    /// already stored there (so calling it twice on one directory is a restart).
    /// Returns the receivers for each peer's outbound channel so a test can read
    /// exactly what this node replied.
    fn make_cluster_in(dir: &Path, peer_ids: &[NodeId]) -> (Cluster, Outputs) {
        make_cluster_with_mode(dir, peer_ids, ReadMode::Linearizable)
    }

    fn make_cluster_with_mode(
        dir: &Path,
        peer_ids: &[NodeId],
        read_mode: ReadMode,
    ) -> (Cluster, Outputs) {
        make_cluster_full(dir, peer_ids, read_mode, Limits::default())
    }

    fn make_cluster_with_limits(
        dir: &Path,
        peer_ids: &[NodeId],
        limits: Limits,
    ) -> (Cluster, Outputs) {
        make_cluster_full(dir, peer_ids, ReadMode::Linearizable, limits)
    }

    fn make_cluster_full(
        dir: &Path,
        peer_ids: &[NodeId],
        read_mode: ReadMode,
        limits: Limits,
    ) -> (Cluster, Outputs) {
        let (cluster, outputs) =
            make_cluster_core(dir, peer_ids, read_mode, limits, discarding_connector());
        (cluster, outputs)
    }

    fn make_cluster_core(
        dir: &Path,
        peer_ids: &[NodeId],
        read_mode: ReadMode,
        limits: Limits,
        connector: PeerConnector,
    ) -> (Cluster, Outputs) {
        let peers = peer_ids.iter().map(|&id| (id, String::new())).collect();
        let config = NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            metrics_addr: "127.0.0.1:0".to_string(),
            peers,
            storage_dir: dir.to_path_buf(),
            read_mode,
            limits,
            join: false,
            catch_up_timeout: crate::config::DEFAULT_CATCH_UP_TIMEOUT,
            snapshot_threshold: 0,
            snapshot_retain: 0,
        };
        let mut outboxes = HashMap::new();
        let mut outputs = HashMap::new();
        for &id in peer_ids {
            let (tx, rx) = mpsc::channel(limits.channel_capacity);
            outboxes.insert(id, tx);
            outputs.insert(id, rx);
        }
        let (_inbox_tx, inbox_rx) = mpsc::channel(limits.channel_capacity);
        let recovered = Persistence::open(dir).expect("storage should open");
        let (cluster, _handle) = Cluster::new(config, recovered, outboxes, inbox_rx, connector);
        (cluster, outputs)
    }

    /// A connector whose connections lead nowhere: whatever the node sends through
    /// them is dropped. For tests that never look at traffic to a new member.
    fn discarding_connector() -> PeerConnector {
        Box::new(|_, _| mpsc::channel(1024).0)
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
            seq: 0,
        })
    }

    /// Like `append_request`, but the leader also reports how far it has committed.
    fn append_request_committing(
        term: Term,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> RpcMessage {
        match append_request(term, leader_id, prev_log_index, prev_log_term, entries) {
            RpcMessage::AppendEntries(req) => {
                RpcMessage::AppendEntries(AppendEntriesRequest { leader_commit, ..req })
            }
            _ => unreachable!(),
        }
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
        put_command_with_receiver(key, value).0
    }

    /// Like `put_command`, but keeps the receiving end so a test can see what
    /// the client would have been told.
    fn put_command_with_receiver(
        key: &str,
        value: &str,
    ) -> (ClientCommand, oneshot::Receiver<ClientCommandResult>) {
        let (respond_to, receiver) = oneshot::channel();
        let command = ClientCommand {
            command: Command::Put { key: key.into(), value: value.as_bytes().to_vec() },
            respond_to,
        };
        (command, receiver)
    }

    /// An AppendEntries answer for heartbeat round `seq`. A rejection still proves
    /// the follower accepted this leader's term.
    fn append_ack(term: Term, seq: u64, success: bool, match_index: u64) -> RpcMessage {
        RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
            term,
            success,
            conflict_index: if success { None } else { Some(0) },
            match_index,
            seq,
        })
    }

    fn read_query(key: &str) -> (ClientQuery, oneshot::Receiver<QueryResult>) {
        let (respond_to, receiver) = oneshot::channel();
        (ClientQuery { key: key.into(), respond_to }, receiver)
    }

    /// A 3-node cluster whose node 1 is leader in term 1 and has committed and
    /// applied its own no-op (index 1), so reads are not held up by it.
    fn settled_leader(dir: &TestDir) -> Cluster {
        let mut cluster = build_cluster(dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader(); // heartbeat round 1, no-op at index 1
        cluster.handle_rpc(2, append_ack(1, 1, true, 1)); // node 2 stores the no-op: committed
        cluster.apply_committed_entries();
        assert_eq!(cluster.volatile.last_applied, 1);
        cluster
    }

    fn higher_term_response(term: Term) -> RpcMessage {
        RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
            term,
            success: false,
            conflict_index: None,
            match_index: 0,
            seq: 0,
        })
    }

    fn pre_vote_request(term: Term, candidate_id: NodeId) -> RpcMessage {
        RpcMessage::PreVote(PreVoteRequest {
            term,
            candidate_id,
            last_log_index: 0,
            last_log_term: 0,
        })
    }

    fn pre_vote_response(term: Term, vote_granted: bool, request_term: Term) -> RpcMessage {
        RpcMessage::PreVoteResponse(PreVoteResponse { term, vote_granted, request_term })
    }

    /// Makes the node behave as if it last heard from a leader long ago.
    fn forget_leader_contact(cluster: &mut Cluster) {
        let long_ago = std::time::Instant::now()
            .checked_sub(Duration::from_secs(5))
            .unwrap_or_else(std::time::Instant::now);
        cluster.volatile.last_heartbeat = long_ago;
    }

    /// Makes every follower's last acknowledgement of this leader ancient.
    fn age_all_acks(cluster: &mut Cluster) {
        let long_ago = std::time::Instant::now()
            .checked_sub(Duration::from_secs(5))
            .unwrap_or_else(std::time::Instant::now);
        for acked_at in cluster.leader_state.last_ack.values_mut() {
            *acked_at = long_ago;
        }
    }

    /// Every message this node has queued for `peer`, oldest first.
    fn drain(outputs: &mut Outputs, peer: NodeId) -> Vec<RpcMessage> {
        let rx = outputs.get_mut(&peer).unwrap();
        let mut messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }
        messages
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
        // A read is parked until leadership is proven. A single node needs no peers
        // to prove it, so the end-of-pass steps the event loop runs are enough.
        cluster.apply_committed_entries();
        cluster.complete_ready_reads();
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
                seq: 0,
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
                seq: 0,
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
                    seq: 0,
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

    // ---- request lifecycle --------------------------------------------------

    #[test]
    fn losing_leadership_fails_every_write_that_was_waiting() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        let (first, mut first_rx) = put_command_with_receiver("a", "1");
        let (second, mut second_rx) = put_command_with_receiver("b", "2");
        cluster.handle_client_command(first);
        cluster.handle_client_command(second);
        // Nothing is committed (no follower has answered), so both are waiting.
        assert!(first_rx.try_recv().is_err());
        assert!(second_rx.try_recv().is_err());

        // A response from a newer term deposes this leader.
        cluster.handle_rpc(2, higher_term_response(7));

        assert_eq!(cluster.volatile.role, Role::Follower);
        assert!(matches!(first_rx.try_recv(), Ok(ClientCommandResult::LeadershipLost)));
        assert!(matches!(second_rx.try_recv(), Ok(ClientCommandResult::LeadershipLost)));
        assert!(cluster.pending_commands.is_empty());
    }

    #[test]
    fn a_vote_request_from_a_newer_term_also_fails_waiting_writes() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        let (command, mut receiver) = put_command_with_receiver("a", "1");
        cluster.handle_client_command(command);

        // Leadership is lost inside the vote handler, not via step_down().
        cluster.handle_rpc(3, vote_request(9, 3));

        assert_eq!(cluster.volatile.role, Role::Follower);
        assert!(matches!(receiver.try_recv(), Ok(ClientCommandResult::LeadershipLost)));
    }

    #[test]
    fn an_append_from_a_newer_leader_also_fails_waiting_writes() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        let (command, mut receiver) = put_command_with_receiver("a", "1");
        cluster.handle_client_command(command);

        cluster.handle_rpc(2, append_request(5, 2, 0, 0, vec![]));

        assert_eq!(cluster.volatile.role, Role::Follower);
        assert!(matches!(receiver.try_recv(), Ok(ClientCommandResult::LeadershipLost)));
    }

    #[test]
    fn writes_keep_waiting_while_leadership_is_intact() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        let (command, mut receiver) = put_command_with_receiver("a", "1");
        cluster.handle_client_command(command);

        // A same-term response that does not carry the entry changes nothing.
        cluster.handle_rpc(
            2,
            RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: 1,
                success: true,
                conflict_index: None,
                match_index: 1,
                seq: 0,
            }),
        );

        assert_eq!(cluster.volatile.role, Role::Leader);
        assert!(receiver.try_recv().is_err(), "still waiting, not failed");
        assert_eq!(cluster.pending_commands.len(), 1);
    }

    #[test]
    fn abandoned_requests_are_pruned_but_live_ones_are_kept() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        let (abandoned, abandoned_rx) = put_command_with_receiver("a", "1");
        let (live, _live_rx) = put_command_with_receiver("b", "2");
        cluster.handle_client_command(abandoned);
        cluster.handle_client_command(live);
        assert_eq!(cluster.pending_commands.len(), 2);

        // The first caller gives up (e.g. its HTTP request timed out).
        drop(abandoned_rx);
        cluster.prune_abandoned_requests();

        assert_eq!(cluster.pending_commands.len(), 1);
        // The entry itself is still in the log: only the reply slot was released.
        assert_eq!(cluster.log.last_index(), 3);
    }

    // ---- pre-vote -----------------------------------------------------------

    #[test]
    fn an_election_timeout_starts_a_pre_vote_and_changes_no_state() {
        let dir = TestDir::new();
        {
            let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
            cluster.begin_pre_vote();

            assert_eq!(cluster.persistent.current_term, 0, "term must not move");
            assert_eq!(cluster.persistent.voted_for, None, "no vote may be cast");
            assert_eq!(cluster.volatile.role, Role::Follower);

            for peer in [2, 3] {
                let sent = drain(&mut outputs, peer);
                assert_eq!(sent.len(), 1);
                assert!(
                    matches!(&sent[0], RpcMessage::PreVote(req) if req.term == 1 && req.candidate_id == 1),
                    "peer {peer} should be asked about term 1, got {sent:?}"
                );
            }
        }
        // And nothing was written to disk either: a node that is cut off and
        // timing out over and over costs no fsyncs.
        let cluster = build_cluster(&dir, &[2, 3]);
        assert_eq!(cluster.persistent.current_term, 0);
    }

    #[test]
    fn a_pre_vote_majority_starts_the_real_election() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.begin_pre_vote();
        drain(&mut outputs, 2);
        drain(&mut outputs, 3);

        // One peer's yes plus our own makes 2 of 3.
        cluster.handle_rpc(2, pre_vote_response(0, true, 1));

        assert_eq!(cluster.persistent.current_term, 1);
        assert_eq!(cluster.persistent.voted_for, Some(1));
        assert_eq!(cluster.volatile.role, Role::Candidate);
        let sent = drain(&mut outputs, 3);
        assert!(
            sent.iter().any(|m| matches!(m, RpcMessage::RequestVote(req) if req.term == 1)),
            "real vote requests must follow, got {sent:?}"
        );
    }

    #[test]
    fn denied_pre_votes_never_start_an_election() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.begin_pre_vote();

        cluster.handle_rpc(2, pre_vote_response(0, false, 1));
        cluster.handle_rpc(3, pre_vote_response(0, false, 1));

        assert_eq!(cluster.persistent.current_term, 0);
        assert_eq!(cluster.volatile.role, Role::Follower);
    }

    #[test]
    fn a_late_answer_to_an_earlier_pre_vote_round_is_ignored() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.begin_pre_vote(); // proposes term 1

        cluster.handle_rpc(2, pre_vote_response(0, true, 99));

        assert_eq!(cluster.persistent.current_term, 0);
        assert_eq!(cluster.volatile.role, Role::Follower);
    }

    #[test]
    fn a_pre_vote_denial_from_a_newer_term_makes_the_asker_step_down_to_it() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.begin_pre_vote();

        cluster.handle_rpc(2, pre_vote_response(8, false, 1));

        assert_eq!(cluster.persistent.current_term, 8, "learns it was out of date");
        assert!(cluster.pre_vote.is_none());
    }

    #[test]
    fn hearing_from_a_leader_cancels_a_pre_vote_in_progress() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.begin_pre_vote();
        assert!(cluster.pre_vote.is_some());

        cluster.handle_rpc(2, append_request(1, 2, 0, 0, vec![]));

        assert!(cluster.pre_vote.is_none());
    }

    #[test]
    fn answering_a_pre_vote_has_no_side_effects_on_the_voter() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 3;
        forget_leader_contact(&mut cluster);
        let deadline_before = cluster.election_deadline;

        cluster.handle_rpc(2, pre_vote_request(4, 2));

        let sent = drain(&mut outputs, 2);
        assert!(
            matches!(&sent[..], [RpcMessage::PreVoteResponse(r)] if r.vote_granted && r.term == 3 && r.request_term == 4),
            "expected a single grant, got {sent:?}"
        );
        assert_eq!(cluster.persistent.current_term, 3, "term must not change");
        assert_eq!(cluster.persistent.voted_for, None, "vote must not be spent");
        assert_eq!(cluster.volatile.role, Role::Follower);
        assert_eq!(cluster.election_deadline, deadline_before, "timer must not be reset");
    }

    #[test]
    fn a_follower_that_recently_heard_from_a_leader_denies_pre_votes() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        // A real leader (node 2) is heard from right now.
        cluster.handle_rpc(2, append_request(1, 2, 0, 0, vec![]));
        drain(&mut outputs, 2);

        cluster.handle_rpc(3, pre_vote_request(2, 3));

        let sent = drain(&mut outputs, 3);
        assert!(
            matches!(&sent[..], [RpcMessage::PreVoteResponse(r)] if !r.vote_granted),
            "a node with a live leader must refuse, got {sent:?}"
        );
    }

    #[test]
    fn a_leader_denies_pre_votes() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        drain(&mut outputs, 2);

        cluster.handle_rpc(2, pre_vote_request(2, 2));

        let sent = drain(&mut outputs, 2);
        assert!(
            matches!(&sent[..], [RpcMessage::PreVoteResponse(r)] if !r.vote_granted),
            "a working leader must refuse, got {sent:?}"
        );
        assert_eq!(cluster.volatile.role, Role::Leader);
    }

    #[test]
    fn a_single_node_cluster_skips_the_pre_vote() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        cluster.begin_pre_vote();
        assert_eq!(cluster.volatile.role, Role::Leader);
    }

    // ---- check-quorum -------------------------------------------------------

    #[test]
    fn a_leader_no_majority_has_answered_steps_down_and_fails_its_waiting_writes() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        let (command, mut receiver) = put_command_with_receiver("a", "1");
        cluster.handle_client_command(command);

        age_all_acks(&mut cluster);
        cluster.check_quorum();

        assert_eq!(cluster.volatile.role, Role::Follower);
        assert_eq!(cluster.persistent.current_term, 1, "stepping down must not touch the term");
        assert!(matches!(receiver.try_recv(), Ok(ClientCommandResult::LeadershipLost)));
        assert_eq!(cluster.known_leader, None);
    }

    #[test]
    fn a_leader_that_a_majority_answered_recently_keeps_its_leadership() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        age_all_acks(&mut cluster);

        // Node 2 answers now: the leader plus node 2 is 2 of 3.
        cluster.handle_rpc(
            2,
            RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: 1,
                success: true,
                conflict_index: None,
                match_index: 1,
                seq: 0,
            }),
        );
        cluster.check_quorum();

        assert_eq!(cluster.volatile.role, Role::Leader);
    }

    #[test]
    fn a_rejection_still_counts_as_the_follower_being_reachable() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        age_all_acks(&mut cluster);

        cluster.handle_rpc(
            2,
            RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: 1,
                success: false,
                conflict_index: Some(0),
                match_index: 0,
                seq: 0,
            }),
        );
        cluster.check_quorum();

        assert_eq!(cluster.volatile.role, Role::Leader);
    }

    #[test]
    fn a_response_from_an_earlier_term_does_not_count_as_contact() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 2;
        cluster.become_leader();
        age_all_acks(&mut cluster);

        cluster.handle_rpc(
            2,
            RpcMessage::AppendEntriesResponse(AppendEntriesResponse {
                term: 1,
                success: true,
                conflict_index: None,
                match_index: 1,
                seq: 0,
            }),
        );
        cluster.check_quorum();

        assert_eq!(cluster.volatile.role, Role::Follower, "a stale response proves nothing");
    }

    #[test]
    fn a_single_node_leader_never_steps_down() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        cluster.begin_election();
        cluster.check_quorum();
        assert_eq!(cluster.volatile.role, Role::Leader);
    }

    // ---- linearizable reads (ReadIndex) -------------------------------------

    #[test]
    fn a_read_is_not_answered_until_a_majority_confirms_a_round_that_began_after_it() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);

        let (query, mut reply) = read_query("k");
        cluster.handle_client_query(query);
        assert_eq!(cluster.heartbeat_seq, 2, "the read must start a fresh round");
        cluster.complete_ready_reads();
        assert!(reply.try_recv().is_err(), "nobody has answered round 2 yet");

        // A late answer to the OLD round 1 proves nothing about this moment.
        cluster.handle_rpc(2, append_ack(1, 1, true, 1));
        cluster.complete_ready_reads();
        assert!(reply.try_recv().is_err(), "an answer to an earlier round must not count");

        // Node 2 answers round 2: the leader plus node 2 is a majority of 3.
        cluster.handle_rpc(2, append_ack(1, 2, true, 1));
        cluster.complete_ready_reads();
        assert!(matches!(reply.try_recv(), Ok(QueryResult::Value(None))));
    }

    #[test]
    fn a_read_waits_for_a_new_leaders_first_commit_even_after_leadership_is_proven() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader(); // no-op at index 1, NOT yet committed

        let (query, mut reply) = read_query("k");
        cluster.handle_client_query(query); // round 2

        // Both followers answer round 2, but neither has stored the no-op, so it
        // stays uncommitted: leadership is proven, the commit index is not trusted.
        cluster.handle_rpc(2, append_ack(1, 2, false, 0));
        cluster.handle_rpc(3, append_ack(1, 2, false, 0));
        cluster.apply_committed_entries();
        cluster.complete_ready_reads();
        assert!(
            reply.try_recv().is_err(),
            "must not answer before the leader's own no-op has committed"
        );

        // Node 2 now stores the no-op: it commits, is applied, and the read completes.
        cluster.handle_rpc(2, append_ack(1, 2, true, 1));
        cluster.apply_committed_entries();
        cluster.complete_ready_reads();
        assert!(matches!(reply.try_recv(), Ok(QueryResult::Value(None))));
    }

    #[test]
    fn each_read_waits_for_its_own_round() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);

        let (first, mut first_reply) = read_query("a");
        cluster.handle_client_query(first); // round 2
        let (second, mut second_reply) = read_query("b");
        cluster.handle_client_query(second); // round 3

        // Round 2 confirmed: that satisfies the first read but not the second,
        // which only began after round 2 had already been sent.
        cluster.handle_rpc(2, append_ack(1, 2, true, 1));
        cluster.complete_ready_reads();
        assert!(matches!(first_reply.try_recv(), Ok(QueryResult::Value(None))));
        assert!(second_reply.try_recv().is_err());

        cluster.handle_rpc(2, append_ack(1, 3, true, 1));
        cluster.complete_ready_reads();
        assert!(matches!(second_reply.try_recv(), Ok(QueryResult::Value(None))));
    }

    #[test]
    fn a_read_reflects_writes_that_were_committed_before_it_arrived() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]); // single node: commits immediately
        cluster.begin_election();
        cluster.handle_client_command(put_command("greeting", "hello"));
        cluster.apply_committed_entries();

        let (query, mut reply) = read_query("greeting");
        cluster.handle_client_query(query);
        cluster.complete_ready_reads();

        assert!(matches!(reply.try_recv(), Ok(QueryResult::Value(Some(v))) if v == b"hello".to_vec()));
    }

    #[test]
    fn losing_leadership_redirects_waiting_reads_instead_of_answering_them() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let (query, mut reply) = read_query("k");
        cluster.handle_client_query(query);

        cluster.handle_rpc(2, higher_term_response(7));

        assert_eq!(cluster.volatile.role, Role::Follower);
        assert!(matches!(reply.try_recv(), Ok(QueryResult::NotLeader { leader_hint: None })));
    }

    #[test]
    fn a_read_deposed_by_a_new_leader_is_redirected_to_that_leader() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let (query, mut reply) = read_query("k");
        cluster.handle_client_query(query);

        cluster.handle_rpc(2, append_request(5, 2, 0, 0, vec![]));

        assert!(matches!(reply.try_recv(), Ok(QueryResult::NotLeader { leader_hint: Some(2) })));
    }

    #[test]
    fn a_leader_that_steps_down_for_lack_of_a_majority_redirects_waiting_reads() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let (query, mut reply) = read_query("k");
        cluster.handle_client_query(query);

        age_all_acks(&mut cluster);
        cluster.check_quorum();

        assert_eq!(cluster.volatile.role, Role::Follower);
        assert!(matches!(reply.try_recv(), Ok(QueryResult::NotLeader { leader_hint: None })));
    }

    #[test]
    fn a_deposed_leader_does_not_name_itself_as_the_leader() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        assert_eq!(cluster.known_leader, Some(1));

        // Deposed by a vote request: nothing tells us who the new leader is.
        cluster.handle_rpc(3, vote_request(9, 3));

        assert_eq!(cluster.known_leader, None, "must not send clients back to ourselves");
    }

    #[test]
    fn local_read_mode_answers_immediately_without_any_confirmation() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) =
            make_cluster_with_mode(dir.path(), &[2, 3], ReadMode::Local);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        cluster.kv_store.apply(&Command::Put { key: "k".into(), value: b"v".to_vec() });

        let (query, mut reply) = read_query("k");
        cluster.handle_client_query(query);

        // No round was started and no follower has answered anything.
        assert!(matches!(reply.try_recv(), Ok(QueryResult::Value(Some(v))) if v == b"v".to_vec()));
        assert!(cluster.pending_reads.is_empty());
    }

    #[test]
    fn abandoned_reads_are_pruned() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let (query, reply) = read_query("k");
        cluster.handle_client_query(query);
        assert_eq!(cluster.pending_reads.len(), 1);

        drop(reply); // the HTTP request timed out
        cluster.prune_abandoned_requests();

        assert!(cluster.pending_reads.is_empty());
    }

    #[test]
    fn every_broadcast_carries_a_new_round_number() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        cluster.broadcast_append_entries();
        cluster.broadcast_append_entries();

        let seqs: Vec<u64> = drain(&mut outputs, 2)
            .into_iter()
            .filter_map(|m| match m {
                RpcMessage::AppendEntries(req) => Some(req.seq),
                _ => None,
            })
            .collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    #[test]
    fn a_follower_echoes_the_round_number_it_was_sent() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        let request = RpcMessage::AppendEntries(AppendEntriesRequest {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            seq: 77,
        });

        cluster.handle_rpc(2, request);

        let sent = drain(&mut outputs, 2);
        assert!(
            matches!(&sent[..], [RpcMessage::AppendEntriesResponse(r)] if r.seq == 77),
            "expected the reply to carry round 77, got {sent:?}"
        );
    }

    // ---- group commit -------------------------------------------------------

    #[test]
    fn a_batch_of_writes_shares_one_disk_sync_and_one_replication_round() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let syncs_before = cluster.persistence.wal_sync_count();
        let round_before = cluster.heartbeat_seq;

        let batch: Vec<ClientCommand> =
            (0..10).map(|i| put_command(&format!("k{i}"), "v")).collect();
        cluster.handle_client_commands(batch);

        assert_eq!(cluster.log.last_index(), 11, "no-op plus ten writes");
        assert_eq!(cluster.pending_commands.len(), 10);
        assert_eq!(
            cluster.persistence.wal_sync_count() - syncs_before,
            1,
            "ten writes must share a single fsync"
        );
        assert_eq!(cluster.heartbeat_seq - round_before, 1, "and a single replication round");
    }

    #[test]
    fn a_batch_is_durable_before_it_is_replicated_or_acknowledged() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        drain(&mut outputs, 2);

        cluster.handle_client_commands((0..3).map(|i| put_command(&format!("k{i}"), "v")).collect());

        // Everything the leader appended is already on disk by the time the
        // replication messages exist.
        let on_disk = Wal::replay(dir.wal_path()).unwrap();
        assert_eq!(on_disk.len(), 4, "no-op plus three writes");
        let sent = drain(&mut outputs, 2);
        assert!(
            sent.iter().any(|m| matches!(m, RpcMessage::AppendEntries(r) if r.entries.len() == 4)),
            "the one replication message carries the whole batch, got {sent:?}"
        );
    }

    #[test]
    fn a_follower_stores_a_whole_batch_of_entries_with_one_sync() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        let syncs_before = cluster.persistence.wal_sync_count();

        let entries: Vec<LogEntry> = (1..=8).map(|i| entry(1, i, "x")).collect();
        cluster.handle_rpc(2, append_request(1, 2, 0, 0, entries));

        assert_eq!(cluster.log.last_index(), 8);
        assert_eq!(cluster.persistence.wal_sync_count() - syncs_before, 1);
        assert_eq!(Wal::replay(dir.wal_path()).unwrap().len(), 8);
    }

    #[test]
    fn a_batch_sent_to_a_follower_replies_with_the_replicated_index() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        let entries: Vec<LogEntry> = (1..=5).map(|i| entry(1, i, "x")).collect();

        cluster.handle_rpc(2, append_request(1, 2, 0, 0, entries));

        let sent = drain(&mut outputs, 2);
        assert!(
            matches!(&sent[..], [RpcMessage::AppendEntriesResponse(r)] if r.success && r.match_index == 5),
            "got {sent:?}"
        );
    }

    #[test]
    fn writes_to_a_follower_are_redirected_without_touching_the_disk() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        let syncs_before = cluster.persistence.wal_sync_count();
        let (first, mut first_rx) = put_command_with_receiver("a", "1");
        let (second, mut second_rx) = put_command_with_receiver("b", "2");

        cluster.handle_client_commands(vec![first, second]);

        assert!(matches!(first_rx.try_recv(), Ok(ClientCommandResult::NotLeader { .. })));
        assert!(matches!(second_rx.try_recv(), Ok(ClientCommandResult::NotLeader { .. })));
        assert_eq!(cluster.persistence.wal_sync_count(), syncs_before);
    }

    #[test]
    fn a_batch_collects_what_is_already_queued_up_to_the_limit() {
        let (tx, mut rx) = mpsc::channel(32);
        for i in 1..=10u32 {
            tx.try_send(i).unwrap();
        }

        let batch = collect_batch(0u32, &mut rx, 4);

        assert_eq!(batch, vec![0, 1, 2, 3], "capped at the batch limit, in arrival order");
        assert_eq!(rx.try_recv().unwrap(), 4, "the rest stays queued for the next batch");
    }

    #[test]
    fn a_lone_request_is_never_held_back_waiting_for_company() {
        let (_tx, mut rx) = mpsc::channel::<u32>(8);
        let batch = collect_batch(7u32, &mut rx, 128);
        assert_eq!(batch, vec![7], "an idle node adds no latency");
    }

    #[test]
    fn queued_reads_in_one_batch_share_one_heartbeat_round() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let round_before = cluster.heartbeat_seq;

        let (queries, replies): (Vec<_>, Vec<_>) =
            (0..5).map(|i| read_query(&format!("k{i}"))).unzip();
        cluster.handle_client_queries(queries);

        assert_eq!(cluster.heartbeat_seq - round_before, 1, "one round for five reads");
        assert_eq!(cluster.pending_reads.len(), 5);

        cluster.handle_rpc(2, append_ack(1, cluster.heartbeat_seq, true, 1));
        cluster.complete_ready_reads();
        for mut reply in replies {
            assert!(matches!(reply.try_recv(), Ok(QueryResult::Value(None))));
        }
    }

    // ---- backpressure -------------------------------------------------------

    fn small_limits(capacity: usize) -> Limits {
        Limits { channel_capacity: capacity, max_batch: 128 }
    }

    #[test]
    fn writes_beyond_the_in_flight_limit_are_turned_away_immediately() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_with_limits(dir.path(), &[2, 3], small_limits(3));
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        let mut replies = Vec::new();
        let mut batch = Vec::new();
        for i in 0..5 {
            let (cmd, rx) = put_command_with_receiver(&format!("k{i}"), "v");
            batch.push(cmd);
            replies.push(rx);
        }
        let log_before = cluster.log.last_index();
        cluster.handle_client_commands(batch);

        assert_eq!(cluster.pending_commands.len(), 3, "only 3 are held");
        assert_eq!(cluster.log.last_index() - log_before, 3, "and only 3 reached the log");
        for (i, mut reply) in replies.into_iter().enumerate() {
            if i < 3 {
                assert!(reply.try_recv().is_err(), "write {i} is accepted and waiting");
            } else {
                assert!(
                    matches!(reply.try_recv(), Ok(ClientCommandResult::Overloaded)),
                    "write {i} must be refused at once, not queued"
                );
            }
        }
    }

    #[test]
    fn a_refused_write_leaves_nothing_behind_in_the_log() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_with_limits(dir.path(), &[2, 3], small_limits(1));
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        cluster.handle_client_command(put_command("accepted", "1"));
        let log_before = cluster.log.last_index();
        let syncs_before = cluster.persistence.wal_sync_count();

        let (cmd, mut reply) = put_command_with_receiver("refused", "2");
        cluster.handle_client_command(cmd);

        assert!(matches!(reply.try_recv(), Ok(ClientCommandResult::Overloaded)));
        assert_eq!(cluster.log.last_index(), log_before, "nothing was logged");
        assert_eq!(cluster.persistence.wal_sync_count(), syncs_before, "and nothing was synced");
    }

    #[test]
    fn capacity_frees_up_as_writes_complete() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_with_limits(dir.path(), &[], small_limits(2));
        cluster.begin_election(); // single node: writes commit immediately

        for round in 0..6 {
            let (cmd, mut reply) = put_command_with_receiver(&format!("k{round}"), "v");
            cluster.handle_client_command(cmd);
            cluster.apply_committed_entries();
            assert!(
                matches!(reply.try_recv(), Ok(ClientCommandResult::Applied(_))),
                "round {round}: finished writes must not count against the limit"
            );
        }
        assert!(cluster.pending_commands.is_empty());
    }

    #[test]
    fn reads_beyond_the_in_flight_limit_are_turned_away_immediately() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_with_limits(dir.path(), &[2, 3], small_limits(2));
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        let (queries, replies): (Vec<_>, Vec<_>) = (0..4).map(|i| read_query(&format!("k{i}"))).unzip();
        cluster.handle_client_queries(queries);

        assert_eq!(cluster.pending_reads.len(), 2);
        let refused = replies
            .into_iter()
            .filter_map(|mut r| r.try_recv().ok())
            .filter(|r| matches!(r, QueryResult::Overloaded))
            .count();
        assert_eq!(refused, 2);
    }

    #[test]
    fn a_full_peer_queue_drops_messages_instead_of_blocking_the_node() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_with_limits(dir.path(), &[2, 3], small_limits(2));
        cluster.persistent.current_term = 1;

        // Nobody reads node 2's queue, so it holds 2 messages and the rest do not fit.
        for _ in 0..5 {
            cluster.send_to(2, pre_vote_request(1, 1));
        }

        assert_eq!(cluster.dropped_messages, 3, "5 sent, 2 queued, 3 dropped");
        assert_eq!(drain(&mut outputs, 2).len(), 2);
        // Node 3's queue is unaffected by node 2 being slow.
        cluster.send_to(3, pre_vote_request(1, 1));
        assert_eq!(drain(&mut outputs, 3).len(), 1);
    }

    #[test]
    fn a_leader_whose_followers_cannot_keep_up_still_makes_progress_locally() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_with_limits(dir.path(), &[2, 3], small_limits(1));
        cluster.persistent.current_term = 1;
        cluster.become_leader();

        // Far more broadcasts than either peer queue can hold.
        for _ in 0..50 {
            cluster.broadcast_append_entries();
        }

        assert!(cluster.dropped_messages > 0, "excess messages were dropped");
        assert_eq!(cluster.volatile.role, Role::Leader, "the node itself was never stalled");
    }

    #[test]
    fn dropped_messages_and_disk_syncs_are_reported_in_the_snapshot() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_with_limits(dir.path(), &[2, 3], small_limits(1));
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        for _ in 0..5 {
            cluster.broadcast_append_entries();
        }
        let mut snapshot_rx = cluster.snapshot_tx.subscribe();
        cluster.publish_snapshot();

        let snapshot = snapshot_rx.borrow_and_update().clone();
        assert!(snapshot.dropped_messages > 0);
        assert!(snapshot.wal_syncs >= 1, "the leader's no-op was synced");
    }

    // ---- membership changes ---------------------------------------------------

    use std::sync::{Arc, Mutex};

    /// The connections a node opened through its `PeerConnector`, keyed by peer.
    type Connections = Arc<Mutex<HashMap<NodeId, mpsc::Receiver<RpcMessage>>>>;

    fn recording_connector(connections: &Connections) -> PeerConnector {
        let connections = connections.clone();
        Box::new(move |id, _addr| {
            let (tx, rx) = mpsc::channel(1024);
            connections.lock().unwrap().insert(id, rx);
            tx
        })
    }

    fn make_cluster_recording(dir: &Path, peer_ids: &[NodeId]) -> (Cluster, Outputs, Connections) {
        let connections = Connections::default();
        let (cluster, outputs) = make_cluster_core(
            dir,
            peer_ids,
            ReadMode::Linearizable,
            Limits::default(),
            recording_connector(&connections),
        );
        (cluster, outputs, connections)
    }

    /// What this node has sent to `peer` over a connection it opened itself.
    fn drain_connection(connections: &Connections, peer: NodeId) -> Vec<RpcMessage> {
        let mut guard = connections.lock().unwrap();
        let rx = guard.get_mut(&peer).unwrap_or_else(|| panic!("no connection to node {peer}"));
        let mut messages = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            messages.push(msg);
        }
        messages
    }

    fn admin(change: MembershipChange) -> (ClientAdmin, oneshot::Receiver<AdminResult>) {
        let (respond_to, receiver) = oneshot::channel();
        (ClientAdmin { change, respond_to }, receiver)
    }

    fn add(id: NodeId) -> MembershipChange {
        MembershipChange::Add { id, addr: format!("node{id}:7000") }
    }

    fn remove(id: NodeId) -> MembershipChange {
        MembershipChange::Remove { id }
    }

    /// What the event loop does after every event.
    fn settle(cluster: &mut Cluster) {
        cluster.apply_committed_entries();
        cluster.maybe_snapshot();
        cluster.drive_admin();
    }

    fn members(cluster: &Cluster) -> Vec<NodeId> {
        cluster.membership.ids().collect()
    }

    fn ack(cluster: &mut Cluster, from: NodeId, match_index: u64) {
        let term = cluster.persistent.current_term;
        cluster.handle_rpc(from, append_ack(term, cluster.heartbeat_seq, true, match_index));
        settle(cluster);
    }

    fn request_change(
        cluster: &mut Cluster,
        change: MembershipChange,
    ) -> oneshot::Receiver<AdminResult> {
        let (request, receiver) = admin(change);
        cluster.handle_client_admin(request);
        settle(cluster);
        receiver
    }

    fn rejected(mut receiver: oneshot::Receiver<AdminResult>) -> AdminRejection {
        match receiver.try_recv() {
            Ok(AdminResult::Rejected(reason)) => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    fn config_entry_commands(cluster: &Cluster) -> Vec<Command> {
        (1..=cluster.log.last_index())
            .filter_map(|i| Command::decode(&cluster.log.get(i).unwrap().command).ok())
            .filter(|c| c.config_change().is_some())
            .collect()
    }

    #[test]
    fn a_follower_refuses_a_membership_change_and_names_the_leader() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.known_leader = Some(2);
        let mut receiver = request_change(&mut cluster, add(4));
        match receiver.try_recv() {
            Ok(AdminResult::NotLeader { leader_hint }) => assert_eq!(leader_hint, Some(2)),
            other => panic!("expected NotLeader, got {other:?}"),
        }
        assert!(cluster.learners.is_empty());
    }

    #[test]
    fn a_new_leader_refuses_changes_until_its_own_noop_has_committed() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader(); // no-op at index 1, not yet committed
        assert_eq!(rejected(request_change(&mut cluster, add(4))), AdminRejection::NotReady);
        assert_eq!(rejected(request_change(&mut cluster, remove(3))), AdminRejection::NotReady);
        assert_eq!(cluster.log.last_index(), 1, "a refused change must log nothing");

        ack(&mut cluster, 2, 1); // the no-op commits
        let receiver = request_change(&mut cluster, add(4));
        drop(receiver);
        assert!(cluster.learners.contains_key(&4), "now it is accepted");
    }

    #[test]
    fn a_new_member_starts_as_a_learner_that_is_sent_the_log_but_not_logged() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);

        let mut receiver = request_change(&mut cluster, add(4));

        assert!(cluster.learners.contains_key(&4));
        assert_eq!(members(&cluster), vec![1, 2, 3], "not a voter yet");
        assert_eq!(config_entry_commands(&cluster).len(), 0, "nothing logged yet");
        assert!(receiver.try_recv().is_err(), "the caller is still waiting");
        let sent = drain_connection(&connections, 4);
        assert!(
            sent.iter().any(|m| matches!(m, RpcMessage::AppendEntries(_))),
            "the learner is sent the log: {sent:?}"
        );
    }

    #[test]
    fn a_learner_that_has_not_caught_up_is_not_admitted() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let _receiver = request_change(&mut cluster, add(4));

        // It answers, but only to say it has nothing yet (match 0 < commit 1).
        let term = cluster.persistent.current_term;
        cluster.handle_rpc(4, append_ack(term, cluster.heartbeat_seq, false, 0));
        settle(&mut cluster);

        assert_eq!(config_entry_commands(&cluster).len(), 0);
        assert_eq!(members(&cluster), vec![1, 2, 3]);
        assert!(cluster.learners.contains_key(&4));
    }

    #[test]
    fn a_caught_up_learner_is_admitted_and_the_new_configuration_applies_at_once() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let mut receiver = request_change(&mut cluster, add(4));

        ack(&mut cluster, 4, 1); // caught up to the commit index

        assert_eq!(
            config_entry_commands(&cluster),
            vec![Command::AddNode { id: 4, addr: "node4:7000".into() }]
        );
        assert_eq!(members(&cluster), vec![1, 2, 3, 4], "in effect on append, before commit");
        assert!(cluster.learners.is_empty(), "no longer a learner");
        assert_eq!(cluster.volatile.commit_index, 1, "the entry itself is not committed yet");
        assert!(receiver.try_recv().is_err(), "still waiting for the commit");
    }

    #[test]
    fn once_a_member_is_added_a_majority_of_the_larger_cluster_is_needed() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let mut receiver = request_change(&mut cluster, add(4));
        ack(&mut cluster, 4, 1); // AddNode appended at index 2, members {1,2,3,4}, quorum 3

        ack(&mut cluster, 2, 2); // leader + node 2 = 2 of 4: NOT a majority any more
        assert_eq!(cluster.volatile.commit_index, 1);
        assert!(receiver.try_recv().is_err());

        ack(&mut cluster, 3, 2); // leader + 2 + 3 = 3 of 4
        assert_eq!(cluster.volatile.commit_index, 2);
        match receiver.try_recv() {
            Ok(AdminResult::Applied { members }) => assert_eq!(members, vec![1, 2, 3, 4]),
            other => panic!("expected Applied, got {other:?}"),
        }
    }

    #[test]
    fn a_learner_acknowledging_a_write_does_not_commit_it() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let _add = request_change(&mut cluster, add(4));

        let (put, _put_rx) = put_command_with_receiver("k", "v"); // index 2
        cluster.handle_client_command(put);
        ack(&mut cluster, 4, 2); // the learner has it; no voting member besides the leader does

        assert_eq!(cluster.volatile.commit_index, 1, "a learner's copy is not a vote");
    }

    #[test]
    fn a_learner_whose_acknowledgements_arrive_does_not_keep_a_dead_cluster_alive() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let _add = request_change(&mut cluster, add(4));
        cluster.handle_rpc(4, append_ack(1, cluster.heartbeat_seq, true, 1));

        age_all_acks(&mut cluster);
        // Only the learner (not a member) answers now; members 2 and 3 are silent.
        cluster.leader_state.record_ack(4, std::time::Instant::now());
        cluster.check_quorum();

        assert_eq!(cluster.volatile.role, Role::Follower, "check-quorum ignores learners");
    }

    #[test]
    fn only_one_membership_change_may_be_in_flight() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);

        let _first = request_change(&mut cluster, add(4));
        // While the new node is catching up...
        assert_eq!(rejected(request_change(&mut cluster, add(5))), AdminRejection::ChangeInProgress);
        assert_eq!(rejected(request_change(&mut cluster, remove(3))), AdminRejection::ChangeInProgress);

        // ...and while its entry is committing.
        ack(&mut cluster, 4, 1);
        assert_eq!(rejected(request_change(&mut cluster, add(5))), AdminRejection::ChangeInProgress);

        // Once it has committed, the next change is allowed.
        ack(&mut cluster, 2, 2);
        ack(&mut cluster, 3, 2);
        let receiver = request_change(&mut cluster, remove(3));
        drop(receiver);
        assert_eq!(config_entry_commands(&cluster).len(), 2);
    }

    #[test]
    fn adding_a_member_or_removing_a_stranger_is_refused_and_logs_nothing() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let before = cluster.log.last_index();
        assert_eq!(rejected(request_change(&mut cluster, add(2))), AdminRejection::AlreadyMember);
        assert_eq!(rejected(request_change(&mut cluster, remove(9))), AdminRejection::NotMember);
        assert_eq!(
            rejected(request_change(
                &mut cluster,
                MembershipChange::Add { id: 4, addr: "  ".into() }
            )),
            AdminRejection::BadAddress
        );
        assert_eq!(cluster.log.last_index(), before);
        assert!(cluster.learners.is_empty());
    }

    #[test]
    fn the_last_member_cannot_be_removed() {
        let dir = TestDir::new();
        let mut cluster = build_cluster(&dir, &[]);
        cluster.begin_election(); // a one-node cluster elects itself
        settle(&mut cluster);
        assert_eq!(cluster.volatile.role, Role::Leader);
        assert_eq!(rejected(request_change(&mut cluster, remove(1))), AdminRejection::LastMember);
    }

    #[test]
    fn a_removal_takes_effect_on_append_and_lowers_the_bar_for_commit() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let mut receiver = request_change(&mut cluster, remove(3));

        assert_eq!(members(&cluster), vec![1, 2], "in effect before it commits");
        assert_eq!(cluster.volatile.commit_index, 1);
        assert!(receiver.try_recv().is_err());

        // Node 3 never answers. Leader + node 2 is all of the new two-node cluster.
        ack(&mut cluster, 2, 2);
        assert_eq!(cluster.volatile.commit_index, 2);
        match receiver.try_recv() {
            Ok(AdminResult::Applied { members }) => assert_eq!(members, vec![1, 2]),
            other => panic!("expected Applied, got {other:?}"),
        }
    }

    #[test]
    fn a_removed_peer_is_still_sent_the_entry_until_it_commits_and_then_forgotten() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        drain(&mut outputs, 3);

        let _receiver = request_change(&mut cluster, remove(3));
        let sent = drain(&mut outputs, 3);
        assert!(
            sent.iter().any(|m| matches!(m, RpcMessage::AppendEntries(_))),
            "so the removed node can learn it was removed"
        );
        assert!(cluster.peer_outboxes.contains_key(&3));

        ack(&mut cluster, 2, 2); // commits and applies the removal
        assert!(!cluster.peer_outboxes.contains_key(&3), "forgotten once the removal committed");
        assert!(!cluster.leader_state.match_index.contains_key(&3));
    }

    #[test]
    fn a_leader_that_removes_itself_steps_down_only_after_the_removal_commits() {
        let dir = TestDir::new();
        let mut cluster = settled_leader(&dir);
        let mut receiver = request_change(&mut cluster, remove(1));

        assert_eq!(members(&cluster), vec![2, 3]);
        assert_eq!(cluster.volatile.role, Role::Leader, "it keeps leading until the change commits");

        // The leader's own copy does not count: it is no longer a voter. One of two
        // remaining members is not a majority of them.
        ack(&mut cluster, 2, 2);
        assert_eq!(cluster.volatile.commit_index, 1);
        assert_eq!(cluster.volatile.role, Role::Leader);

        ack(&mut cluster, 3, 2);
        assert_eq!(cluster.volatile.commit_index, 2);
        match receiver.try_recv() {
            Ok(AdminResult::Applied { members }) => assert_eq!(members, vec![2, 3]),
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(cluster.volatile.role, Role::Follower, "handed over once committed");
    }

    #[test]
    fn a_node_that_is_not_a_member_never_campaigns() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        // The leader tells this node that it has been removed.
        cluster.handle_rpc(
            2,
            append_request(
                1,
                2,
                0,
                0,
                vec![
                    LogEntry {
                        term: 1,
                        index: 1,
                        command: Command::RemoveNode { id: 1 }.encode().unwrap(),
                    },
                ],
            ),
        );
        assert_eq!(members(&cluster), vec![2, 3]);
        drain(&mut outputs, 2);
        drain(&mut outputs, 3);
        let term = cluster.persistent.current_term;

        forget_leader_contact(&mut cluster);
        cluster.begin_pre_vote();

        assert!(drain(&mut outputs, 2).is_empty(), "no pre-vote sent");
        assert!(drain(&mut outputs, 3).is_empty(), "no pre-vote sent");
        assert!(cluster.pre_vote.is_none());
        assert_eq!(cluster.persistent.current_term, term, "and no term change");
    }

    #[test]
    fn a_follower_puts_a_configuration_entry_into_effect_on_append_and_undoes_it_on_truncation() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, connections) = make_cluster_recording(dir.path(), &[2, 3]);
        let add4 = LogEntry {
            term: 1,
            index: 2,
            command: Command::AddNode { id: 4, addr: "node4:7000".into() }.encode().unwrap(),
        };
        let noop = LogEntry { term: 1, index: 1, command: Command::NoOp.encode().unwrap() };
        cluster.handle_rpc(2, append_request(1, 2, 0, 0, vec![noop, add4]));

        assert_eq!(members(&cluster), vec![1, 2, 3, 4], "in effect though not committed");
        assert!(cluster.peer_outboxes.contains_key(&4), "and the new member is dialled");
        assert!(connections.lock().unwrap().contains_key(&4));

        // A new leader's log disagrees from index 2 on: the change never happened.
        let replacement = LogEntry { term: 2, index: 2, command: Command::NoOp.encode().unwrap() };
        cluster.handle_rpc(3, append_request(2, 3, 1, 1, vec![replacement]));
        assert_eq!(members(&cluster), vec![1, 2, 3], "a truncated change stops counting");
    }

    #[test]
    fn the_membership_is_rebuilt_from_the_log_after_a_restart() {
        let dir = TestDir::new();
        {
            let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
            let entries = vec![
                LogEntry { term: 1, index: 1, command: Command::NoOp.encode().unwrap() },
                LogEntry {
                    term: 1,
                    index: 2,
                    command: Command::AddNode { id: 4, addr: "node4:7000".into() }
                        .encode()
                        .unwrap(),
                },
                LogEntry {
                    term: 1,
                    index: 3,
                    command: Command::RemoveNode { id: 3 }.encode().unwrap(),
                },
            ];
            cluster.handle_rpc(2, append_request(1, 2, 0, 0, entries));
            assert_eq!(members(&cluster), vec![1, 2, 4]);
        }

        // Restarted with the ORIGINAL peer list: the log, not the environment, wins.
        let (restarted, _outputs, connections) = make_cluster_recording(dir.path(), &[2, 3]);
        assert_eq!(members(&restarted), vec![1, 2, 4]);
        assert!(connections.lock().unwrap().contains_key(&4), "dials the member it learned of");
        assert!(!restarted.peer_outboxes.contains_key(&3), "and drops the one removed");
    }

    #[test]
    fn votes_and_pre_votes_from_a_non_member_are_refused_without_side_effects() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        forget_leader_contact(&mut cluster);
        cluster.persistent.current_term = 3;

        // Node 9 is not in the cluster.
        cluster.handle_rpc(9, vote_request(7, 9));
        cluster.handle_rpc(9, pre_vote_request(8, 9));

        assert_eq!(cluster.persistent.current_term, 3, "a stranger cannot bump our term");
        assert_eq!(cluster.persistent.voted_for, None, "nor spend our vote");
        let _ = &mut outputs; // replies to node 9 go nowhere: it has no connection
    }

    #[test]
    fn a_removed_node_is_denied_a_pre_vote_it_could_otherwise_win() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        // Node 3 is removed; the entry is only appended, so we still have a
        // connection to it and can see exactly how we answer it.
        let removal = LogEntry {
            term: 1,
            index: 1,
            command: Command::RemoveNode { id: 3 }.encode().unwrap(),
        };
        cluster.handle_rpc(2, append_request(1, 2, 0, 0, vec![removal]));
        assert_eq!(members(&cluster), vec![1, 2]);
        drain(&mut outputs, 2);
        drain(&mut outputs, 3);
        forget_leader_contact(&mut cluster); // no active leader: a member would win this

        // Its log is as up to date as ours, so only its membership can deny it.
        cluster.handle_rpc(
            3,
            RpcMessage::PreVote(PreVoteRequest {
                term: 2,
                candidate_id: 3,
                last_log_index: 1,
                last_log_term: 1,
            }),
        );
        match drain(&mut outputs, 3).pop() {
            Some(RpcMessage::PreVoteResponse(resp)) => {
                assert!(!resp.vote_granted, "a removed node must not be granted a pre-vote")
            }
            other => panic!("expected a pre-vote answer, got {other:?}"),
        }

        cluster.handle_rpc(
            3,
            RpcMessage::RequestVote(RequestVoteRequest {
                term: 2,
                candidate_id: 3,
                last_log_index: 1,
                last_log_term: 1,
            }),
        );
        assert!(!vote_reply(&mut outputs, 3).vote_granted);
        assert_eq!(cluster.persistent.voted_for, None);
    }

    #[test]
    fn a_vote_request_from_a_member_is_still_honoured() {
        let dir = TestDir::new();
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.handle_rpc(2, vote_request(1, 2));
        assert!(vote_reply(&mut outputs, 2).vote_granted);
    }

    #[test]
    fn a_learner_that_never_catches_up_is_abandoned_and_forgotten() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.config.catch_up_timeout = Duration::ZERO;
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let before = cluster.log.last_index();

        let receiver = request_change(&mut cluster, add(4));

        assert_eq!(rejected(receiver), AdminRejection::CatchUpFailed);
        assert!(cluster.learners.is_empty());
        assert!(!cluster.peer_outboxes.contains_key(&4), "its connection is dropped");
        assert_eq!(cluster.log.last_index(), before, "nothing was logged");

        // The slot is free again.
        cluster.config.catch_up_timeout = Duration::from_secs(60);
        let receiver = request_change(&mut cluster, add(4));
        drop(receiver);
        assert!(cluster.learners.contains_key(&4));
    }

    #[test]
    fn a_caller_that_gives_up_cancels_an_add_that_is_still_catching_up() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let receiver = request_change(&mut cluster, add(4));
        assert!(cluster.learners.contains_key(&4));

        drop(receiver); // the HTTP request timed out
        settle(&mut cluster);

        assert!(cluster.learners.is_empty());
        assert!(cluster.pending_admin.is_none(), "the slot is free");
    }

    #[test]
    fn losing_leadership_fails_a_change_in_progress_and_drops_its_learner() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let mut receiver = request_change(&mut cluster, add(4));

        cluster.handle_rpc(2, higher_term_response(5));

        assert!(matches!(receiver.try_recv(), Ok(AdminResult::LeadershipLost)));
        assert!(cluster.learners.is_empty());
        assert!(!cluster.peer_outboxes.contains_key(&4));
        assert!(cluster.pending_admin.is_none());
    }

    #[test]
    fn growing_a_single_node_cluster_needs_the_new_node_to_commit_anything() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[]);
        cluster.begin_election();
        settle(&mut cluster);
        assert_eq!(cluster.volatile.role, Role::Leader);
        assert_eq!(cluster.volatile.commit_index, 1, "a lone node commits its own no-op");

        let mut receiver = request_change(&mut cluster, add(2));
        ack(&mut cluster, 2, 1); // caught up: AddNode appended, members {1,2}, quorum 2
        assert_eq!(members(&cluster), vec![1, 2]);
        assert_eq!(cluster.volatile.commit_index, 1, "needs node 2's copy now");
        assert!(receiver.try_recv().is_err());

        ack(&mut cluster, 2, 2);
        assert_eq!(cluster.volatile.commit_index, 2);
        assert!(matches!(receiver.try_recv(), Ok(AdminResult::Applied { .. })));
    }

    #[test]
    fn members_and_learners_are_published_in_the_snapshot() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        let snapshots = cluster.snapshot_tx.subscribe();
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        let _receiver = request_change(&mut cluster, add(4));
        cluster.publish_snapshot();

        let snapshot = snapshots.borrow().clone();
        assert_eq!(snapshot.members, vec![1, 2, 3]);
        assert_eq!(snapshot.learners, vec![4]);
    }

    // ---- starting from a snapshot ---------------------------------------------

    /// Writes a storage directory as a node leaves it after compaction: entries
    /// 1..=`last_index` (each `Put k{i} = v{i}` in term 1), a snapshot of the state
    /// through `base_index` carrying `membership`, and the log trimmed to match.
    fn write_snapshot_fixture(dir: &Path, base_index: u64, last_index: u64, membership: Membership) {
        let mut persistence = Persistence::open(dir).unwrap().persistence;
        let mut state = KvStore::new();
        for i in 1..=last_index {
            let command = Command::Put { key: format!("k{i}"), value: format!("v{i}").into_bytes() };
            persistence
                .append(&LogEntry { term: 1, index: i, command: command.encode().unwrap() })
                .unwrap();
            if i <= base_index {
                state.apply(&command);
            }
        }
        persistence
            .save_snapshot_and_compact(
                storage::SnapshotMetadata {
                    last_included_index: base_index,
                    last_included_term: 1,
                    membership,
                },
                &state,
            )
            .unwrap();
    }

    fn value_of(cluster: &Cluster, key: &str) -> Option<Vec<u8>> {
        cluster.kv_store.get(key).cloned()
    }

    #[test]
    fn a_node_starts_from_its_snapshot_and_replays_only_what_follows_it() {
        let dir = TestDir::new();
        write_snapshot_fixture(dir.path(), 5, 7, Membership::of_ids([1, 2, 3]));
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);

        assert_eq!((cluster.log.base_index(), cluster.log.last_index()), (5, 7));
        assert_eq!(cluster.log.len(), 2, "only the two entries after the snapshot are held");
        assert_eq!((cluster.volatile.commit_index, cluster.volatile.last_applied), (5, 5));
        assert_eq!(value_of(&cluster, "k5"), Some(b"v5".to_vec()), "state came from the snapshot");
        assert_eq!(value_of(&cluster, "k1"), Some(b"v1".to_vec()));
        assert_eq!(value_of(&cluster, "k6"), None, "entries after it are not committed yet");

        // Once a leader says they are committed they are applied on top.
        cluster.handle_rpc(2, append_request_committing(1, 2, 7, 1, vec![], 7));
        settle(&mut cluster);
        assert_eq!(value_of(&cluster, "k6"), Some(b"v6".to_vec()));
        assert_eq!(value_of(&cluster, "k7"), Some(b"v7".to_vec()));
        assert_eq!(cluster.volatile.last_applied, 7);
    }

    #[test]
    fn the_snapshots_published_position_starts_at_its_index() {
        let dir = TestDir::new();
        write_snapshot_fixture(dir.path(), 5, 5, Membership::of_ids([1, 2, 3]));
        let (cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        let snapshots = cluster.snapshot_tx.subscribe();
        cluster.publish_snapshot();
        let snapshot = snapshots.borrow().clone();
        assert_eq!((snapshot.commit_index, snapshot.last_applied, snapshot.log_len), (5, 5, 5));
    }

    #[test]
    fn the_snapshots_membership_replaces_the_one_from_the_environment() {
        let dir = TestDir::new();
        write_snapshot_fixture(
            dir.path(),
            4,
            4,
            Membership::new([
                (1, String::new()),
                (2, String::new()),
                (4, "node4:7000".to_string()),
            ]),
        );
        // The environment still says {1,2,3}: node 3 was removed and 4 added since.
        let (cluster, _outputs, connections) = make_cluster_recording(dir.path(), &[2, 3]);
        assert_eq!(members(&cluster), vec![1, 2, 4]);
        assert!(connections.lock().unwrap().contains_key(&4), "dials the member it learned of");
        assert!(!cluster.peer_outboxes.contains_key(&3), "and drops the one that was removed");
    }

    #[test]
    fn configuration_entries_after_the_snapshot_build_on_the_snapshots_membership() {
        let dir = TestDir::new();
        write_snapshot_fixture(dir.path(), 3, 3, Membership::of_ids([1, 2, 3]));
        {
            let mut persistence = Persistence::open(dir.path()).unwrap().persistence;
            persistence
                .append(&LogEntry {
                    term: 1,
                    index: 4,
                    command: Command::AddNode { id: 4, addr: "node4:7000".into() }
                        .encode()
                        .unwrap(),
                })
                .unwrap();
        }
        let (cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        assert_eq!(members(&cluster), vec![1, 2, 3, 4]);
        assert_eq!(cluster.history.len(), 1, "only the entry after the snapshot is tracked");
    }

    #[test]
    fn a_node_with_no_snapshot_still_takes_its_membership_from_the_environment() {
        let dir = TestDir::new();
        let (cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        assert_eq!(members(&cluster), vec![1, 2, 3]);
        assert_eq!(cluster.log.base_index(), 0);
    }

    #[test]
    fn a_follower_with_a_snapshot_accepts_entries_that_overlap_it() {
        let dir = TestDir::new();
        write_snapshot_fixture(dir.path(), 5, 7, Membership::of_ids([1, 2, 3]));
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);

        // The leader believes this node is only at index 2 and resends 3..=9.
        let entries: Vec<LogEntry> = (3..=9)
            .map(|i| {
                let command = Command::Put { key: format!("k{i}"), value: format!("v{i}").into_bytes() };
                LogEntry { term: 1, index: i, command: command.encode().unwrap() }
            })
            .collect();
        cluster.handle_rpc(2, append_request_committing(1, 2, 2, 1, entries, 9));
        settle(&mut cluster);

        match drain(&mut outputs, 2).pop() {
            Some(RpcMessage::AppendEntriesResponse(resp)) => {
                assert!(resp.success);
                assert_eq!(resp.match_index, 9);
            }
            other => panic!("expected an append response, got {other:?}"),
        }
        assert_eq!((cluster.log.base_index(), cluster.log.last_index()), (5, 9));
        assert_eq!(value_of(&cluster, "k9"), Some(b"v9".to_vec()));
        drop(cluster);
        // What reached the disk continues straight after the snapshot, with no
        // duplicates of 6 and 7 and nothing from before the snapshot.
        let recovered = Persistence::open(dir.path()).unwrap();
        assert_eq!(
            recovered.entries.iter().map(|e| e.index).collect::<Vec<_>>(),
            vec![6, 7, 8, 9]
        );
    }

    #[test]
    fn a_leader_does_not_try_to_replicate_entries_it_has_compacted_away() {
        let dir = TestDir::new();
        write_snapshot_fixture(dir.path(), 5, 7, Membership::of_ids([1, 2, 3]));
        let (mut cluster, mut outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 2;
        cluster.become_leader(); // no-op at index 8; both peers start at next_index 9
        drain(&mut outputs, 2);
        drain(&mut outputs, 3);

        // Node 3 turns out to be far behind: it needs entry 4, which is gone.
        cluster.leader_state.next_index.insert(3, 4);
        cluster.broadcast_append_entries();

        assert!(
            !drain(&mut outputs, 2).is_empty(),
            "the caught-up peer is still sent its entries"
        );
        assert!(
            drain(&mut outputs, 3).is_empty(),
            "nothing is sent that would have a gap before it"
        );

        // A peer that is exactly at the snapshot point can still be served: it
        // needs entry 6 onward, which is held.
        cluster.leader_state.next_index.insert(3, 6);
        cluster.broadcast_append_entries();
        assert!(!drain(&mut outputs, 3).is_empty());
    }

    #[test]
    fn a_leader_with_a_snapshot_commits_and_applies_new_writes() {
        let dir = TestDir::new();
        write_snapshot_fixture(dir.path(), 5, 5, Membership::of_ids([1, 2, 3]));
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.persistent.current_term = 2;
        cluster.become_leader(); // no-op at index 6
        ack(&mut cluster, 2, 6);
        assert_eq!(cluster.volatile.commit_index, 6);

        let (put, mut rx) = put_command_with_receiver("new", "value");
        cluster.handle_client_command(put); // index 7
        ack(&mut cluster, 2, 7);
        assert!(matches!(rx.try_recv(), Ok(ClientCommandResult::Applied(_))));
        assert_eq!(value_of(&cluster, "new"), Some(b"value".to_vec()));
        assert_eq!(value_of(&cluster, "k3"), Some(b"v3".to_vec()), "snapshot state is intact");
    }

    // ---- taking snapshots and trimming the log ---------------------------------

    fn put_log_entry(index: u64, term: Term) -> LogEntry {
        let command = Command::Put { key: format!("k{index}"), value: format!("v{index}").into_bytes() };
        LogEntry { term, index, command: command.encode().unwrap() }
    }

    /// A follower (leader is node 2, term 1) that has been told entries `from..=to`
    /// are all committed, and has applied them.
    fn follower_applies(cluster: &mut Cluster, from: u64, to: u64) {
        let entries: Vec<LogEntry> = (from..=to).map(|i| put_log_entry(i, 1)).collect();
        let prev_term = if from > 1 { 1 } else { 0 };
        cluster.handle_rpc(2, append_request_committing(1, 2, from - 1, prev_term, entries, to));
        settle(cluster);
    }

    fn wal_indices(dir: &Path) -> Vec<u64> {
        storage::Wal::replay(dir.join(storage::WAL_FILE)).unwrap().iter().map(|e| e.index).collect()
    }

    fn snapshots_on(cluster: &Cluster) -> ClusterSnapshot {
        let rx = cluster.snapshot_tx.subscribe();
        cluster.publish_snapshot();
        let snapshot = rx.borrow().clone();
        snapshot
    }

    #[test]
    fn snapshots_are_off_by_default_and_the_log_is_never_trimmed() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        follower_applies(&mut cluster, 1, 12);
        assert_eq!(cluster.persistence.snapshot_base(), 0);
        assert_eq!(cluster.log.base_index(), 0);
        assert_eq!(wal_indices(dir.path()).len(), 12);
        assert_eq!(snapshots_on(&cluster).snapshots_taken, 0);
    }

    #[test]
    fn nothing_happens_below_the_threshold() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.config.snapshot_threshold = 10;
        follower_applies(&mut cluster, 1, 9);
        assert_eq!(cluster.persistence.snapshot_base(), 0);
        follower_applies(&mut cluster, 10, 10);
        assert_eq!(cluster.persistence.snapshot_base(), 10, "now it is due");
    }

    #[test]
    fn a_snapshot_is_taken_when_enough_has_been_applied_and_the_log_is_trimmed() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.config.snapshot_threshold = 3;
        follower_applies(&mut cluster, 1, 5);

        assert_eq!(cluster.persistence.snapshot_base(), 5);
        assert_eq!((cluster.log.base_index(), cluster.log.last_index()), (5, 5));
        assert!(wal_indices(dir.path()).is_empty(), "the file shrank to nothing");
        let published = snapshots_on(&cluster);
        assert_eq!((published.snapshot_index, published.log_base, published.snapshots_taken), (5, 5, 1));

        let (metadata, state) = storage::load_snapshot(dir.path().join(storage::SNAPSHOT_FILE))
            .unwrap()
            .expect("the snapshot is on disk");
        assert_eq!((metadata.last_included_index, metadata.last_included_term), (5, 1));
        assert_eq!(state.get("k5"), Some(&b"v5".to_vec()));
        assert_eq!(state.len(), 5);
        assert_eq!(metadata.membership.ids().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    #[test]
    fn entries_just_behind_the_snapshot_are_retained_for_followers_that_lag() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.config.snapshot_threshold = 3;
        cluster.config.snapshot_retain = 2;
        follower_applies(&mut cluster, 1, 5);

        assert_eq!(cluster.persistence.snapshot_base(), 5);
        assert_eq!(cluster.log.base_index(), 3, "5 minus the 2 retained");
        assert_eq!(wal_indices(dir.path()), vec![4, 5]);
    }

    #[test]
    fn a_node_restarts_from_the_snapshot_it_took_and_carries_on() {
        let dir = TestDir::new();
        {
            let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
            cluster.config.snapshot_threshold = 4;
            cluster.config.snapshot_retain = 1;
            follower_applies(&mut cluster, 1, 6);
            follower_applies(&mut cluster, 7, 8);
        }
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        assert_eq!(cluster.persistence.snapshot_base(), 6, "the snapshot taken at 6");
        assert_eq!((cluster.volatile.last_applied, cluster.log.last_index()), (6, 8));
        for key in ["k1", "k6"] {
            assert!(cluster.kv_store.get(key).is_some(), "{key} came from the snapshot");
        }
        assert!(cluster.kv_store.get("k7").is_none(), "7 and 8 are replayed once committed");

        cluster.handle_rpc(2, append_request_committing(1, 2, 8, 1, vec![], 8));
        settle(&mut cluster);
        assert_eq!(cluster.kv_store.get("k8"), Some(&b"v8".to_vec()));
        assert_eq!(cluster.kv_store.len(), 8, "every write, none lost, none doubled");
    }

    #[test]
    fn the_membership_at_the_snapshot_is_what_the_snapshot_records() {
        let dir = TestDir::new();
        {
            let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
            cluster.config.snapshot_threshold = 3;
            let add4 = LogEntry {
                term: 1,
                index: 2,
                command: Command::AddNode { id: 4, addr: "node4:7000".into() }.encode().unwrap(),
            };
            let entries = vec![put_log_entry(1, 1), add4, put_log_entry(3, 1), put_log_entry(4, 1)];
            cluster.handle_rpc(2, append_request_committing(1, 2, 0, 0, entries, 4));
            settle(&mut cluster);
            assert_eq!(cluster.persistence.snapshot_base(), 4);
            assert_eq!(members(&cluster), vec![1, 2, 3, 4], "unchanged by trimming the log");
        }
        // The entry that added node 4 is gone from the log; only the snapshot knows.
        assert!(wal_indices(dir.path()).is_empty());
        let (restarted, _outputs, connections) = make_cluster_recording(dir.path(), &[2, 3]);
        assert_eq!(members(&restarted), vec![1, 2, 3, 4]);
        assert!(connections.lock().unwrap().contains_key(&4));
    }

    #[test]
    fn a_leader_keeps_the_log_its_slowest_peer_still_needs() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.config.snapshot_threshold = 2;
        cluster.persistent.current_term = 1;
        cluster.become_leader(); // no-op at index 1
        ack(&mut cluster, 2, 1);
        for _ in 0..2 {
            let (put, _rx) = put_command_with_receiver("k", "v");
            cluster.handle_client_command(put);
        }
        ack(&mut cluster, 2, 3); // 1 and 2 are a majority: committed and applied through 3

        assert_eq!(cluster.persistence.snapshot_base(), 3, "a snapshot is taken regardless");
        assert_eq!(cluster.log.base_index(), 0, "but node 3 has confirmed nothing: keep it all");
        assert_eq!(wal_indices(dir.path()).len(), 3);

        ack(&mut cluster, 3, 2); // node 3 catches up part of the way
        assert_eq!(cluster.log.base_index(), 2, "trimmed exactly as far as the slowest peer");

        ack(&mut cluster, 3, 3);
        assert_eq!(cluster.log.base_index(), 3);
    }

    #[test]
    fn a_learner_that_is_still_catching_up_also_holds_the_log_back() {
        let dir = TestDir::new();
        let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        cluster.config.snapshot_threshold = 2;
        cluster.persistent.current_term = 1;
        cluster.become_leader();
        ack(&mut cluster, 2, 1);
        ack(&mut cluster, 3, 1);
        let _add = request_change(&mut cluster, add(4)); // learner 4 starts at match 0
        for _ in 0..2 {
            let (put, _rx) = put_command_with_receiver("k", "v");
            cluster.handle_client_command(put);
        }
        ack(&mut cluster, 2, 3);
        ack(&mut cluster, 3, 3);
        assert_eq!(cluster.persistence.snapshot_base(), 3);
        assert_eq!(cluster.log.base_index(), 0, "the learner could not be sent trimmed entries");
    }

    #[test]
    fn a_failed_snapshot_is_logged_and_retried_later_not_fatal() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[2, 3]);
        cluster.config.snapshot_threshold = 2;
        // Something is in the way of the snapshot file: the write cannot succeed.
        std::fs::create_dir(dir.path().join(storage::SNAPSHOT_FILE)).unwrap();

        follower_applies(&mut cluster, 1, 4);
        assert_eq!(cluster.volatile.last_applied, 4, "the node carries on");
        assert_eq!(cluster.snapshots_taken, 0);
        assert_eq!(cluster.persistence.snapshot_base(), 0);
        assert_eq!(cluster.log.base_index(), 0, "and the log is untouched");
        assert_eq!(cluster.kv_store.len(), 4);

        // Not retried on every event, only once another threshold's worth is applied.
        assert_eq!(cluster.snapshot_retry_after, 6, "backs off for another threshold's worth");
        follower_applies(&mut cluster, 5, 5);
        assert_eq!(cluster.snapshots_taken, 0);

        std::fs::remove_dir(dir.path().join(storage::SNAPSHOT_FILE)).unwrap();
        follower_applies(&mut cluster, 6, 6);
        assert_eq!(cluster.snapshots_taken, 1, "the obstacle is gone: it works");
        assert_eq!(cluster.persistence.snapshot_base(), 6);
    }

    #[test]
    fn a_node_cannot_be_added_once_the_log_has_been_compacted() {
        let dir = TestDir::new();
        let (mut cluster, _outputs) = make_cluster_in(dir.path(), &[]);
        cluster.config.snapshot_threshold = 2;
        cluster.begin_election();
        settle(&mut cluster);
        for _ in 0..3 {
            let (put, _rx) = put_command_with_receiver("k", "v");
            cluster.handle_client_command(put);
            settle(&mut cluster);
        }
        assert!(cluster.log.base_index() > 0, "a lone node trims as soon as it has a snapshot");

        assert_eq!(rejected(request_change(&mut cluster, add(2))), AdminRejection::LogCompacted);
        assert!(cluster.learners.is_empty());
    }

    #[test]
    fn a_snapshot_does_not_include_a_configuration_change_that_is_not_applied_yet() {
        let dir = TestDir::new();
        {
            let (mut cluster, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
            cluster.config.snapshot_threshold = 3;
            let add4 = LogEntry {
                term: 1,
                index: 5,
                command: Command::AddNode { id: 4, addr: "node4:7000".into() }.encode().unwrap(),
            };
            let mut entries: Vec<LogEntry> = (1..=4).map(|i| put_log_entry(i, 1)).collect();
            entries.push(add4);
            // Entry 5 (the change) is in the log, so it is already in effect here, but
            // only 1..=4 are committed and applied.
            cluster.handle_rpc(2, append_request_committing(1, 2, 0, 0, entries, 4));
            settle(&mut cluster);
            assert_eq!(cluster.persistence.snapshot_base(), 4);
            assert_eq!(members(&cluster), vec![1, 2, 3, 4]);
        }
        let (metadata, _) = storage::load_snapshot(dir.path().join(storage::SNAPSHOT_FILE))
            .unwrap()
            .unwrap();
        assert_eq!(
            metadata.membership.ids().collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the snapshot describes the cluster as of index 4, before the change"
        );
        // The change is still in the log tail, so a restart puts it back into effect.
        let (restarted, _outputs, _connections) = make_cluster_recording(dir.path(), &[2, 3]);
        assert_eq!(members(&restarted), vec![1, 2, 3, 4]);
    }
}

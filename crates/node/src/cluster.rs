use raft_core::{
    advance_commit_index, build_append_entries, handle_append_entries,
    handle_append_entries_response, handle_pre_vote, handle_request_vote, start_election,
    ElectionTimeoutConfig, LeaderState, Log, NodeId, PersistentState, PreVoteRequest, Role,
    RpcMessage, Term, VoteTally, VolatileState,
};
use std::collections::HashMap;
use storage::{Command, KvStore, Persistence, Recovered};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};

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
}

pub struct ClusterHandle {
    pub command_tx: mpsc::Sender<ClientCommand>,
    pub query_tx: mpsc::Sender<ClientQuery>,
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
    pub fn new(
        config: NodeConfig,
        recovered: Recovered,
        peer_outboxes: HashMap<NodeId, mpsc::Sender<RpcMessage>>,
        inbox_rx: mpsc::Receiver<(NodeId, RpcMessage)>,
    ) -> (Self, ClusterHandle) {
        let Recovered { persistence, hard_state: persistent, entries, .. } = recovered;
        let log = Log::from_entries(entries);
        let kv_store = KvStore::new();
        let capacity = config.limits.channel_capacity;
        let (command_tx, command_rx) = mpsc::channel(capacity);
        let (query_tx, query_rx) = mpsc::channel(capacity);
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
            wal_syncs: persistence.wal_sync_count(),
            dropped_messages: 0,
        });

        let cluster = Self {
            config,
            persistent,
            volatile: VolatileState::new(),
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
            dropped_messages: 0,
            pending_commands: HashMap::new(),
            pending_reads: Vec::new(),
            heartbeat_seq: 0,
            noop_index: 0,
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
                    let batch = collect_batch(cmd, &mut self.command_rx, self.config.limits.max_batch);
                    self.handle_client_commands(batch);
                }
                Some(query) = self.query_rx.recv() => {
                    let batch = collect_batch(query, &mut self.query_rx, self.config.limits.max_batch);
                    self.handle_client_queries(batch);
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
        // With no peers there is no one to ask; a one-node cluster just elects itself.
        if self.config.cluster_size() == 1 {
            self.begin_election();
            return;
        }

        let proposed_term = self.persistent.current_term + 1;
        self.pre_vote = Some(PreVoteRound {
            term: proposed_term,
            tally: VoteTally::new(self.config.id, self.config.cluster_size()),
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
        let peer_ids: Vec<NodeId> = self.peer_outboxes.keys().copied().collect();
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
        if self.config.cluster_size() == 1 {
            return;
        }
        let has_quorum = self.leader_state.has_recent_quorum(
            self.config.cluster_size(),
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
        let cluster_size = self.config.cluster_size();
        let last_applied = self.volatile.last_applied;

        let mut still_waiting = Vec::new();
        for read in self.pending_reads.drain(..) {
            let leadership_proven = self.leader_state.has_quorum_acked(read.required_seq, cluster_size);
            if leadership_proven && last_applied >= read.read_index {
                let value = self.kv_store.get(&read.key).cloned();
                let _ = read.respond_to.send(QueryResult::Value(value));
            } else {
                still_waiting.push(read);
            }
        }
        self.pending_reads = still_waiting;
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
        let peers = peer_ids.iter().map(|&id| (id, String::new())).collect();
        let config = NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            metrics_addr: "127.0.0.1:0".to_string(),
            peers,
            storage_dir: dir.to_path_buf(),
            read_mode,
            limits,
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
            seq: 0,
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
}
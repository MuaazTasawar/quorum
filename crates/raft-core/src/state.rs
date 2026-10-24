use std::time::{Duration, Instant};

/// A monotonically increasing Raft term. Every RPC and vote is scoped to a term;
/// a node that sees a higher term than its own immediately steps down to Follower.
pub type Term = u64;

/// A stable identifier for a node in the cluster. Must be unique and consistent
/// across restarts (used as the key in peer address maps and vote tallies).
pub type NodeId = u32;

/// The three roles a Raft node can occupy. Transitions are strictly:
/// Follower -> Candidate -> Leader, or any state -> Follower (on higher term).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Persistent state that MUST survive a crash/restart before responding to any RPC.
/// This is the subset of Raft state that goes to the WAL before being acted on -
/// losing `current_term` or `voted_for` after a crash can violate Raft's safety
/// guarantees (a node could vote twice in the same term after restarting).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

/// Volatile state, reset on every restart, reconstructed from persistent state + log.
#[derive(Debug)]
pub struct VolatileState {
    pub role: Role,
    /// Index of highest log entry known to be committed (replicated to a majority).
    pub commit_index: u64,
    /// Index of highest log entry applied to the local state machine.
    pub last_applied: u64,
    /// When this node last heard from a leader (or granted a vote). Used to detect
    /// election timeout - if `now - last_heartbeat > timeout`, a Follower becomes
    /// a Candidate and starts an election.
    pub last_heartbeat: Instant,
}

/// Leader-only volatile state, reinitialized every time a node becomes Leader.
/// Tracks per-follower replication progress so the leader knows what to send next
/// and when an entry has reached a majority (and can be committed).
#[derive(Debug, Default)]
pub struct LeaderState {
    /// For each follower: index of the next log entry to send them.
    /// Optimistically initialized to leader's (last log index + 1), decremented
    /// on AppendEntries rejection until a matching prefix is found.
    pub next_index: std::collections::HashMap<NodeId, u64>,
    /// For each follower: highest log index known to be replicated on that follower.
    /// Used to compute commit_index (index replicated on a majority).
    pub match_index: std::collections::HashMap<NodeId, u64>,
    /// For each follower: when it last answered this leader (any AppendEntries
    /// response from the current term, accepting or rejecting - either proves the
    /// follower is reachable and still recognises this leader). Check-quorum uses
    /// this to notice a leader that has been cut off from the majority.
    pub last_ack: std::collections::HashMap<NodeId, Instant>,
    /// For each follower: the highest heartbeat round (`AppendEntriesRequest::seq`)
    /// it has answered in this leadership. A read that must prove leadership
    /// waits until a majority has answered a round that began after the read
    /// arrived (ReadIndex).
    pub ack_seq: std::collections::HashMap<NodeId, u64>,
}

impl LeaderState {
    /// Records that `peer` answered this leader at `now`.
    pub fn record_ack(&mut self, peer: NodeId, now: Instant) {
        self.last_ack.insert(peer, now);
    }

    /// Records that `peer` answered heartbeat round `seq`. Only ever moves forward:
    /// a late answer to an older round cannot lower what the peer has confirmed.
    pub fn record_ack_seq(&mut self, peer: NodeId, seq: u64) {
        let confirmed = self.ack_seq.entry(peer).or_insert(0);
        *confirmed = (*confirmed).max(seq);
    }

    /// True if, counting the leader itself, a majority of the cluster has answered
    /// heartbeat round `seq` or a later one. For a read that arrived before round
    /// `seq` was sent, this proves a majority still recognised this node as leader
    /// after the read arrived, so no newer leader can have committed a write the
    /// read would miss.
    pub fn has_quorum_acked(&self, seq: u64, cluster_size: usize) -> bool {
        let confirmed_peers = self.ack_seq.values().filter(|&&acked| acked >= seq).count();
        confirmed_peers + 1 >= cluster_size / 2 + 1
    }

    /// True if, counting the leader itself, a majority of the cluster has answered
    /// within `window` of `now`. A leader for which this is false cannot commit
    /// anything and no longer has any claim to being the leader, so it should
    /// step down rather than keep accepting writes it cannot complete.
    pub fn has_recent_quorum(&self, cluster_size: usize, now: Instant, window: Duration) -> bool {
        let recent_peers = self
            .last_ack
            .values()
            .filter(|&&acked_at| now.saturating_duration_since(acked_at) <= window)
            .count();
        recent_peers + 1 >= cluster_size / 2 + 1
    }
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            last_heartbeat: Instant::now(),
        }
    }

    /// True if enough time has passed without a heartbeat/vote grant that this
    /// Follower should become a Candidate and start an election.
    pub fn election_timed_out(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_millis(300);

    fn leader_with_acks(acks: &[(NodeId, Instant)]) -> LeaderState {
        let mut state = LeaderState::default();
        for &(peer, at) in acks {
            state.record_ack(peer, at);
        }
        state
    }

    #[test]
    fn a_single_node_cluster_always_has_quorum() {
        let state = LeaderState::default();
        assert!(state.has_recent_quorum(1, Instant::now(), WINDOW));
    }

    #[test]
    fn three_nodes_need_one_recent_peer_besides_the_leader() {
        let now = Instant::now();
        assert!(!leader_with_acks(&[]).has_recent_quorum(3, now, WINDOW));
        assert!(leader_with_acks(&[(2, now)]).has_recent_quorum(3, now, WINDOW));
    }

    #[test]
    fn five_nodes_need_two_recent_peers_besides_the_leader() {
        let now = Instant::now();
        assert!(!leader_with_acks(&[(2, now)]).has_recent_quorum(5, now, WINDOW));
        assert!(leader_with_acks(&[(2, now), (3, now)]).has_recent_quorum(5, now, WINDOW));
    }

    #[test]
    fn acks_older_than_the_window_do_not_count() {
        let long_ago = Instant::now();
        let now = long_ago + Duration::from_millis(500);
        let state = leader_with_acks(&[(2, long_ago), (3, long_ago)]);
        assert!(!state.has_recent_quorum(3, now, WINDOW));
    }

    #[test]
    fn a_newer_ack_replaces_an_older_one() {
        let long_ago = Instant::now();
        let now = long_ago + Duration::from_millis(500);
        let mut state = leader_with_acks(&[(2, long_ago)]);
        assert!(!state.has_recent_quorum(3, now, WINDOW));
        state.record_ack(2, now);
        assert!(state.has_recent_quorum(3, now, WINDOW));
    }

    // ---- heartbeat round acknowledgements (ReadIndex) -----------------------

    #[test]
    fn a_single_node_cluster_confirms_any_round_by_itself() {
        assert!(LeaderState::default().has_quorum_acked(99, 1));
    }

    #[test]
    fn a_round_is_confirmed_once_a_majority_has_answered_it() {
        let mut state = LeaderState::default();
        assert!(!state.has_quorum_acked(5, 5), "nobody has answered yet");
        state.record_ack_seq(2, 5);
        assert!(!state.has_quorum_acked(5, 5), "leader + 1 of 5 is not a majority");
        state.record_ack_seq(3, 5);
        assert!(state.has_quorum_acked(5, 5), "leader + 2 of 5 is a majority");
    }

    #[test]
    fn answers_to_an_earlier_round_do_not_confirm_a_later_one() {
        let mut state = LeaderState::default();
        state.record_ack_seq(2, 4);
        assert!(!state.has_quorum_acked(5, 3), "round 4 was sent before round 5 began");
        assert!(state.has_quorum_acked(4, 3));
    }

    #[test]
    fn answering_a_later_round_also_confirms_earlier_ones() {
        let mut state = LeaderState::default();
        state.record_ack_seq(2, 9);
        assert!(state.has_quorum_acked(3, 3));
    }

    #[test]
    fn a_late_answer_never_lowers_what_a_peer_has_confirmed() {
        let mut state = LeaderState::default();
        state.record_ack_seq(2, 9);
        state.record_ack_seq(2, 3);
        assert_eq!(state.ack_seq.get(&2), Some(&9));
    }
}
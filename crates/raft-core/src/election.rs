use crate::log::Log;
use crate::messages::{RequestVoteRequest, RequestVoteResponse};
use crate::state::{NodeId, PersistentState, VolatileState, Role};
use rand::Rng;
use std::collections::HashSet;
use std::time::Duration;

/// Election timeouts must be randomized within a range so that, when a leader
/// dies, followers don't all become candidates in the same instant and split
/// the vote forever (Raft paper §5.2). 150-300ms is the paper's reference range;
/// exposed as a config so tests can shrink it for fast simulated elections.
#[derive(Debug, Clone, Copy)]
pub struct ElectionTimeoutConfig {
    pub min: Duration,
    pub max: Duration,
}

impl Default for ElectionTimeoutConfig {
    fn default() -> Self {
        Self { min: Duration::from_millis(150), max: Duration::from_millis(300) }
    }
}

impl ElectionTimeoutConfig {
    pub fn random_timeout(&self) -> Duration {
        let min_ms = self.min.as_millis() as u64;
        let max_ms = self.max.as_millis() as u64;
        let ms = rand::thread_rng().gen_range(min_ms..=max_ms);
        Duration::from_millis(ms)
    }
}

/// Tracks votes received while this node is a Candidate. Not part of
/// `PersistentState` (doesn't survive restart - a restarted node just starts a
/// fresh election) and not part of `LeaderState` (only relevant pre-leadership).
#[derive(Debug, Default)]
pub struct VoteTally {
    votes_received: HashSet<NodeId>,
    cluster_size: usize,
}

impl VoteTally {
    pub fn new(self_id: NodeId, cluster_size: usize) -> Self {
        let mut votes_received = HashSet::new();
        votes_received.insert(self_id); // a candidate always votes for itself
        Self { votes_received, cluster_size }
    }

    pub fn record_vote(&mut self, voter: NodeId) {
        self.votes_received.insert(voter);
    }

    /// A candidate becomes leader once it has votes from a strict majority of
    /// the cluster (including itself) - e.g. 3 out of 5, not 3 out of 3 others.
    pub fn has_majority(&self) -> bool {
        self.votes_received.len() > self.cluster_size / 2
    }
}

/// Transitions this node from Follower/Candidate into a new election round:
/// increments the term, votes for itself, and returns the request to broadcast
/// to every peer. Callers (the node crate) are responsible for actually sending
/// it over the network and resetting the election timer.
pub fn start_election(
    persistent: &mut PersistentState,
    volatile: &mut VolatileState,
    log: &Log,
    self_id: NodeId,
) -> RequestVoteRequest {
    persistent.current_term += 1;
    persistent.voted_for = Some(self_id);
    volatile.role = Role::Candidate;
    volatile.last_heartbeat = std::time::Instant::now();

    RequestVoteRequest {
        term: persistent.current_term,
        candidate_id: self_id,
        last_log_index: log.last_index(),
        last_log_term: log.last_term(),
    }
}

/// Implements the RequestVote RPC receiver logic from the Raft paper (§5.2, §5.4.1).
/// Two conditions must both hold for a vote to be granted:
///   1. The requester's term is >= ours (an old candidate can't win votes), and
///      we haven't already voted for someone else this term.
///   2. The requester's log is at least as up-to-date as ours (compares last
///      log term first, then index) - this is what prevents a node with missing
///      committed entries from ever becoming leader.
pub fn handle_request_vote(
    persistent: &mut PersistentState,
    volatile: &mut VolatileState,
    log: &Log,
    req: &RequestVoteRequest,
) -> RequestVoteResponse {
    // Stale term: reject outright, tell the candidate our (higher) term so it
    // can step down out of its own stale candidacy.
    if req.term < persistent.current_term {
        return RequestVoteResponse { term: persistent.current_term, vote_granted: false };
    }

    // Newer term: we must update ours and revert to Follower before considering
    // the vote - a Candidate or Leader that sees a higher term is no longer valid.
    if req.term > persistent.current_term {
        persistent.current_term = req.term;
        persistent.voted_for = None;
        volatile.role = Role::Follower;
    }

    let already_voted_for_other = matches!(
        persistent.voted_for,
        Some(voted) if voted != req.candidate_id
    );

    let candidate_log_up_to_date = (req.last_log_term, req.last_log_index)
        >= (log.last_term(), log.last_index());

    if !already_voted_for_other && candidate_log_up_to_date {
        persistent.voted_for = Some(req.candidate_id);
        volatile.last_heartbeat = std::time::Instant::now(); // granting a vote counts as contact
        RequestVoteResponse { term: persistent.current_term, vote_granted: true }
    } else {
        RequestVoteResponse { term: persistent.current_term, vote_granted: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_state() -> (PersistentState, VolatileState, Log) {
        (PersistentState::default(), VolatileState::new(), Log::new())
    }

    #[test]
    fn grants_vote_for_higher_term_with_up_to_date_log() {
        let (mut persistent, mut volatile, log) = fresh_state();
        let req = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let resp = handle_request_vote(&mut persistent, &mut volatile, &log, &req);
        assert!(resp.vote_granted);
        assert_eq!(persistent.voted_for, Some(2));
    }

    #[test]
    fn rejects_vote_for_stale_term() {
        let (mut persistent, mut volatile, log) = fresh_state();
        persistent.current_term = 5;
        let req = RequestVoteRequest {
            term: 3,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let resp = handle_request_vote(&mut persistent, &mut volatile, &log, &req);
        assert!(!resp.vote_granted);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn rejects_second_vote_in_same_term() {
        let (mut persistent, mut volatile, log) = fresh_state();
        let req1 = RequestVoteRequest { term: 1, candidate_id: 2, last_log_index: 0, last_log_term: 0 };
        let req2 = RequestVoteRequest { term: 1, candidate_id: 3, last_log_index: 0, last_log_term: 0 };

        let resp1 = handle_request_vote(&mut persistent, &mut volatile, &log, &req1);
        let resp2 = handle_request_vote(&mut persistent, &mut volatile, &log, &req2);

        assert!(resp1.vote_granted);
        assert!(!resp2.vote_granted);
    }

    #[test]
    fn rejects_vote_when_candidate_log_is_behind() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        log.append(1, b"existing entry".to_vec()); // our log has 1 entry at term 1

        let req = RequestVoteRequest {
            term: 2,
            candidate_id: 2,
            last_log_index: 0, // candidate's log is empty - behind ours
            last_log_term: 0,
        };
        let resp = handle_request_vote(&mut persistent, &mut volatile, &log, &req);
        assert!(!resp.vote_granted);
    }

    #[test]
    fn vote_tally_reaches_majority_at_correct_threshold() {
        let mut tally = VoteTally::new(1, 5); // 5-node cluster, self_id = 1
        assert!(!tally.has_majority()); // 1/5 votes
        tally.record_vote(2);
        assert!(!tally.has_majority()); // 2/5 votes
        tally.record_vote(3);
        assert!(tally.has_majority()); // 3/5 votes - majority reached
    }
}

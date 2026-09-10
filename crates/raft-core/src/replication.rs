use crate::log::Log;
use crate::messages::{AppendEntriesRequest, AppendEntriesResponse};
use crate::state::{LeaderState, NodeId, PersistentState, Role, Term, VolatileState};

/// Implements the AppendEntries RPC receiver logic (Raft paper §5.3). Used both
/// for actual log replication and as a heartbeat (when `req.entries` is empty) -
/// there's no separate heartbeat RPC in this implementation, matching the paper.
pub fn handle_append_entries(
    persistent: &mut PersistentState,
    volatile: &mut VolatileState,
    log: &mut Log,
    req: &AppendEntriesRequest,
) -> AppendEntriesResponse {
    // Stale leader: reject and tell it our higher term so it steps down.
    if req.term < persistent.current_term {
        return AppendEntriesResponse {
            term: persistent.current_term,
            success: false,
            conflict_index: None,
        };
    }

    // Valid (or newer) leader contacted us: accept its term, revert to Follower.
    // This covers a Candidate losing an election it didn't know it lost, and a
    // Follower simply hearing from the current leader.
    if req.term > persistent.current_term {
        persistent.current_term = req.term;
        persistent.voted_for = None;
    }
    volatile.role = Role::Follower;
    volatile.last_heartbeat = std::time::Instant::now();

    // Log-matching check: does our log agree with the leader on the entry
    // immediately preceding what it's sending? If not, reject so the leader
    // backs up next_index and retries with an earlier prev_log_index.
    if !log.matches(req.prev_log_index, req.prev_log_term) {
        // Conflict-index optimization (paper §5.3 / extended Raft): instead of
        // the leader decrementing next_index by 1 per rejected RPC (slow when
        // a follower is far behind), tell it the last index we actually have,
        // so it can jump back in one round-trip instead of many.
        let conflict_index = log.last_index().min(req.prev_log_index.saturating_sub(1));
        return AppendEntriesResponse {
            term: persistent.current_term,
            success: false,
            conflict_index: Some(conflict_index),
        };
    }

    // Log matches up to prev_log_index - safe to overwrite anything after it
    // with the leader's entries. Raft safety: this never touches entries at or
    // below commit_index, because a leader never sends prev_log_index below
    // what a majority (including this follower, if it's caught up) has already
    // committed.
    if !req.entries.is_empty() {
        log.truncate_and_append(req.prev_log_index + 1, req.entries.clone());
    }

    // Advance our commit_index to whatever the leader has told us is safe,
    // capped at what we actually have on disk (we may be mid-replication).
    if req.leader_commit > volatile.commit_index {
        volatile.commit_index = req.leader_commit.min(log.last_index());
    }

    AppendEntriesResponse { term: persistent.current_term, success: true, conflict_index: None }
}

/// Builds the AppendEntries request the leader should send to a specific
/// follower, based on that follower's `next_index`. Called on the heartbeat
/// interval and whenever a new entry is appended to the leader's own log.
pub fn build_append_entries(
    persistent: &PersistentState,
    log: &Log,
    leader_id: NodeId,
    follower_next_index: u64,
    commit_index: u64,
) -> AppendEntriesRequest {
    let prev_log_index = follower_next_index.saturating_sub(1);
    let prev_log_term = log.term_at(prev_log_index).unwrap_or(0);

    AppendEntriesRequest {
        term: persistent.current_term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries: log.entries_after(prev_log_index),
        leader_commit: commit_index,
    }
}

/// Updates leader-side replication tracking after receiving a follower's
/// response. On success, advances next_index/match_index for that follower.
/// On failure, backs next_index up using the follower's conflict_index (or a
/// simple decrement as a fallback) so the next AppendEntries has a chance to
/// find a matching prefix.
pub fn handle_append_entries_response(
    leader_state: &mut LeaderState,
    follower_id: NodeId,
    sent_prev_log_index: u64,
    sent_entries_count: u64,
    response: &AppendEntriesResponse,
) {
    if response.success {
        let new_match_index = sent_prev_log_index + sent_entries_count;
        leader_state.match_index.insert(follower_id, new_match_index);
        leader_state.next_index.insert(follower_id, new_match_index + 1);
    } else {
        let retreat_to = response
            .conflict_index
            .unwrap_or_else(|| sent_prev_log_index.saturating_sub(1));
        leader_state
            .next_index
            .insert(follower_id, (retreat_to + 1).max(1));
    }
}

/// Recomputes the leader's commit_index from `match_index` (Raft paper §5.3,
/// §5.4.2). An index N becomes committed once a majority of nodes (leader
/// included) have match_index >= N. Critically, the leader may ONLY do this
/// for entries from its OWN current term - committing an earlier-term entry
/// directly (even if replicated to a majority) can be unsafe if that entry
/// gets silently overwritten by a future leader that never saw it. Earlier
/// entries get committed indirectly, as a side effect of a current-term entry
/// committing on top of them (the log-matching property guarantees everything
/// before a committed entry is also safe).
pub fn advance_commit_index(
    leader_state: &LeaderState,
    log: &Log,
    current_term: Term,
    cluster_size: usize,
    self_match_index: u64,
) -> Option<u64> {
    let majority = cluster_size / 2 + 1;

    // Candidate commit indices: every index any node (including self) has
    // reached, checked from highest to lowest so we find the largest N that
    // qualifies.
    let mut candidates: Vec<u64> = leader_state.match_index.values().copied().collect();
    candidates.push(self_match_index);
    candidates.sort_unstable_by(|a, b| b.cmp(a));

    for &candidate in &candidates {
        if candidate == 0 {
            continue;
        }
        // Only entries from the leader's current term can be committed directly.
        if log.term_at(candidate) != Some(current_term) {
            continue;
        }
        let count = candidates.iter().filter(|&&m| m >= candidate).count();
        if count >= majority {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::Log;

    fn fresh_state() -> (PersistentState, VolatileState, Log) {
        (PersistentState::default(), VolatileState::new(), Log::new())
    }

    #[test]
    fn accepts_heartbeat_on_empty_log() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        let resp = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(resp.success);
    }

    #[test]
    fn rejects_stale_leader_term() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 5;
        let req = AppendEntriesRequest {
            term: 3,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        let resp = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(!resp.success);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn rejects_and_reports_conflict_on_log_mismatch() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        // Follower's log is empty, but leader claims prev_log_index=5 - can't match.
        let req = AppendEntriesRequest {
            term: 1,
            leader_id: 1,
            prev_log_index: 5,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        };
        let resp = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(!resp.success);
        assert!(resp.conflict_index.is_some());
    }

    #[test]
    fn truncates_conflicting_entries_and_appends_new_ones() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 2;
        log.append(1, b"stale-entry".to_vec()); // conflicting entry from an old term

        let new_entry = crate::log::LogEntry { term: 2, index: 1, command: b"correct".to_vec() };
        let req = AppendEntriesRequest {
            term: 2,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![new_entry],
            leader_commit: 0,
        };
        let resp = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(resp.success);
        assert_eq!(log.get(1).unwrap().command, b"correct".to_vec());
    }

    #[test]
    fn commit_index_only_advances_for_current_term_entries() {
        let mut log = Log::new();
        log.append(1, b"old-term-entry".to_vec()); // index 1, term 1
        log.append(2, b"current-term-entry".to_vec()); // index 2, term 2

        let mut leader_state = LeaderState::default();
        // 3-node cluster: leader (self_match_index) + 2 followers both caught up to index 1.
        leader_state.match_index.insert(2, 1);
        leader_state.match_index.insert(3, 1);

        // Even though index 1 is replicated on all 3 nodes (majority), it's from
        // term 1, not the leader's current term 2 - must NOT commit directly.
        let result = advance_commit_index(&leader_state, &log, 2, 3, 1);
        assert_eq!(result, None);

        // Now followers catch up to index 2 (current term) - should commit.
        leader_state.match_index.insert(2, 2);
        leader_state.match_index.insert(3, 2);
        let result = advance_commit_index(&leader_state, &log, 2, 3, 2);
        assert_eq!(result, Some(2));
    }

    #[test]
    fn replication_response_advances_next_and_match_index_on_success() {
        let mut leader_state = LeaderState::default();
        let response = AppendEntriesResponse { term: 1, success: true, conflict_index: None };
        handle_append_entries_response(&mut leader_state, 2, 0, 3, &response);
        assert_eq!(leader_state.match_index.get(&2), Some(&3));
        assert_eq!(leader_state.next_index.get(&2), Some(&4));
    }

    #[test]
    fn replication_response_retreats_next_index_on_conflict() {
        let mut leader_state = LeaderState::default();
        let response =
            AppendEntriesResponse { term: 1, success: false, conflict_index: Some(2) };
        handle_append_entries_response(&mut leader_state, 2, 10, 1, &response);
        assert_eq!(leader_state.next_index.get(&2), Some(&3));
    }
}
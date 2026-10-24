use crate::log::{Log, LogEntry};
use crate::messages::{AppendEntriesRequest, AppendEntriesResponse};
use crate::state::{LeaderState, NodeId, PersistentState, Role, Term, VolatileState};

/// What `handle_append_entries` actually did to the follower's log. raft-core
/// does no I/O, so it reports these changes and lets the caller mirror them
/// onto disk (the WAL) before acknowledging the request.
#[derive(Debug, Clone, Default)]
pub struct AppendEffect {
    /// `Some(i)` if existing entries with index >= `i` were discarded because
    /// they conflicted with the leader's log. The WAL must drop the same suffix.
    pub truncated_from: Option<u64>,
    /// Entries that were genuinely new to this follower and were appended to
    /// its log. Entries the follower already had are NOT repeated here, so the
    /// WAL never receives duplicate records.
    pub appended: Vec<LogEntry>,
}

/// Implements the AppendEntries RPC receiver logic (Raft paper §5.3). Used both
/// for actual log replication and as a heartbeat (when `req.entries` is empty) -
/// there's no separate heartbeat RPC in this implementation, matching the paper.
///
/// Returns the response to send plus the `AppendEffect` describing exactly how
/// the log changed.
pub fn handle_append_entries(
    persistent: &mut PersistentState,
    volatile: &mut VolatileState,
    log: &mut Log,
    req: &AppendEntriesRequest,
) -> (AppendEntriesResponse, AppendEffect) {
    let mut effect = AppendEffect::default();

    // Stale leader: reject and tell it our higher term so it steps down.
    if req.term < persistent.current_term {
        let resp = AppendEntriesResponse {
            term: persistent.current_term,
            success: false,
            conflict_index: None,
            match_index: 0,
            seq: req.seq,
        };
        return (resp, effect);
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

    // Protocol sanity check: entry N of the batch must claim index
    // prev_log_index + N + 1. A batch that doesn't is a bug in the sender;
    // applying it would corrupt the log, so reject it without touching anything.
    let well_formed = req
        .entries
        .iter()
        .enumerate()
        .all(|(offset, e)| e.index == req.prev_log_index + 1 + offset as u64);
    if !well_formed {
        let resp = AppendEntriesResponse {
            term: persistent.current_term,
            success: false,
            conflict_index: None,
            match_index: 0,
            seq: req.seq,
        };
        return (resp, effect);
    }

    // Log-matching check: does our log agree with the leader on the entry
    // immediately preceding what it's sending? If not, reject so the leader
    // backs up next_index and retries with an earlier prev_log_index.
    if !log.matches(req.prev_log_index, req.prev_log_term) {
        // Conflict-index optimization (paper §5.3 / extended Raft): instead of
        // the leader decrementing next_index by 1 per rejected RPC (slow when
        // a follower is far behind), tell it the last index we actually have,
        // so it can jump back in one round-trip instead of many.
        let conflict_index = log.last_index().min(req.prev_log_index.saturating_sub(1));
        let resp = AppendEntriesResponse {
            term: persistent.current_term,
            success: false,
            conflict_index: Some(conflict_index),
            match_index: 0,
            seq: req.seq,
        };
        return (resp, effect);
    }

    // Paper §5.3, receiver steps 3 and 4: walk the incoming entries and find
    // the first one that is either missing from our log or conflicts with it
    // (same index, different term). Entries we already hold are skipped. We
    // only ever truncate on a real conflict - a delayed or duplicated
    // AppendEntries carrying an older, shorter batch must not be able to delete
    // entries we have already accepted.
    let mut first_new_offset: Option<usize> = None;
    for (offset, entry) in req.entries.iter().enumerate() {
        match log.term_at(entry.index) {
            Some(existing_term) if existing_term == entry.term => continue,
            Some(_) => {
                debug_assert!(
                    entry.index > volatile.commit_index,
                    "attempted to truncate committed entry {} (commit_index {})",
                    entry.index,
                    volatile.commit_index
                );
                effect.truncated_from = Some(entry.index);
                first_new_offset = Some(offset);
                break;
            }
            None => {
                first_new_offset = Some(offset);
                break;
            }
        }
    }

    if let Some(offset) = first_new_offset {
        let new_entries = req.entries[offset..].to_vec();
        let from_index = new_entries[0].index;
        log.truncate_and_append(from_index, new_entries.clone());
        effect.appended = new_entries;
    }

    // Index of the last entry this request vouches for. Everything up to here is
    // known to match the leader's log. Anything past it in our log may be a stale
    // suffix the leader has not reconciled yet, so it must NOT be committed on
    // the strength of leader_commit alone (paper §5.3 step 5).
    let last_new_index = req.prev_log_index + req.entries.len() as u64;

    // Never move commit_index backwards: a delayed request with an old,
    // short prefix must not undo progress made by a newer one.
    if req.leader_commit > volatile.commit_index {
        let advanced = req.leader_commit.min(last_new_index);
        volatile.commit_index = volatile.commit_index.max(advanced);
    }

    let resp = AppendEntriesResponse {
        term: persistent.current_term,
        success: true,
        conflict_index: None,
        match_index: last_new_index,
        seq: req.seq,
    };
    (resp, effect)
}

/// Builds the AppendEntries request the leader should send to a specific
/// follower, based on that follower's `next_index`. Called on the heartbeat
/// interval and whenever a new entry is appended to the leader's own log.
/// `seq` numbers the broadcast round this request belongs to; the follower
/// echoes it in its response.
pub fn build_append_entries(
    persistent: &PersistentState,
    log: &Log,
    leader_id: NodeId,
    follower_next_index: u64,
    commit_index: u64,
    seq: u64,
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
        seq,
    }
}

/// Updates leader-side replication tracking after receiving a follower's
/// response. The response is self-describing: on success it carries the index
/// the follower verified (`match_index`), so the leader never infers it from
/// its own current log tail. Responses can arrive late or out of order, so both
/// branches only ever move `next_index` / `match_index` in the safe direction.
pub fn handle_append_entries_response(
    leader_state: &mut LeaderState,
    follower_id: NodeId,
    response: &AppendEntriesResponse,
) {
    let known_match = leader_state.match_index.get(&follower_id).copied().unwrap_or(0);
    let current_next = leader_state.next_index.get(&follower_id).copied();

    if response.success {
        // match_index is monotonic: a late response for an older request can
        // never pull it backwards.
        let new_match = known_match.max(response.match_index);
        leader_state.match_index.insert(follower_id, new_match);
        leader_state.next_index.insert(follower_id, new_match + 1);
    } else {
        let hinted_next = match response.conflict_index {
            Some(conflict_index) => conflict_index + 1,
            // No hint (malformed batch): fall back to a one-step retreat.
            None => current_next.unwrap_or(2).saturating_sub(1),
        };
        // A rejection may only move next_index DOWN (a late rejection with an
        // optimistic hint must not undo a retreat already made), and never below
        // match_index + 1, because entries up to match_index are already
        // known to match.
        let new_next = current_next
            .map_or(hinted_next, |n| n.min(hinted_next))
            .max(known_match + 1)
            .max(1);
        leader_state.next_index.insert(follower_id, new_next);
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

    fn entry(term: Term, index: u64, command: &str) -> LogEntry {
        LogEntry { term, index, command: command.as_bytes().to_vec() }
    }

    fn request(
        term: Term,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term,
            leader_id: 1,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            seq: 0,
        }
    }

    fn ok_response(match_index: u64) -> AppendEntriesResponse {
        AppendEntriesResponse { term: 1, success: true, conflict_index: None, match_index, seq: 0 }
    }

    fn rejection(conflict_index: Option<u64>) -> AppendEntriesResponse {
        AppendEntriesResponse { term: 1, success: false, conflict_index, match_index: 0, seq: 0 }
    }

    // ---- receiver side: handle_append_entries -------------------------------

    #[test]
    fn accepts_heartbeat_on_empty_log() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        let req = request(1, 0, 0, vec![], 0);
        let (resp, effect) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(resp.success);
        assert_eq!(resp.match_index, 0);
        assert!(effect.truncated_from.is_none());
        assert!(effect.appended.is_empty());
    }

    #[test]
    fn rejects_stale_leader_term() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 5;
        let req = request(3, 0, 0, vec![], 0);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(!resp.success);
        assert_eq!(resp.term, 5);
    }

    #[test]
    fn rejects_and_reports_conflict_on_log_mismatch() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        // Follower's log is empty, but leader claims prev_log_index=5 - can't match.
        let req = request(1, 5, 1, vec![], 0);
        let (resp, effect) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(!resp.success);
        assert!(resp.conflict_index.is_some());
        assert_eq!(resp.match_index, 0);
        assert!(effect.appended.is_empty());
    }

    #[test]
    fn truncates_conflicting_entries_and_appends_new_ones() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 2;
        log.append(1, b"stale-entry".to_vec()); // conflicting entry from an old term

        let req = request(2, 0, 0, vec![entry(2, 1, "correct")], 0);
        let (resp, effect) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(resp.success);
        assert_eq!(log.get(1).unwrap().command, b"correct".to_vec());
        assert_eq!(effect.truncated_from, Some(1));
        assert_eq!(effect.appended.len(), 1);
    }

    #[test]
    fn conflict_in_the_middle_truncates_only_from_the_first_mismatch() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 2;
        log.append(1, b"a".to_vec());
        log.append(1, b"b".to_vec());
        log.append(1, b"c".to_vec());

        // Leader agrees on index 1, then diverges from index 2 onward.
        let req = request(2, 1, 1, vec![entry(2, 2, "B"), entry(2, 3, "C")], 0);
        let (resp, effect) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);

        assert!(resp.success);
        assert_eq!(resp.match_index, 3);
        assert_eq!(effect.truncated_from, Some(2));
        assert_eq!(effect.appended.len(), 2);
        assert_eq!(log.term_at(1), Some(1)); // untouched
        assert_eq!(log.term_at(2), Some(2));
        assert_eq!(log.term_at(3), Some(2));
    }

    #[test]
    fn delayed_shorter_request_does_not_truncate_entries_already_accepted() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        log.append(1, b"a".to_vec());
        log.append(1, b"b".to_vec());
        log.append(1, b"c".to_vec());

        // A late-arriving request from earlier in the same term that only
        // carries entry 1. Everything it says already matches our log.
        let req = request(1, 0, 0, vec![entry(1, 1, "a")], 0);
        let (resp, effect) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);

        assert!(resp.success);
        assert_eq!(resp.match_index, 1);
        assert!(effect.truncated_from.is_none());
        assert!(effect.appended.is_empty());
        assert_eq!(log.last_index(), 3, "entries 2 and 3 must survive");
    }

    #[test]
    fn appends_only_the_entries_it_does_not_already_have() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        log.append(1, b"a".to_vec());
        log.append(1, b"b".to_vec());

        let req = request(
            1,
            0,
            0,
            vec![entry(1, 1, "a"), entry(1, 2, "b"), entry(1, 3, "c")],
            0,
        );
        let (resp, effect) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);

        assert!(resp.success);
        assert_eq!(resp.match_index, 3);
        assert!(effect.truncated_from.is_none());
        assert_eq!(effect.appended.len(), 1);
        assert_eq!(effect.appended[0].index, 3);
        assert_eq!(log.last_index(), 3);
    }

    #[test]
    fn rejects_batch_whose_entry_indices_do_not_line_up() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        // Claims to follow index 0 but the first entry says it is index 5.
        let req = request(1, 0, 0, vec![entry(1, 5, "bogus")], 0);
        let (resp, effect) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);

        assert!(!resp.success);
        assert!(effect.appended.is_empty());
        assert_eq!(log.last_index(), 0, "log must be untouched");
    }

    #[test]
    fn commit_index_is_capped_at_the_last_entry_the_request_vouches_for() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        // Follower has 5 entries, but only entry 1 is known to match the leader.
        for _ in 0..5 {
            log.append(1, b"x".to_vec());
        }
        let req = request(1, 0, 0, vec![entry(1, 1, "x")], 5);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);

        assert!(resp.success);
        assert_eq!(
            volatile.commit_index, 1,
            "must not commit the unverified tail just because leader_commit is higher"
        );
    }

    #[test]
    fn heartbeat_commit_is_capped_at_prev_log_index() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        for _ in 0..3 {
            log.append(1, b"x".to_vec());
        }
        let req = request(1, 2, 1, vec![], 3);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);

        assert!(resp.success);
        assert_eq!(resp.match_index, 2);
        assert_eq!(volatile.commit_index, 2);
    }

    #[test]
    fn delayed_request_cannot_move_commit_index_backwards() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        for _ in 0..6 {
            log.append(1, b"x".to_vec());
        }
        volatile.commit_index = 5;

        // Late request: short prefix, leader_commit still above our commit_index.
        let req = request(1, 0, 0, vec![entry(1, 1, "x")], 6);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);

        assert!(resp.success);
        assert_eq!(volatile.commit_index, 5, "commit_index must be monotonic");
    }

    #[test]
    fn success_response_reports_the_verified_index() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        log.append(1, b"a".to_vec());
        log.append(1, b"b".to_vec());

        let req = request(
            1,
            2,
            1,
            vec![entry(1, 3, "c"), entry(1, 4, "d"), entry(1, 5, "e")],
            0,
        );
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(resp.success);
        assert_eq!(resp.match_index, 5);
    }

    // ---- leader side: handle_append_entries_response ------------------------

    #[test]
    fn replication_response_advances_next_and_match_index_on_success() {
        let mut leader_state = LeaderState::default();
        handle_append_entries_response(&mut leader_state, 2, &ok_response(3));
        assert_eq!(leader_state.match_index.get(&2), Some(&3));
        assert_eq!(leader_state.next_index.get(&2), Some(&4));
    }

    #[test]
    fn late_success_response_cannot_move_match_index_backwards() {
        let mut leader_state = LeaderState::default();
        leader_state.match_index.insert(2, 5);
        leader_state.next_index.insert(2, 6);

        handle_append_entries_response(&mut leader_state, 2, &ok_response(2));

        assert_eq!(leader_state.match_index.get(&2), Some(&5));
        assert_eq!(leader_state.next_index.get(&2), Some(&6));
    }

    #[test]
    fn replication_response_retreats_next_index_on_conflict() {
        let mut leader_state = LeaderState::default();
        leader_state.next_index.insert(2, 11);
        handle_append_entries_response(&mut leader_state, 2, &rejection(Some(2)));
        assert_eq!(leader_state.next_index.get(&2), Some(&3));
    }

    #[test]
    fn rejection_never_retreats_below_what_is_already_matched() {
        let mut leader_state = LeaderState::default();
        leader_state.match_index.insert(2, 4);
        leader_state.next_index.insert(2, 5);

        handle_append_entries_response(&mut leader_state, 2, &rejection(Some(1)));

        assert_eq!(leader_state.next_index.get(&2), Some(&5));
    }

    #[test]
    fn late_rejection_with_an_optimistic_hint_cannot_raise_next_index() {
        let mut leader_state = LeaderState::default();
        leader_state.next_index.insert(2, 3);

        handle_append_entries_response(&mut leader_state, 2, &rejection(Some(8)));

        assert_eq!(leader_state.next_index.get(&2), Some(&3));
    }

    #[test]
    fn rejection_without_a_hint_retreats_one_step() {
        let mut leader_state = LeaderState::default();
        leader_state.next_index.insert(2, 7);

        handle_append_entries_response(&mut leader_state, 2, &rejection(None));

        assert_eq!(leader_state.next_index.get(&2), Some(&6));
    }

    // ---- commit rule --------------------------------------------------------

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

    // ---- heartbeat round numbers (ReadIndex support) ------------------------

    fn with_seq(mut req: AppendEntriesRequest, seq: u64) -> AppendEntriesRequest {
        req.seq = seq;
        req
    }

    #[test]
    fn a_successful_response_echoes_the_round_number() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        let req = with_seq(request(1, 0, 0, vec![entry(1, 1, "a")], 0), 42);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(resp.success);
        assert_eq!(resp.seq, 42);
    }

    #[test]
    fn a_rejection_echoes_the_round_number_too() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        // prev_log_index 5 cannot match an empty log.
        let req = with_seq(request(1, 5, 1, vec![], 0), 7);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(!resp.success);
        assert_eq!(resp.seq, 7, "a rejection still proves the follower accepted this leader");
    }

    #[test]
    fn a_stale_leaders_rejection_echoes_the_round_number() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 5;
        let req = with_seq(request(3, 0, 0, vec![], 0), 9);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(!resp.success);
        assert_eq!(resp.seq, 9);
    }

    #[test]
    fn a_malformed_batch_rejection_echoes_the_round_number() {
        let (mut persistent, mut volatile, mut log) = fresh_state();
        persistent.current_term = 1;
        let req = with_seq(request(1, 0, 0, vec![entry(1, 5, "bogus")], 0), 11);
        let (resp, _) = handle_append_entries(&mut persistent, &mut volatile, &mut log, &req);
        assert!(!resp.success);
        assert_eq!(resp.seq, 11);
    }

    #[test]
    fn built_requests_carry_the_round_number_they_are_given() {
        let persistent = PersistentState { current_term: 2, voted_for: None };
        let log = Log::new();
        let req = build_append_entries(&persistent, &log, 1, 1, 0, 123);
        assert_eq!(req.seq, 123);
        assert_eq!(req.term, 2);
    }
}
use crate::state::Term;
use serde::{Deserialize, Serialize};

/// A single entry in the replicated log. `command` is opaque bytes at this layer -
/// raft-core doesn't know or care that it's a KV Put/Delete; that's the storage
/// crate's state machine's job to interpret once the entry is committed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The term in which this entry was created by a leader. Used for the
    /// log-matching property: two entries with the same (index, term) are
    /// guaranteed to hold the same command, and every entry up to that point
    /// in both logs is identical.
    pub term: Term,
    /// 1-indexed position in the log. Index 0 is reserved as a sentinel
    /// "before the log starts" entry, never actually stored.
    pub index: u64,
    pub command: Vec<u8>,
}

/// What `Log::install_snapshot_point` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotInstall {
    /// The log already starts at or past this snapshot point: nothing changed.
    Ignored,
    /// The log held the snapshot's last entry (same index and term), so the
    /// entries after it were kept (Raft paper §7, step 6).
    KeptSuffix,
    /// The log did not match the snapshot and was discarded entirely (§7, step 7).
    ReplacedLog,
}

/// Asked to compact past the end of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BeyondLog {
    pub requested: u64,
    pub last_index: u64,
}

/// In-memory view of the replicated log. The storage crate is responsible for
/// durability (WAL); this type is the logical structure raft-core's election
/// and replication logic reasons about.
///
/// After compaction the log no longer starts at index 1. Everything up to
/// `base_index` has been folded into a snapshot and discarded; only its index and
/// term are remembered (`base_index` / `base_term`), because the log-matching check
/// for the first retained entry needs the term of the entry just before it.
#[derive(Debug, Default)]
pub struct Log {
    /// Index of the last entry covered by the snapshot (0 if there is none).
    base_index: u64,
    /// Term of that entry.
    base_term: Term,
    /// Entries with index `base_index + 1` onward, contiguous.
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reconstructs a Log from a fully-ordered list of entries starting at index 1,
    /// used at node startup after WAL replay. The WAL's on-disk order already IS the
    /// log order (entries are appended in order, truncated in order), so this is
    /// just wrapping already-correct recovered data, not re-deriving anything.
    pub fn from_entries(entries: Vec<LogEntry>) -> Self {
        Self::from_parts(0, 0, entries)
    }

    /// Reconstructs a Log that begins after a snapshot: everything up to
    /// `base_index` (whose term was `base_term`) is gone, and `entries` continue
    /// from `base_index + 1`.
    pub fn from_parts(base_index: u64, base_term: Term, entries: Vec<LogEntry>) -> Self {
        debug_assert!(
            entries.first().map_or(true, |e| e.index == base_index + 1),
            "entries must continue straight after the snapshot point"
        );
        Self { base_index, base_term, entries }
    }

    /// Index of the last entry folded into the snapshot; 0 if the log was never compacted.
    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    /// Term of the entry at `base_index`.
    pub fn base_term(&self) -> Term {
        self.base_term
    }

    /// Index of the first entry still held.
    pub fn first_index(&self) -> u64 {
        self.base_index + 1
    }

    /// How many entries are held in memory (not the same as `last_index` once the
    /// log has been compacted).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|e| e.index).unwrap_or(self.base_index)
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(self.base_term)
    }

    /// The entry at `index`, if it is still held. Entries at or below `base_index`
    /// have been compacted away and are not available.
    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.base_index {
            return None;
        }
        self.entries.get((index - self.base_index - 1) as usize)
    }

    /// The term of the entry at `index`. Also answers for the snapshot's last entry,
    /// which is no longer held but whose term is remembered.
    pub fn term_at(&self, index: u64) -> Option<Term> {
        if index == 0 {
            return None;
        }
        if index == self.base_index {
            return Some(self.base_term);
        }
        self.get(index).map(|e| e.term)
    }

    /// Appends a new entry created locally by this node as leader.
    pub fn append(&mut self, term: Term, command: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry { term, index, command });
        index
    }

    /// Overwrites the log from `from_index` onward with `new_entries`, used when
    /// a Follower receives AppendEntries and must discard conflicting entries.
    /// Raft safety rule: never truncate entries that are already committed -
    /// callers must ensure `from_index > commit_index` before calling this. That
    /// also keeps it above the snapshot point, which is always committed.
    pub fn truncate_and_append(&mut self, from_index: u64, new_entries: Vec<LogEntry>) {
        debug_assert!(from_index > self.base_index, "cannot truncate into a snapshot");
        let keep = from_index.saturating_sub(1).saturating_sub(self.base_index) as usize;
        self.entries.truncate(keep);
        self.entries.extend(new_entries);
    }

    /// Entries strictly after `after_index`, used by the leader to figure out
    /// what to send a follower whose `next_index` is behind. If `after_index` is
    /// below the snapshot point the entries in between are gone and the result has
    /// a gap; check `can_send_after` first.
    pub fn entries_after(&self, after_index: u64) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.index > after_index)
            .cloned()
            .collect()
    }

    /// True if the leader still holds every entry after `after_index`, so it can
    /// send a follower that is caught up to that point the rest of the log. A
    /// follower further behind than the snapshot needs the snapshot itself.
    pub fn can_send_after(&self, after_index: u64) -> bool {
        after_index >= self.base_index
    }

    /// The log-matching check from the Raft paper (§5.3): does this log contain
    /// an entry at `prev_index` with term `prev_term`? Empty log + prev_index 0
    /// trivially matches (start of log). The snapshot's last entry matches by its
    /// remembered term.
    pub fn matches(&self, prev_index: u64, prev_term: Term) -> bool {
        if prev_index == 0 {
            return true;
        }
        self.term_at(prev_index) == Some(prev_term)
    }

    /// Discards every entry up to and including `index` after they have been
    /// captured in a snapshot. The caller must already have made that snapshot
    /// durable: once this returns, those entries exist nowhere else in memory.
    /// Does nothing if the log has already been compacted that far.
    pub fn compact_through(&mut self, index: u64) -> Result<(), BeyondLog> {
        if index <= self.base_index {
            return Ok(());
        }
        let Some(term) = self.term_at(index) else {
            return Err(BeyondLog { requested: index, last_index: self.last_index() });
        };
        let drop_count = (index - self.base_index) as usize;
        self.entries.drain(..drop_count);
        self.base_index = index;
        self.base_term = term;
        Ok(())
    }

    /// Adopts a snapshot received from the leader whose last entry is
    /// (`index`, `term`). If this log already holds that entry the entries after it
    /// are still valid and are kept; otherwise the whole log is discarded
    /// (Raft paper §7, steps 6 and 7).
    pub fn install_snapshot_point(&mut self, index: u64, term: Term) -> SnapshotInstall {
        if index <= self.base_index {
            return SnapshotInstall::Ignored;
        }
        if index <= self.last_index() && self.term_at(index) == Some(term) {
            self.compact_through(index).expect("the entry was just found in the log");
            return SnapshotInstall::KeptSuffix;
        }
        self.entries.clear();
        self.base_index = index;
        self.base_term = term;
        SnapshotInstall::ReplacedLog
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_of(n: u64) -> Log {
        let mut log = Log::new();
        for i in 1..=n {
            log.append(if i <= 3 { 1 } else { 2 }, format!("c{i}").into_bytes());
        }
        log
    }

    #[test]
    fn an_uncompacted_log_behaves_as_before() {
        let log = log_of(5);
        assert_eq!((log.base_index(), log.first_index()), (0, 1));
        assert_eq!((log.last_index(), log.last_term()), (5, 2));
        assert_eq!(log.get(1).unwrap().command, b"c1".to_vec());
        assert!(log.get(0).is_none());
        assert_eq!(log.term_at(0), None);
        assert!(log.matches(0, 0));
    }

    #[test]
    fn compaction_discards_the_prefix_and_remembers_its_end() {
        let mut log = log_of(6);
        log.compact_through(4).unwrap();
        assert_eq!((log.base_index(), log.base_term()), (4, 2));
        assert_eq!(log.first_index(), 5);
        assert_eq!(log.len(), 2);
        assert_eq!(log.last_index(), 6);
        assert!(log.get(4).is_none(), "compacted entries are gone");
        assert!(log.get(1).is_none());
        assert_eq!(log.get(5).unwrap().command, b"c5".to_vec());
    }

    #[test]
    fn the_snapshot_point_still_answers_for_its_term_and_for_matching() {
        let mut log = log_of(6);
        log.compact_through(3).unwrap();
        assert_eq!(log.term_at(3), Some(1), "the snapshot's last entry keeps its term");
        assert_eq!(log.term_at(2), None, "earlier entries do not");
        assert!(log.matches(3, 1));
        assert!(!log.matches(3, 2));
    }

    #[test]
    fn a_fully_compacted_log_reports_the_snapshot_as_its_end() {
        let mut log = log_of(4);
        log.compact_through(4).unwrap();
        assert!(log.is_empty());
        assert_eq!((log.last_index(), log.last_term()), (4, 2));
        assert_eq!(log.append(3, b"next".to_vec()), 5, "numbering continues after the snapshot");
        assert_eq!(log.get(5).unwrap().term, 3);
    }

    #[test]
    fn compacting_twice_or_backwards_is_harmless() {
        let mut log = log_of(6);
        log.compact_through(4).unwrap();
        log.compact_through(2).unwrap();
        assert_eq!(log.base_index(), 4);
        log.compact_through(4).unwrap();
        assert_eq!(log.len(), 2);
        log.compact_through(6).unwrap();
        assert_eq!(log.base_index(), 6);
    }

    #[test]
    fn compacting_past_the_end_of_the_log_is_an_error_and_changes_nothing() {
        let mut log = log_of(3);
        assert_eq!(log.compact_through(9), Err(BeyondLog { requested: 9, last_index: 3 }));
        assert_eq!(log.base_index(), 0);
        assert_eq!(log.len(), 3);
    }

    #[test]
    fn truncation_and_append_work_above_the_snapshot_point() {
        let mut log = log_of(6);
        log.compact_through(3).unwrap();
        let replacement = vec![LogEntry { term: 5, index: 5, command: b"new".to_vec() }];
        log.truncate_and_append(5, replacement);
        assert_eq!(log.last_index(), 5);
        assert_eq!(log.get(4).unwrap().command, b"c4".to_vec());
        assert_eq!(log.get(5).unwrap().term, 5);
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn entries_after_and_can_send_after_respect_the_snapshot() {
        let mut log = log_of(6);
        log.compact_through(4).unwrap();
        assert_eq!(log.entries_after(4).len(), 2);
        assert!(log.can_send_after(4));
        assert!(log.can_send_after(5));
        assert!(!log.can_send_after(3), "entry 4 is gone: that follower needs the snapshot");
    }

    #[test]
    fn a_log_rebuilt_from_parts_continues_after_the_snapshot() {
        let tail = vec![
            LogEntry { term: 2, index: 8, command: b"x".to_vec() },
            LogEntry { term: 2, index: 9, command: b"y".to_vec() },
        ];
        let log = Log::from_parts(7, 2, tail);
        assert_eq!((log.base_index(), log.last_index(), log.last_term()), (7, 9, 2));
        assert_eq!(log.get(8).unwrap().command, b"x".to_vec());
        assert!(log.matches(7, 2));
    }

    #[test]
    fn installing_a_snapshot_the_log_already_holds_keeps_the_entries_after_it() {
        let mut log = log_of(6);
        assert_eq!(log.install_snapshot_point(4, 2), SnapshotInstall::KeptSuffix);
        assert_eq!((log.base_index(), log.last_index()), (4, 6));
        assert_eq!(log.len(), 2);
    }

    #[test]
    fn installing_a_snapshot_that_disagrees_with_the_log_discards_it_all() {
        let mut log = log_of(6);
        // Same index, different term: this log's entry 4 is not the snapshot's.
        assert_eq!(log.install_snapshot_point(4, 9), SnapshotInstall::ReplacedLog);
        assert!(log.is_empty());
        assert_eq!((log.base_index(), log.base_term(), log.last_index()), (4, 9, 4));
    }

    #[test]
    fn installing_a_snapshot_beyond_the_log_replaces_it() {
        let mut log = log_of(3);
        assert_eq!(log.install_snapshot_point(10, 4), SnapshotInstall::ReplacedLog);
        assert_eq!((log.base_index(), log.last_index(), log.last_term()), (10, 10, 4));
    }

    #[test]
    fn installing_an_older_snapshot_than_the_log_starts_at_is_ignored() {
        let mut log = log_of(6);
        log.compact_through(5).unwrap();
        assert_eq!(log.install_snapshot_point(3, 1), SnapshotInstall::Ignored);
        assert_eq!(log.base_index(), 5);
    }
}

use crate::hard_state::{load_hard_state, save_hard_state, HardStateError};
use crate::kv_store::KvStore;
use crate::snapshot::{
    load_snapshot, save_snapshot, write_snapshot_bytes, SnapshotError, SnapshotMetadata,
    SNAPSHOT_FILE,
};
use crate::wal::{Wal, WalError, WalRecovery};
use raft_core::{LogEntry, NodeId, PersistentState, Term};
use std::path::{Path, PathBuf};

/// File names inside a node's storage directory.
pub const WAL_FILE: &str = "wal.log";
pub const HARD_STATE_FILE: &str = "hard.state";

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error(transparent)]
    Wal(#[from] WalError),
    #[error(transparent)]
    HardState(#[from] HardStateError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// The log on disk is missing entries: it should continue at `expected` but
    /// continues at `found`. Carrying on would silently skip committed history.
    #[error("the log on disk has a gap: expected entry {expected}, found {found}")]
    LogGap { expected: u64, found: u64 },
    /// A snapshot older than the one already on disk. Taking it would move the node
    /// backwards and could resurrect entries already discarded.
    #[error("refusing a snapshot at index {requested}: one at index {current} is already stored")]
    StaleSnapshot { requested: u64, current: u64 },
    /// Asked to discard log entries the snapshot on disk does not cover. They would
    /// exist nowhere afterwards.
    #[error("cannot discard the log through {requested}: the snapshot only reaches {snapshot}")]
    CompactBeyondSnapshot { requested: u64, snapshot: u64 },
}

/// A snapshot found on disk at startup.
pub struct RecoveredSnapshot {
    pub metadata: SnapshotMetadata,
    pub state: KvStore,
}

/// Everything a node needs to resume after a restart: the open storage handle
/// plus what was recovered from disk.
pub struct Recovered {
    pub persistence: Persistence,
    pub hard_state: PersistentState,
    /// The snapshot the node starts from, if it has one. `entries` then holds only
    /// the entries AFTER it (index > `last_included_index`).
    pub snapshot: Option<RecoveredSnapshot>,
    pub entries: Vec<LogEntry>,
    /// Bytes of an unfinished write cut off the end of the WAL during recovery.
    /// Non-zero means the previous run died mid-append; nothing that write held
    /// was ever acknowledged, so nothing promised was lost.
    pub discarded_wal_bytes: u64,
}

/// Everything Raft requires to survive a crash, behind one handle: the log
/// (write-ahead log) and the hard state (`current_term` / `voted_for`).
///
/// Keeping both here means the node has a single place to ask "is this durable?",
/// and later changes to how storage works (batched fsyncs, snapshots, compaction)
/// touch this type instead of every call site in the node.
pub struct Persistence {
    dir: PathBuf,
    wal: Wal,
    hard_state_path: PathBuf,
    snapshot_path: PathBuf,
    /// Index the on-disk snapshot reaches (0 if there is none).
    snapshot_base: u64,
    /// What is currently on disk, so unchanged state never costs an fsync.
    last_saved: (Term, Option<NodeId>),
}

impl Persistence {
    /// Opens (creating if needed) the storage directory and recovers its contents.
    ///
    /// Recovery order: the snapshot first (it is the oldest state), then the WAL on
    /// top of it. WAL entries the snapshot already covers are dropped, and if there
    /// were any the WAL file is rewritten without them: that finishes a compaction
    /// the previous run was interrupted in (snapshot saved, WAL not yet trimmed).
    /// The entries that remain must continue exactly where the snapshot ends.
    pub fn open(dir: impl AsRef<Path>) -> Result<Recovered, PersistenceError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let wal_path = dir.join(WAL_FILE);
        let hard_state_path = dir.join(HARD_STATE_FILE);
        let snapshot_path = dir.join(SNAPSHOT_FILE);

        let snapshot = load_snapshot(&snapshot_path)?
            .map(|(metadata, state)| RecoveredSnapshot { metadata, state });
        let (base_index, base_term) = snapshot
            .as_ref()
            .map_or((0, 0), |s| (s.metadata.last_included_index, s.metadata.last_included_term));

        let WalRecovery { mut entries, discarded_bytes } = Wal::recover(&wal_path)?;
        let mut wal = Wal::open(&wal_path)?;
        let before = entries.len();
        entries.retain(|e| e.index > base_index);
        if entries.len() != before {
            wal.compact_through(base_index)?;
        }
        let mut expected = base_index + 1;
        for entry in &entries {
            if entry.index != expected {
                return Err(PersistenceError::LogGap { expected, found: entry.index });
            }
            expected += 1;
        }

        let mut hard_state = load_hard_state(&hard_state_path)?;
        let last_saved = (hard_state.current_term, hard_state.voted_for);

        // A node cannot hold an entry from term T without having seen term T.
        // If the log is ahead of the stored term, the hard state is older than
        // the log: this is data written by a version that did not persist terms.
        // Raise the term to match so the node never operates below its own log.
        // (The vote for that term is unknowable, so it is cleared; this only
        // matters for that one-time upgrade. The corrected state is written on
        // the node's next save, since it differs from `last_saved`.) The same holds
        // for the last entry a snapshot covers.
        let log_term = entries.last().map(|e| e.term).unwrap_or(base_term);
        if log_term > hard_state.current_term {
            hard_state.current_term = log_term;
            hard_state.voted_for = None;
        }

        Ok(Recovered {
            persistence: Persistence {
                dir,
                wal,
                hard_state_path,
                snapshot_path,
                snapshot_base: base_index,
                last_saved,
            },
            hard_state,
            snapshot,
            entries,
            discarded_wal_bytes: discarded_bytes,
        })
    }

    /// Makes a snapshot durable, THEN discards the log entries it covers.
    ///
    /// The order is the whole point. If the process dies after the snapshot is
    /// saved but before the WAL is trimmed, the next start finds redundant entries
    /// and drops them (see `open`); nothing is lost. Trimming first would, on a
    /// crash in between, lose entries that exist nowhere else. A snapshot older
    /// than the one already stored is refused.
    pub fn save_snapshot_and_compact(
        &mut self,
        metadata: SnapshotMetadata,
        state: &KvStore,
    ) -> Result<(), PersistenceError> {
        let index = metadata.last_included_index;
        self.save_snapshot_only(metadata, state)?;
        self.compact_log_through(index)
    }

    /// Makes a snapshot durable and leaves the log alone. Taking a snapshot and
    /// trimming the log are separate steps because a leader may have to keep log
    /// entries a slow follower still needs even though a snapshot already covers
    /// them; the trimming then waits (`compact_log_through`).
    pub fn save_snapshot_only(
        &mut self,
        metadata: SnapshotMetadata,
        state: &KvStore,
    ) -> Result<(), PersistenceError> {
        let index = metadata.last_included_index;
        if index < self.snapshot_base {
            return Err(PersistenceError::StaleSnapshot {
                requested: index,
                current: self.snapshot_base,
            });
        }
        save_snapshot(&self.snapshot_path, metadata, state)?;
        self.snapshot_base = index;
        Ok(())
    }

    /// Discards log entries up to and including `index`. Only entries the snapshot
    /// on disk already covers may go, so that every entry removed is still
    /// recoverable. A crash at any point is safe: the WAL is rewritten atomically,
    /// and `open` finishes or ignores whatever was in flight. Entries between the
    /// point trimmed here and the snapshot's own index are dropped the next time
    /// the node starts, since the snapshot makes them redundant.
    pub fn compact_log_through(&mut self, index: u64) -> Result<(), PersistenceError> {
        if index > self.snapshot_base {
            return Err(PersistenceError::CompactBeyondSnapshot {
                requested: index,
                snapshot: self.snapshot_base,
            });
        }
        self.wal.compact_through(index)?;
        Ok(())
    }

    /// The bytes of the snapshot file, for sending to a follower that needs it.
    pub fn read_snapshot_bytes(&self) -> Result<Option<Vec<u8>>, PersistenceError> {
        match std::fs::read(&self.snapshot_path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Adopts a snapshot received from the leader: its bytes become this node's
    /// snapshot file, and the log is trimmed to match.
    ///
    /// `keep_log_suffix` says whether the log already held the snapshot's last entry
    /// (same index and term), in which case the entries after it are still valid and
    /// are kept; otherwise the whole log is discarded (Raft paper §7). As everywhere,
    /// the snapshot is made durable FIRST. If the process dies before the log is
    /// trimmed, entries that are not part of the snapshot's history may remain after
    /// it, which is harmless: each (index, term) names one entry cluster-wide, so a
    /// stale entry conflicts with the leader's at the same index and the ordinary
    /// append path replaces it.
    pub fn install_snapshot_bytes(
        &mut self,
        bytes: &[u8],
        metadata: &SnapshotMetadata,
        keep_log_suffix: bool,
    ) -> Result<(), PersistenceError> {
        let index = metadata.last_included_index;
        if index < self.snapshot_base {
            return Err(PersistenceError::StaleSnapshot {
                requested: index,
                current: self.snapshot_base,
            });
        }
        write_snapshot_bytes(&self.snapshot_path, bytes)?;
        self.snapshot_base = index;
        self.wal.compact_through(if keep_log_suffix { index } else { u64::MAX })?;
        Ok(())
    }

    /// Index the snapshot on disk reaches (0 if there is none).
    pub fn snapshot_base(&self) -> u64 {
        self.snapshot_base
    }

    /// Appends one log entry and fsyncs it before returning.
    pub fn append(&mut self, entry: &LogEntry) -> Result<(), PersistenceError> {
        self.wal.append(entry)?;
        Ok(())
    }

    /// Appends several entries with a single fsync (group commit). Entries that
    /// arrive together share one disk sync instead of paying for one each; none of
    /// them may be acknowledged until this returns.
    pub fn append_batch(&mut self, entries: &[LogEntry]) -> Result<(), PersistenceError> {
        self.wal.append_batch(entries)?;
        Ok(())
    }

    /// How many times the log has been forced to disk. Observability, and how the
    /// tests check that a batch really shares a sync.
    pub fn wal_sync_count(&self) -> u64 {
        self.wal.sync_count()
    }

    /// Durably discards every log entry with index >= `from_index`.
    pub fn truncate_from(&mut self, from_index: u64) -> Result<(), PersistenceError> {
        self.wal.truncate_from(from_index)?;
        Ok(())
    }

    /// Persists `current_term` / `voted_for` if they differ from what is already
    /// on disk. Returns whether a write happened. Raft requires this to complete
    /// BEFORE the node replies to the RPC that changed the state; otherwise a
    /// crash could make the node forget a vote it already told someone about.
    pub fn save_hard_state_if_changed(
        &mut self,
        state: &PersistentState,
    ) -> Result<bool, PersistenceError> {
        let current = (state.current_term, state.voted_for);
        if current == self.last_saved {
            return Ok(false);
        }
        save_hard_state(&self.hard_state_path, state)?;
        self.last_saved = current;
        Ok(true)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            Self(std::env::temp_dir().join(format!("quorum-persist-{nanos}-{n}")))
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn entry(term: Term, index: u64, command: &str) -> LogEntry {
        LogEntry { term, index, command: command.as_bytes().to_vec() }
    }

    fn state(term: Term, voted_for: Option<NodeId>) -> PersistentState {
        PersistentState { current_term: term, voted_for }
    }

    #[test]
    fn opening_an_empty_directory_gives_a_fresh_node() {
        let dir = TestDir::new();
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(recovered.hard_state.current_term, 0);
        assert_eq!(recovered.hard_state.voted_for, None);
        assert!(recovered.entries.is_empty());
    }

    #[test]
    fn reopening_recovers_both_the_log_and_the_hard_state() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append(&entry(2, 1, "a")).unwrap();
            p.append(&entry(2, 2, "b")).unwrap();
            p.save_hard_state_if_changed(&state(3, Some(2))).unwrap();
        }
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(recovered.entries.len(), 2);
        assert_eq!(recovered.entries[1].command, b"b".to_vec());
        assert_eq!(recovered.hard_state.current_term, 3);
        assert_eq!(recovered.hard_state.voted_for, Some(2));
    }

    #[test]
    fn unchanged_hard_state_costs_no_write() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;

        assert!(p.save_hard_state_if_changed(&state(1, Some(1))).unwrap());

        // Remove the file: if the next call (same state) wrote again, it would reappear.
        std::fs::remove_file(dir.0.join(HARD_STATE_FILE)).unwrap();
        assert!(!p.save_hard_state_if_changed(&state(1, Some(1))).unwrap());
        assert!(!dir.0.join(HARD_STATE_FILE).exists());

        // A real change is written.
        assert!(p.save_hard_state_if_changed(&state(2, None)).unwrap());
        assert!(dir.0.join(HARD_STATE_FILE).exists());
    }

    #[test]
    fn a_vote_change_within_the_same_term_is_persisted() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        assert!(p.save_hard_state_if_changed(&state(5, None)).unwrap());
        assert!(p.save_hard_state_if_changed(&state(5, Some(4))).unwrap());
        drop(p);
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(recovered.hard_state.voted_for, Some(4));
    }

    #[test]
    fn truncation_survives_a_reopen() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            for i in 1..=4 {
                p.append(&entry(1, i, "x")).unwrap();
            }
            p.truncate_from(3).unwrap();
        }
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(recovered.entries.len(), 2);
        assert_eq!(recovered.entries.last().unwrap().index, 2);
    }

    #[test]
    fn a_term_older_than_the_log_is_raised_to_match_it() {
        let dir = TestDir::new();
        {
            // Simulates data from a version that wrote the log but never the term.
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append(&entry(4, 1, "a")).unwrap();
        }
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(recovered.hard_state.current_term, 4);
    }

    #[test]
    fn a_batch_survives_a_reopen_and_costs_one_sync() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            let batch: Vec<LogEntry> = (1..=20).map(|i| entry(1, i, "x")).collect();
            p.append_batch(&batch).unwrap();
            assert_eq!(p.wal_sync_count(), 1);
        }
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(recovered.entries.len(), 20);
        assert_eq!(recovered.discarded_wal_bytes, 0);
    }

    #[test]
    fn opening_after_a_crash_mid_append_recovers_the_complete_records() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append(&entry(1, 1, "a")).unwrap();
            p.append(&entry(1, 2, "b")).unwrap();
        }
        // The process died part-way through writing a third record.
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(dir.0.join(WAL_FILE))
            .unwrap()
            .write_all(&[40, 0, 0, 0, 1, 2, 3])
            .unwrap();

        let recovered = Persistence::open(&dir.0).unwrap();

        assert_eq!(recovered.entries.len(), 2, "the unfinished record is dropped");
        assert_eq!(recovered.discarded_wal_bytes, 7);
        // The node can carry on appending to a clean file.
        let mut p = recovered.persistence;
        p.append(&entry(1, 3, "c")).unwrap();
        drop(p);
        assert_eq!(Persistence::open(&dir.0).unwrap().entries.len(), 3);
    }

    // ---- snapshots and compaction -------------------------------------------

    use crate::kv_store::Command;
    use raft_core::Membership;

    fn meta(index: u64, term: Term) -> SnapshotMetadata {
        SnapshotMetadata {
            last_included_index: index,
            last_included_term: term,
            membership: Membership::new([(1, String::new()), (2, "node2:7000".to_string())]),
        }
    }

    fn store_with(key: &str, value: &str) -> KvStore {
        let mut store = KvStore::new();
        store.apply(&Command::Put { key: key.into(), value: value.as_bytes().to_vec() });
        store
    }

    fn indices(recovered: &Recovered) -> Vec<u64> {
        recovered.entries.iter().map(|e| e.index).collect()
    }

    #[test]
    fn a_node_with_no_snapshot_recovers_none() {
        let dir = TestDir::new();
        assert!(Persistence::open(&dir.0).unwrap().snapshot.is_none());
    }

    #[test]
    fn saving_a_snapshot_compacts_the_log_and_recovery_starts_from_it() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            for i in 1..=8 {
                p.append(&entry(2, i, &format!("c{i}"))).unwrap();
            }
            p.save_snapshot_and_compact(meta(5, 2), &store_with("k", "v5")).unwrap();
            assert_eq!(p.snapshot_base(), 5);
        }
        let recovered = Persistence::open(&dir.0).unwrap();
        let snapshot = recovered.snapshot.as_ref().expect("snapshot recovered");
        assert_eq!(snapshot.metadata.last_included_index, 5);
        assert_eq!(snapshot.state.get("k"), Some(&b"v5".to_vec()));
        assert_eq!(indices(&recovered), vec![6, 7, 8], "only what the snapshot does not cover");
        assert_eq!(Wal::replay(dir.0.join(WAL_FILE)).unwrap().len(), 3, "and the file shrank");
    }

    #[test]
    fn new_entries_continue_after_a_compacted_log() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append_batch(&(1..=4).map(|i| entry(1, i, "x")).collect::<Vec<_>>()).unwrap();
            p.save_snapshot_and_compact(meta(4, 1), &KvStore::new()).unwrap();
            p.append(&entry(1, 5, "y")).unwrap();
        }
        assert_eq!(indices(&Persistence::open(&dir.0).unwrap()), vec![5]);
    }

    #[test]
    fn a_crash_between_saving_the_snapshot_and_trimming_the_log_loses_nothing() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            for i in 1..=6 {
                p.append(&entry(1, i, "x")).unwrap();
            }
        }
        // The snapshot reached the disk; the process died before the WAL was trimmed.
        save_snapshot(dir.0.join(SNAPSHOT_FILE), meta(4, 1), &store_with("a", "b")).unwrap();
        assert_eq!(Wal::replay(dir.0.join(WAL_FILE)).unwrap().len(), 6);

        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(indices(&recovered), vec![5, 6], "redundant entries are dropped");
        assert!(recovered.snapshot.is_some());
        assert_eq!(
            Wal::replay(dir.0.join(WAL_FILE)).unwrap().len(),
            2,
            "and the interrupted compaction is finished on disk"
        );
    }

    #[test]
    fn a_snapshot_ahead_of_the_whole_log_leaves_an_empty_log() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append(&entry(1, 1, "x")).unwrap();
        }
        save_snapshot(dir.0.join(SNAPSHOT_FILE), meta(10, 3), &KvStore::new()).unwrap();
        let recovered = Persistence::open(&dir.0).unwrap();
        assert!(recovered.entries.is_empty());
        assert_eq!(recovered.snapshot.unwrap().metadata.last_included_index, 10);
    }

    #[test]
    fn a_log_that_does_not_continue_from_the_snapshot_is_refused() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append(&entry(1, 8, "x")).unwrap(); // 6 and 7 are missing
        }
        save_snapshot(dir.0.join(SNAPSHOT_FILE), meta(5, 1), &KvStore::new()).unwrap();
        match Persistence::open(&dir.0) {
            Err(PersistenceError::LogGap { expected: 6, found: 8 }) => {}
            other => panic!("expected a gap error, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn a_log_that_does_not_start_at_one_without_a_snapshot_is_refused() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append(&entry(1, 3, "x")).unwrap();
        }
        assert!(matches!(
            Persistence::open(&dir.0),
            Err(PersistenceError::LogGap { expected: 1, found: 3 })
        ));
    }

    #[test]
    fn a_damaged_snapshot_stops_startup_instead_of_starting_empty() {
        let dir = TestDir::new();
        std::fs::create_dir_all(&dir.0).unwrap();
        save_snapshot(dir.0.join(SNAPSHOT_FILE), meta(4, 1), &store_with("a", "b")).unwrap();
        let path = dir.0.join(SNAPSHOT_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            Persistence::open(&dir.0),
            Err(PersistenceError::Snapshot(SnapshotError::ChecksumMismatch))
        ));
    }

    #[test]
    fn an_older_snapshot_than_the_stored_one_is_refused_and_changes_nothing() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        p.append_batch(&(1..=6).map(|i| entry(1, i, "x")).collect::<Vec<_>>()).unwrap();
        p.save_snapshot_and_compact(meta(5, 1), &store_with("new", "1")).unwrap();
        let error = p.save_snapshot_and_compact(meta(3, 1), &store_with("old", "1")).unwrap_err();
        assert!(matches!(error, PersistenceError::StaleSnapshot { requested: 3, current: 5 }));
        assert_eq!(p.snapshot_base(), 5);
        let (stored, state) = load_snapshot(dir.0.join(SNAPSHOT_FILE)).unwrap().unwrap();
        assert_eq!(stored.last_included_index, 5);
        assert!(state.get("new").is_some());
    }

    #[test]
    fn the_term_is_raised_to_the_snapshots_when_the_log_after_it_is_empty() {
        let dir = TestDir::new();
        std::fs::create_dir_all(&dir.0).unwrap();
        save_snapshot(dir.0.join(SNAPSHOT_FILE), meta(9, 7), &KvStore::new()).unwrap();
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(recovered.hard_state.current_term, 7);
        assert_eq!(recovered.hard_state.voted_for, None);
    }

    #[test]
    fn truncating_the_log_after_a_snapshot_still_works_across_a_reopen() {
        let dir = TestDir::new();
        {
            let mut p = Persistence::open(&dir.0).unwrap().persistence;
            p.append_batch(&(1..=8).map(|i| entry(1, i, "x")).collect::<Vec<_>>()).unwrap();
            p.save_snapshot_and_compact(meta(3, 1), &KvStore::new()).unwrap();
            p.truncate_from(6).unwrap();
        }
        assert_eq!(indices(&Persistence::open(&dir.0).unwrap()), vec![4, 5]);
    }

    #[test]
    fn a_snapshot_can_be_saved_without_trimming_the_log_and_trimmed_later() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        p.append_batch(&(1..=8).map(|i| entry(1, i, "x")).collect::<Vec<_>>()).unwrap();
        p.save_snapshot_only(meta(6, 1), &store_with("k", "v")).unwrap();
        assert_eq!(p.snapshot_base(), 6);
        assert_eq!(Wal::replay(dir.0.join(WAL_FILE)).unwrap().len(), 8, "the log is untouched");

        p.compact_log_through(4).unwrap(); // a slow follower still needs 5 onward
        let kept: Vec<u64> =
            Wal::replay(dir.0.join(WAL_FILE)).unwrap().iter().map(|e| e.index).collect();
        assert_eq!(kept, vec![5, 6, 7, 8]);
        drop(p);

        // On restart the snapshot makes 5 and 6 redundant: the log continues after it.
        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(indices(&recovered), vec![7, 8]);
        assert_eq!(recovered.snapshot.unwrap().metadata.last_included_index, 6);
    }

    #[test]
    fn the_log_cannot_be_trimmed_past_what_a_snapshot_covers() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        p.append_batch(&(1..=8).map(|i| entry(1, i, "x")).collect::<Vec<_>>()).unwrap();
        assert!(matches!(
            p.compact_log_through(1),
            Err(PersistenceError::CompactBeyondSnapshot { requested: 1, snapshot: 0 })
        ));
        p.save_snapshot_only(meta(5, 1), &KvStore::new()).unwrap();
        assert!(matches!(
            p.compact_log_through(6),
            Err(PersistenceError::CompactBeyondSnapshot { requested: 6, snapshot: 5 })
        ));
        assert_eq!(Wal::replay(dir.0.join(WAL_FILE)).unwrap().len(), 8, "nothing was lost");
    }

    #[test]
    fn the_snapshot_file_can_be_read_back_for_sending() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        assert!(p.read_snapshot_bytes().unwrap().is_none());
        p.save_snapshot_only(meta(4, 1), &store_with("k", "v")).unwrap();
        let bytes = p.read_snapshot_bytes().unwrap().expect("a snapshot exists");
        let (decoded, state) = crate::snapshot::decode_snapshot(&bytes).unwrap();
        assert_eq!(decoded.last_included_index, 4);
        assert_eq!(state.get("k"), Some(&b"v".to_vec()));
    }

    fn encoded(index: u64, key: &str) -> (SnapshotMetadata, Vec<u8>) {
        let metadata = meta(index, 1);
        let bytes = crate::snapshot::encode_snapshot(&metadata, &store_with(key, "v")).unwrap();
        (metadata, bytes)
    }

    #[test]
    fn installing_a_received_snapshot_can_keep_the_log_after_it() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        p.append_batch(&(1..=9).map(|i| entry(1, i, "x")).collect::<Vec<_>>()).unwrap();
        let (metadata, bytes) = encoded(6, "from-leader");
        p.install_snapshot_bytes(&bytes, &metadata, true).unwrap();
        assert_eq!(p.snapshot_base(), 6);
        drop(p);

        let recovered = Persistence::open(&dir.0).unwrap();
        assert_eq!(indices(&recovered), vec![7, 8, 9]);
        let snapshot = recovered.snapshot.unwrap();
        assert_eq!(snapshot.metadata.last_included_index, 6);
        assert!(snapshot.state.get("from-leader").is_some());
    }

    #[test]
    fn installing_a_received_snapshot_can_discard_the_whole_log() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        p.append_batch(&(1..=9).map(|i| entry(1, i, "x")).collect::<Vec<_>>()).unwrap();
        let (metadata, bytes) = encoded(4, "from-leader");
        p.install_snapshot_bytes(&bytes, &metadata, false).unwrap();
        assert!(Wal::replay(dir.0.join(WAL_FILE)).unwrap().is_empty(), "nothing is left");
        drop(p);
        assert!(indices(&Persistence::open(&dir.0).unwrap()).is_empty());
    }

    #[test]
    fn installing_an_older_snapshot_than_the_stored_one_is_refused() {
        let dir = TestDir::new();
        let mut p = Persistence::open(&dir.0).unwrap().persistence;
        p.save_snapshot_only(meta(8, 1), &KvStore::new()).unwrap();
        let (metadata, bytes) = encoded(5, "old");
        assert!(matches!(
            p.install_snapshot_bytes(&bytes, &metadata, true),
            Err(PersistenceError::StaleSnapshot { requested: 5, current: 8 })
        ));
    }
}

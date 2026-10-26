use crate::hard_state::{load_hard_state, save_hard_state, HardStateError};
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
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Everything a node needs to resume after a restart: the open storage handle
/// plus what was recovered from disk.
pub struct Recovered {
    pub persistence: Persistence,
    pub hard_state: PersistentState,
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
    /// What is currently on disk, so unchanged state never costs an fsync.
    last_saved: (Term, Option<NodeId>),
}

impl Persistence {
    /// Opens (creating if needed) the storage directory and recovers its contents.
    pub fn open(dir: impl AsRef<Path>) -> Result<Recovered, PersistenceError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let wal_path = dir.join(WAL_FILE);
        let hard_state_path = dir.join(HARD_STATE_FILE);

        let WalRecovery { entries, discarded_bytes } = Wal::recover(&wal_path)?;
        let wal = Wal::open(&wal_path)?;
        let mut hard_state = load_hard_state(&hard_state_path)?;
        let last_saved = (hard_state.current_term, hard_state.voted_for);

        // A node cannot hold an entry from term T without having seen term T.
        // If the log is ahead of the stored term, the hard state is older than
        // the log: this is data written by a version that did not persist terms.
        // Raise the term to match so the node never operates below its own log.
        // (The vote for that term is unknowable, so it is cleared; this only
        // matters for that one-time upgrade. The corrected state is written on
        // the node's next save, since it differs from `last_saved`.)
        let log_term = entries.last().map(|e| e.term).unwrap_or(0);
        if log_term > hard_state.current_term {
            hard_state.current_term = log_term;
            hard_state.voted_for = None;
        }

        Ok(Recovered {
            persistence: Persistence { dir, wal, hard_state_path, last_saved },
            hard_state,
            entries,
            discarded_wal_bytes: discarded_bytes,
        })
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
}
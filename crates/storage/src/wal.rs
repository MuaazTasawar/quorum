use raft_core::LogEntry;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode/decode error: {0}")]
    Codec(#[from] bincode::Error),
    #[error("corrupt WAL: unexpected end of file mid-record")]
    Truncated,
}

/// What `Wal::recover` found on disk.
#[derive(Debug)]
pub struct WalRecovery {
    /// Every complete record, in order.
    pub entries: Vec<LogEntry>,
    /// Bytes of an incomplete record at the very end of the file that were cut
    /// off. Non-zero means the process died in the middle of a write that was
    /// never acknowledged (see `Wal::recover`).
    pub discarded_bytes: u64,
}

/// Write-ahead log: every log entry is durably fsync'd here BEFORE a leader
/// counts it toward a majority, and before a follower acknowledges AppendEntries.
/// This is the mechanism that makes Raft's "committed entries are never lost"
/// guarantee actually true on real disks, not just in the in-memory model.
pub struct Wal {
    file: File,
    path: PathBuf,
    /// How many times this handle has forced data to disk. One batch of entries
    /// costs one sync however many entries it holds; the count is how that is
    /// observed and tested.
    sync_count: u64,
}

/// On-disk record framing: [4-byte little-endian length][bincode-encoded LogEntry].
/// Length-prefixing lets replay() know exactly where one record ends and the next
/// begins, without needing delimiters that could collide with binary data.
fn encode_record(entry: &LogEntry, out: &mut Vec<u8>) -> Result<(), WalError> {
    let encoded = bincode::serialize(entry)?;
    out.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    out.extend_from_slice(&encoded);
    Ok(())
}

/// Parses as many complete records as the bytes hold. Returns them together with
/// the number of bytes they occupy; anything after that is an incomplete record at
/// the end of the data (a torn write).
///
/// Only an INCOMPLETE final record counts as torn. A record that is complete but
/// does not decode is real corruption and is returned as an error, never skipped.
fn parse_records(bytes: &[u8]) -> Result<(Vec<LogEntry>, usize), WalError> {
    let mut entries = Vec::new();
    let mut offset = 0usize;

    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        if remaining.len() < 4 {
            break; // not even a whole length prefix
        }
        let len = u32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]]) as usize;
        if remaining.len() - 4 < len {
            break; // the payload runs past the end of the file
        }
        entries.push(bincode::deserialize(&remaining[4..4 + len])?);
        offset += 4 + len;
    }
    Ok((entries, offset))
}

impl Wal {
    /// Opens (creating if absent) the WAL file in append mode.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self { file, path, sync_count: 0 })
    }

    /// Appends one entry and fsyncs before returning. The fsync is the whole
    /// point - without it, entries could be "committed" from Raft's perspective
    /// but live only in the OS page cache, and a power loss would silently lose
    /// data a client was told was safely replicated.
    pub fn append(&mut self, entry: &LogEntry) -> Result<(), WalError> {
        self.append_batch(std::slice::from_ref(entry))
    }

    /// Appends several entries with ONE write and ONE fsync (group commit).
    ///
    /// The fsync dominates the cost of a durable write, and it costs the same
    /// whether it covers one record or a hundred, so entries that arrive together
    /// should share it. The guarantee is unchanged: nothing in the batch is
    /// acknowledged until this returns, and by then all of it is on disk. If the
    /// process dies part-way through, the unfinished tail is dropped on recovery
    /// (see `recover`), which is safe because none of it was ever acknowledged.
    pub fn append_batch(&mut self, entries: &[LogEntry]) -> Result<(), WalError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buffer = Vec::new();
        for entry in entries {
            encode_record(entry, &mut buffer)?;
        }
        self.file.write_all(&buffer)?;
        self.file.sync_all()?;
        self.sync_count += 1;
        Ok(())
    }

    /// Number of fsyncs this handle has performed.
    pub fn sync_count(&self) -> u64 {
        self.sync_count
    }

    /// Reads every record from the start of the file, in order, reconstructing
    /// the log as it stood at the last successful write. Strict: an incomplete
    /// record at the end is an error (`Truncated`). Use `recover` at startup.
    pub fn replay(path: impl AsRef<Path>) -> Result<Vec<LogEntry>, WalError> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = std::fs::read(path)?;
        let (entries, valid_len) = parse_records(&bytes)?;
        if valid_len < bytes.len() {
            return Err(WalError::Truncated);
        }
        Ok(entries)
    }

    /// Startup recovery: like `replay`, but a torn write at the END of the file is
    /// repaired instead of being fatal.
    ///
    /// If the process was killed in the middle of appending, the file can end with
    /// part of a record. That write never finished its fsync, so no client was told
    /// it succeeded and no peer was told it was stored; dropping it loses nothing
    /// that was ever promised. Refusing to start instead would turn an ordinary
    /// crash into an outage. The partial tail is cut off the file and the number of
    /// bytes removed is reported.
    ///
    /// This applies only to an incomplete final record. A complete record that
    /// fails to decode is corruption and still fails recovery.
    pub fn recover(path: impl AsRef<Path>) -> Result<WalRecovery, WalError> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(WalRecovery { entries: Vec::new(), discarded_bytes: 0 });
        }
        let bytes = std::fs::read(path)?;
        let (entries, valid_len) = parse_records(&bytes)?;
        let discarded_bytes = (bytes.len() - valid_len) as u64;

        if discarded_bytes > 0 {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(valid_len as u64)?;
            file.sync_all()?;
        }
        Ok(WalRecovery { entries, discarded_bytes })
    }

    /// Rewrites the WAL keeping only entries with index < `from_index`, used when
    /// a follower must discard conflicting entries after AppendEntries detects a
    /// log mismatch. WAL is append-only on disk, so "truncating" means: read
    /// everything back, filter, write to a temp file, then atomically rename over
    /// the original. The rename is atomic on the same filesystem, so a crash
    /// mid-rewrite leaves either the old file or the new one intact - never a
    /// half-written, corrupt WAL.
    pub fn truncate_from(&mut self, from_index: u64) -> Result<(), WalError> {
        let kept: Vec<LogEntry> = Self::replay(&self.path)?
            .into_iter()
            .filter(|e| e.index < from_index)
            .collect();

        let tmp_path = self.path.with_extension("wal.tmp");
        {
            let mut tmp_file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            let mut buffer = Vec::new();
            for entry in &kept {
                encode_record(entry, &mut buffer)?;
            }
            tmp_file.write_all(&buffer)?;
            tmp_file.sync_all()?;
            self.sync_count += 1;
        }
        std::fs::rename(&tmp_path, &self.path)?;

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;
        Ok(())
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
            let dir = std::env::temp_dir().join(format!("quorum-wal-{nanos}-{n}"));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn wal(&self) -> PathBuf {
            self.0.join("wal.log")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn entry(term: u64, index: u64, command: &str) -> LogEntry {
        LogEntry { term, index, command: command.as_bytes().to_vec() }
    }

    fn entries(count: u64) -> Vec<LogEntry> {
        (1..=count).map(|i| entry(1, i, &format!("cmd-{i}"))).collect()
    }

    #[test]
    fn a_batch_round_trips_in_order() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(5)).unwrap();

        let replayed = Wal::replay(dir.wal()).unwrap();
        assert_eq!(replayed.len(), 5);
        assert_eq!(replayed[4].command, b"cmd-5".to_vec());
        assert_eq!(replayed[0].index, 1);
    }

    #[test]
    fn a_batch_costs_one_sync_however_many_entries_it_holds() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(100)).unwrap();
        assert_eq!(wal.sync_count(), 1, "100 entries must share a single fsync");
    }

    #[test]
    fn appending_one_at_a_time_costs_a_sync_each() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        for e in entries(5) {
            wal.append(&e).unwrap();
        }
        assert_eq!(wal.sync_count(), 5);
    }

    #[test]
    fn an_empty_batch_writes_and_syncs_nothing() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&[]).unwrap();
        assert_eq!(wal.sync_count(), 0);
        assert!(Wal::replay(dir.wal()).unwrap().is_empty());
    }

    #[test]
    fn batched_and_single_appends_produce_identical_files() {
        let single = TestDir::new();
        let batched = TestDir::new();
        let mut a = Wal::open(single.wal()).unwrap();
        for e in entries(6) {
            a.append(&e).unwrap();
        }
        let mut b = Wal::open(batched.wal()).unwrap();
        b.append_batch(&entries(6)).unwrap();

        assert_eq!(std::fs::read(single.wal()).unwrap(), std::fs::read(batched.wal()).unwrap());
    }

    #[test]
    fn strict_replay_still_rejects_an_incomplete_final_record() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(2)).unwrap();
        let mut file = OpenOptions::new().append(true).open(dir.wal()).unwrap();
        file.write_all(&[9, 0]).unwrap(); // two stray bytes: half a length prefix

        assert!(matches!(Wal::replay(dir.wal()), Err(WalError::Truncated)));
    }

    #[test]
    fn recovery_drops_half_a_length_prefix_and_cuts_it_off_the_file() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(3)).unwrap();
        let good_len = std::fs::metadata(dir.wal()).unwrap().len();
        OpenOptions::new().append(true).open(dir.wal()).unwrap().write_all(&[9, 0]).unwrap();

        let recovery = Wal::recover(dir.wal()).unwrap();

        assert_eq!(recovery.entries.len(), 3);
        assert_eq!(recovery.discarded_bytes, 2);
        assert_eq!(std::fs::metadata(dir.wal()).unwrap().len(), good_len, "file must be cut back");
        // And it is now a clean WAL again.
        assert_eq!(Wal::replay(dir.wal()).unwrap().len(), 3);
    }

    #[test]
    fn recovery_drops_a_record_whose_payload_was_cut_short() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(2)).unwrap();
        let good_len = std::fs::metadata(dir.wal()).unwrap().len();

        // A third record whose length prefix promises 50 bytes but only 10 arrived.
        let mut torn = Vec::new();
        torn.extend_from_slice(&50u32.to_le_bytes());
        torn.extend_from_slice(&[7u8; 10]);
        OpenOptions::new().append(true).open(dir.wal()).unwrap().write_all(&torn).unwrap();

        let recovery = Wal::recover(dir.wal()).unwrap();

        assert_eq!(recovery.entries.len(), 2);
        assert_eq!(recovery.discarded_bytes, 14);
        assert_eq!(std::fs::metadata(dir.wal()).unwrap().len(), good_len);
    }

    #[test]
    fn recovery_of_a_clean_wal_changes_nothing() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(4)).unwrap();
        let before = std::fs::read(dir.wal()).unwrap();

        let recovery = Wal::recover(dir.wal()).unwrap();

        assert_eq!(recovery.entries.len(), 4);
        assert_eq!(recovery.discarded_bytes, 0);
        assert_eq!(std::fs::read(dir.wal()).unwrap(), before);
    }

    #[test]
    fn a_missing_wal_recovers_as_empty() {
        let dir = TestDir::new();
        let recovery = Wal::recover(dir.wal()).unwrap();
        assert!(recovery.entries.is_empty());
        assert_eq!(recovery.discarded_bytes, 0);
    }

    #[test]
    fn a_complete_record_that_does_not_decode_is_corruption_not_a_torn_write() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(1)).unwrap();

        // A complete 3-byte record in the middle, followed by a valid record.
        let mut bytes = std::fs::read(dir.wal()).unwrap();
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&[1, 2, 3]);
        let mut tail = Vec::new();
        encode_record(&entry(1, 2, "after"), &mut tail).unwrap();
        bytes.extend_from_slice(&tail);
        std::fs::write(dir.wal(), &bytes).unwrap();

        assert!(
            Wal::recover(dir.wal()).is_err(),
            "silently skipping a corrupt record would lose acknowledged data"
        );
        assert_eq!(std::fs::read(dir.wal()).unwrap(), bytes, "and the file must be left alone");
    }

    #[test]
    fn appending_after_recovery_extends_the_clean_log() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(2)).unwrap();
        OpenOptions::new().append(true).open(dir.wal()).unwrap().write_all(&[1, 2, 3]).unwrap();
        drop(wal);

        Wal::recover(dir.wal()).unwrap();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append(&entry(1, 3, "next")).unwrap();

        let replayed = Wal::replay(dir.wal()).unwrap();
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[2].command, b"next".to_vec());
    }

    #[test]
    fn truncation_keeps_only_earlier_entries_and_counts_its_sync() {
        let dir = TestDir::new();
        let mut wal = Wal::open(dir.wal()).unwrap();
        wal.append_batch(&entries(5)).unwrap();
        wal.truncate_from(3).unwrap();

        let replayed = Wal::replay(dir.wal()).unwrap();
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[1].index, 2);
        assert_eq!(wal.sync_count(), 2, "one for the batch, one for the rewritten file");
    }
}
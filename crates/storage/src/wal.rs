use raft_core::LogEntry;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
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

/// Write-ahead log: every log entry is durably fsync'd here BEFORE a leader
/// counts it toward a majority, and before a follower acknowledges AppendEntries.
/// This is the mechanism that makes Raft's "committed entries are never lost"
/// guarantee actually true on real disks, not just in the in-memory model.
pub struct Wal {
    file: File,
    path: PathBuf,
}

/// On-disk record framing: [4-byte little-endian length][bincode-encoded LogEntry].
/// Length-prefixing lets replay() know exactly where one record ends and the next
/// begins, without needing delimiters that could collide with binary data.
fn write_record(file: &mut File, entry: &LogEntry) -> Result<(), WalError> {
    let encoded = bincode::serialize(entry)?;
    let len = (encoded.len() as u32).to_le_bytes();
    file.write_all(&len)?;
    file.write_all(&encoded)?;
    Ok(())
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
        Ok(Self { file, path })
    }

    /// Appends one entry and fsyncs before returning. The fsync is the whole
    /// point - without it, entries could be "committed" from Raft's perspective
    /// but live only in the OS page cache, and a power loss would silently lose
    /// data a client was told was safely replicated.
    pub fn append(&mut self, entry: &LogEntry) -> Result<(), WalError> {
        write_record(&mut self.file, entry)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Reads every record from the start of the file, in order, reconstructing
    /// the log as it stood at the last successful write. Called once at node
    /// startup before the node accepts any RPCs.
    pub fn replay(path: impl AsRef<Path>) -> Result<Vec<LogEntry>, WalError> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(WalError::Io(e)),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    WalError::Truncated
                } else {
                    WalError::Io(e)
                }
            })?;
            entries.push(bincode::deserialize(&buf)?);
        }
        Ok(entries)
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
            for entry in &kept {
                write_record(&mut tmp_file, entry)?;
            }
            tmp_file.sync_all()?;
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
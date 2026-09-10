use crate::kv_store::KvStore;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// Marks the point up to which a snapshot has captured the state machine.
/// Once a snapshot with `last_included_index = N` exists, every WAL entry with
/// index <= N is redundant (its effect is already folded into the snapshot) and
/// can be safely discarded via `Wal::truncate_from` - this is Raft's log
/// compaction (§7 in the paper), preventing the WAL from growing forever on a
/// long-running cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotFile {
    metadata: SnapshotMetadata,
    state: KvStore,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode/decode error: {0}")]
    Codec(#[from] bincode::Error),
}

/// Writes a snapshot atomically: serialize to a temp file, fsync, then rename
/// over the real snapshot path. Same crash-safety reasoning as `Wal::truncate_from` -
/// a reader must never observe a half-written snapshot.
pub fn save_snapshot(
    path: impl AsRef<Path>,
    metadata: SnapshotMetadata,
    state: &KvStore,
) -> Result<(), SnapshotError> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("snap.tmp");

    let payload = SnapshotFile { metadata, state: state.clone() };
    let encoded = bincode::serialize(&payload)?;

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Loads the most recent snapshot, if one exists. Called at startup, before WAL
/// replay: the node restores the snapshot first, then replays only the WAL
/// entries *after* `last_included_index` on top of it, rather than replaying
/// the entire history from scratch every restart.
pub fn load_snapshot(
    path: impl AsRef<Path>,
) -> Result<Option<(SnapshotMetadata, KvStore)>, SnapshotError> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    let payload: SnapshotFile = bincode::deserialize(&bytes)?;
    Ok(Some((payload.metadata, payload.state)))
}
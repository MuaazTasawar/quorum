use crate::kv_store::KvStore;
use raft_core::Membership;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// Name of the snapshot file inside a node's storage directory.
pub const SNAPSHOT_FILE: &str = "snapshot.snap";

/// Identifies a file as a snapshot, so a different file (or one from some older,
/// unframed format) is refused outright instead of being fed to the decoder.
const MAGIC: [u8; 4] = *b"QSNP";

/// The snapshot file format this build writes and reads. A file written by a newer
/// version is refused with a clear error instead of being misread: a node that
/// silently misinterprets its own snapshot would corrupt every answer it gives.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// magic (4) + version (4) + payload length (8) + payload CRC-32 (4).
const HEADER_LEN: usize = 4 + 4 + 8 + 4;

/// Marks the point up to which a snapshot has captured the state machine, and
/// everything else a node needs in order to carry on without the entries it covers.
/// Once a snapshot with `last_included_index = N` exists, every WAL entry with
/// index <= N is redundant (its effect is already folded into the snapshot) and
/// can be discarded: this is Raft's log compaction (§7 in the paper), preventing
/// the WAL from growing forever on a long-running cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    pub last_included_index: u64,
    pub last_included_term: u64,
    /// The cluster's voting members as of `last_included_index`, with addresses.
    /// Membership lives in the log, so discarding the log would discard it too; the
    /// snapshot carries it forward. After a restart this, not the environment, is
    /// the starting point that later configuration entries build on.
    pub membership: Membership,
}

#[derive(Serialize)]
struct SnapshotPayloadRef<'a> {
    metadata: &'a SnapshotMetadata,
    state: &'a KvStore,
}

#[derive(Deserialize)]
struct SnapshotPayload {
    metadata: SnapshotMetadata,
    state: KvStore,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode/decode error: {0}")]
    Codec(#[from] bincode::Error),
    #[error("not a snapshot file (bad magic bytes)")]
    BadMagic,
    #[error(
        "snapshot format version {found} is not supported by this build \
         (it reads version {supported}); it was probably written by a newer release"
    )]
    UnsupportedVersion { found: u32, supported: u32 },
    #[error("snapshot file is incomplete: expected {expected} bytes of payload, found {found}")]
    Truncated { expected: u64, found: u64 },
    #[error("snapshot file is corrupt: checksum mismatch")]
    ChecksumMismatch,
}

/// CRC-32 (IEEE 802.3), bit by bit. Snapshots are written rarely and read once at
/// startup, so a table or a dependency would buy nothing.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

/// Writes a snapshot atomically: serialize to a temp file, fsync, then rename
/// over the real snapshot path. Same crash-safety reasoning as `Wal::truncate_from` -
/// a reader must never observe a half-written snapshot. The file carries a magic
/// number, a format version, the payload length and a checksum, so damage is
/// detected on load instead of being decoded as if it were state.
pub fn save_snapshot(
    path: impl AsRef<Path>,
    metadata: SnapshotMetadata,
    state: &KvStore,
) -> Result<(), SnapshotError> {
    let path = path.as_ref();
    let tmp_path = path.with_extension("snap.tmp");

    let payload = bincode::serialize(&SnapshotPayloadRef { metadata: &metadata, state })?;
    let mut encoded = Vec::with_capacity(HEADER_LEN + payload.len());
    encoded.extend_from_slice(&MAGIC);
    encoded.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    encoded.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    encoded.extend_from_slice(&crc32(&payload).to_le_bytes());
    encoded.extend_from_slice(&payload);

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
///
/// A snapshot that is damaged, from an unsupported version, or not a snapshot at
/// all is an ERROR, never "no snapshot": the log before it may already have been
/// discarded, so quietly starting empty would lose acknowledged data.
pub fn load_snapshot(
    path: impl AsRef<Path>,
) -> Result<Option<(SnapshotMetadata, KvStore)>, SnapshotError> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)?;
    if bytes.len() < HEADER_LEN || bytes[..4] != MAGIC {
        return Err(SnapshotError::BadMagic);
    }
    let version = u32::from_le_bytes(bytes[4..8].try_into().expect("4 bytes"));
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedVersion {
            found: version,
            supported: SNAPSHOT_FORMAT_VERSION,
        });
    }
    let expected_len = u64::from_le_bytes(bytes[8..16].try_into().expect("8 bytes"));
    let expected_crc = u32::from_le_bytes(bytes[16..20].try_into().expect("4 bytes"));
    let payload = &bytes[HEADER_LEN..];
    if payload.len() as u64 != expected_len {
        return Err(SnapshotError::Truncated { expected: expected_len, found: payload.len() as u64 });
    }
    if crc32(payload) != expected_crc {
        return Err(SnapshotError::ChecksumMismatch);
    }
    let decoded: SnapshotPayload = bincode::deserialize(payload)?;
    Ok(Some((decoded.metadata, decoded.state)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_store::Command;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::SeqCst);
            let dir = std::env::temp_dir().join(format!("quorum-snap-{nanos}-{n}"));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn path(&self) -> PathBuf {
            self.0.join(SNAPSHOT_FILE)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn metadata(index: u64, term: u64) -> SnapshotMetadata {
        SnapshotMetadata {
            last_included_index: index,
            last_included_term: term,
            membership: Membership::new([
                (1, "node1:7000".to_string()),
                (2, "node2:7000".to_string()),
                (4, "node4:7000".to_string()),
            ]),
        }
    }

    fn state() -> KvStore {
        let mut store = KvStore::new();
        store.apply(&Command::Put { key: "alpha".into(), value: b"1".to_vec() });
        // A value that is not valid UTF-8 must survive untouched.
        store.apply(&Command::Put { key: "bin".into(), value: vec![0, 159, 146, 150, 255] });
        store
    }

    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[test]
    fn a_snapshot_round_trips_its_position_membership_and_state() {
        let dir = TestDir::new();
        save_snapshot(dir.path(), metadata(42, 3), &state()).unwrap();

        let (loaded, store) = load_snapshot(dir.path()).unwrap().expect("a snapshot exists");
        assert_eq!(loaded, metadata(42, 3));
        assert_eq!(store.len(), 2);
        assert_eq!(store.get("alpha"), Some(&b"1".to_vec()));
        assert_eq!(store.get("bin"), Some(&vec![0, 159, 146, 150, 255]));
    }

    #[test]
    fn no_file_means_no_snapshot() {
        let dir = TestDir::new();
        assert!(load_snapshot(dir.path()).unwrap().is_none());
    }

    #[test]
    fn a_later_snapshot_replaces_the_earlier_one_and_leaves_no_temp_file() {
        let dir = TestDir::new();
        save_snapshot(dir.path(), metadata(10, 1), &state()).unwrap();
        save_snapshot(dir.path(), metadata(20, 2), &KvStore::new()).unwrap();
        let (loaded, store) = load_snapshot(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.last_included_index, 20);
        assert!(store.is_empty());
        assert!(!dir.path().with_extension("snap.tmp").exists());
    }

    #[test]
    fn the_file_starts_with_the_magic_and_the_version() {
        let dir = TestDir::new();
        save_snapshot(dir.path(), metadata(1, 1), &state()).unwrap();
        let bytes = std::fs::read(dir.path()).unwrap();
        assert_eq!(&bytes[..4], b"QSNP");
        assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), SNAPSHOT_FORMAT_VERSION);
    }

    #[test]
    fn a_snapshot_from_a_newer_version_is_refused_with_a_clear_error() {
        let dir = TestDir::new();
        save_snapshot(dir.path(), metadata(1, 1), &state()).unwrap();
        let mut bytes = std::fs::read(dir.path()).unwrap();
        bytes[4..8].copy_from_slice(&(SNAPSHOT_FORMAT_VERSION + 1).to_le_bytes());
        std::fs::write(dir.path(), &bytes).unwrap();

        let error = load_snapshot(dir.path()).unwrap_err();
        assert!(
            matches!(error, SnapshotError::UnsupportedVersion { found, .. } if found == SNAPSHOT_FORMAT_VERSION + 1),
            "{error}"
        );
        assert!(error.to_string().contains("newer release"));
    }

    #[test]
    fn a_file_that_is_not_a_snapshot_is_refused() {
        let dir = TestDir::new();
        std::fs::write(dir.path(), b"definitely not a snapshot, but longer than a header").unwrap();
        assert!(matches!(load_snapshot(dir.path()), Err(SnapshotError::BadMagic)));
        std::fs::write(dir.path(), b"tiny").unwrap();
        assert!(matches!(load_snapshot(dir.path()), Err(SnapshotError::BadMagic)));
    }

    #[test]
    fn a_flipped_bit_in_the_payload_is_caught_by_the_checksum() {
        let dir = TestDir::new();
        save_snapshot(dir.path(), metadata(7, 1), &state()).unwrap();
        let mut bytes = std::fs::read(dir.path()).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(dir.path(), &bytes).unwrap();
        assert!(matches!(load_snapshot(dir.path()), Err(SnapshotError::ChecksumMismatch)));
    }

    #[test]
    fn a_file_cut_short_is_reported_as_truncated_not_decoded() {
        let dir = TestDir::new();
        save_snapshot(dir.path(), metadata(7, 1), &state()).unwrap();
        let bytes = std::fs::read(dir.path()).unwrap();
        std::fs::write(dir.path(), &bytes[..bytes.len() - 5]).unwrap();
        assert!(matches!(load_snapshot(dir.path()), Err(SnapshotError::Truncated { .. })));
    }
}

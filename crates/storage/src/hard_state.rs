use raft_core::PersistentState;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum HardStateError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("hard-state file is unreadable or corrupt: {0}")]
    Codec(#[from] bincode::Error),
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".tmp");
    PathBuf::from(name)
}

/// Loads the persisted `current_term` / `voted_for`.
///
/// A missing file means this is a brand-new node, so it starts from the default
/// (term 0, no vote). A file that exists but cannot be decoded is an ERROR, never
/// a silent reset to defaults: a node that quietly forgets its vote could grant a
/// second vote in a term it already voted in, which is exactly the failure this
/// file exists to prevent. Better to refuse to start and let an operator look.
pub fn load_hard_state(path: &Path) -> Result<PersistentState, HardStateError> {
    match fs::read(path) {
        Ok(bytes) => Ok(bincode::deserialize(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(PersistentState::default()),
        Err(e) => Err(e.into()),
    }
}

/// Durably replaces the hard-state file: write a temp file, fsync it, rename it
/// over the real one, then fsync the directory so the rename itself survives a
/// power loss. The rename is atomic, so a crash at any point leaves either the
/// complete old state or the complete new state, never a torn mixture.
pub fn save_hard_state(path: &Path, state: &PersistentState) -> Result<(), HardStateError> {
    let encoded = bincode::serialize(state)?;
    let tmp = temp_path(path);
    {
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

/// On Unix a rename is only durable once the containing directory has been
/// fsynced. Windows has no equivalent (and does not allow opening a directory
/// this way), so there it is a no-op.
#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    std::fs::File::open(parent)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
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
            let dir = std::env::temp_dir().join(format!("quorum-hs-{nanos}-{n}"));
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn file(&self) -> PathBuf {
            self.0.join("hard.state")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn state(term: u64, voted_for: Option<u32>) -> PersistentState {
        PersistentState { current_term: term, voted_for }
    }

    #[test]
    fn missing_file_loads_as_a_fresh_node() {
        let dir = TestDir::new();
        let loaded = load_hard_state(&dir.file()).unwrap();
        assert_eq!(loaded.current_term, 0);
        assert_eq!(loaded.voted_for, None);
    }

    #[test]
    fn saved_state_round_trips() {
        let dir = TestDir::new();
        save_hard_state(&dir.file(), &state(7, Some(3))).unwrap();
        let loaded = load_hard_state(&dir.file()).unwrap();
        assert_eq!(loaded.current_term, 7);
        assert_eq!(loaded.voted_for, Some(3));
    }

    #[test]
    fn a_later_save_replaces_the_earlier_one() {
        let dir = TestDir::new();
        save_hard_state(&dir.file(), &state(1, Some(2))).unwrap();
        save_hard_state(&dir.file(), &state(4, None)).unwrap();
        let loaded = load_hard_state(&dir.file()).unwrap();
        assert_eq!(loaded.current_term, 4);
        assert_eq!(loaded.voted_for, None);
    }

    #[test]
    fn a_corrupt_file_is_an_error_not_a_silent_reset_to_term_zero() {
        let dir = TestDir::new();
        fs::write(dir.file(), b"\x01\x02").unwrap();
        assert!(load_hard_state(&dir.file()).is_err());
    }

    #[test]
    fn no_temp_file_is_left_behind_after_a_save() {
        let dir = TestDir::new();
        save_hard_state(&dir.file(), &state(2, None)).unwrap();
        assert!(!temp_path(&dir.file()).exists());
    }
}
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The actual command a client issues, encoded into `LogEntry.command` bytes
/// at the raft-core layer. raft-core never inspects these - it only ever sees
/// opaque `Vec<u8>`. This is the bridge between "generic replicated log" and
/// "this specific KV application".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    /// A leader appends this immediately upon election (Raft paper §8), in
    /// its OWN current term. It changes nothing in the state machine - its
    /// only purpose is to give the new leader something from its current
    /// term to commit, which indirectly drags forward the commit index of
    /// every earlier entry the previous leader replicated but never got to
    /// mark committed before crashing (see cluster::become_leader).
    NoOp,
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("failed to encode command: {0}")]
    Encode(#[from] bincode::Error),
}

impl Command {
    pub fn encode(&self) -> Result<Vec<u8>, CommandError> {
        Ok(bincode::serialize(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CommandError> {
        Ok(bincode::deserialize(bytes)?)
    }
}

/// The state machine itself. Every node applies the *same* committed commands
/// in the *same* order, which is what gives the cluster a single consistent
/// view of the data despite running on N independent processes.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KvStore {
    data: HashMap<String, Vec<u8>>,
}

impl KvStore {
    pub fn new() -> Self {
        Self { data: HashMap::new() }
    }

    /// Applies a single committed command. Only ever called with commands
    /// that have already reached Raft's commit_index (i.e. replicated to a
    /// majority) - applying an uncommitted command would let a node expose
    /// data that could still be rolled back if that leader fails before
    /// committing.
    pub fn apply(&mut self, command: &Command) -> Option<Vec<u8>> {
        match command {
            Command::Put { key, value } => self.data.insert(key.clone(), value.clone()),
            Command::Delete { key } => self.data.remove(key),
            Command::NoOp => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_round_trips_through_encode_decode() {
        let encoded = Command::NoOp.encode().unwrap();
        let decoded = Command::decode(&encoded).unwrap();
        assert!(matches!(decoded, Command::NoOp));
    }

    #[test]
    fn applying_noop_changes_nothing() {
        let mut store = KvStore::new();
        store.apply(&Command::Put { key: "k".into(), value: b"v".to_vec() });
        assert_eq!(store.len(), 1);

        let result = store.apply(&Command::NoOp);
        assert!(result.is_none());
        assert_eq!(store.len(), 1); // untouched
        assert_eq!(store.get("k"), Some(&b"v".to_vec()));
    }
}
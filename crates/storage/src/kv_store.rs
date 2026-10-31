use raft_core::{ConfigChange, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The actual command a client issues, encoded into `LogEntry.command` bytes
/// at the raft-core layer. raft-core never inspects these - it only ever sees
/// opaque `Vec<u8>`. This is the bridge between "generic replicated log" and
/// "this specific KV application".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Adds a voting member to the cluster. Takes effect on every node the moment
    /// the entry is appended to its log, not when it commits. `addr` is where the
    /// other nodes dial it. APPEND-ONLY enum: new variants go at the end, because
    /// the variant's position is part of the on-disk and on-wire encoding.
    AddNode { id: NodeId, addr: String },
    /// Removes a voting member. Same timing rules as `AddNode`.
    RemoveNode { id: NodeId },
}

#[derive(Debug, thiserror::Error)]
pub enum CommandError {
    #[error("failed to encode command: {0}")]
    Encode(#[from] bincode::Error),
}

impl Command {
    /// The membership change this command stands for, if it is one. The key-value
    /// state machine ignores these; the consensus layer reads them straight out of
    /// the log to decide who is in the cluster.
    pub fn config_change(&self) -> Option<ConfigChange> {
        match self {
            Command::AddNode { id, addr } => Some(ConfigChange::Add { id: *id, addr: addr.clone() }),
            Command::RemoveNode { id } => Some(ConfigChange::Remove { id: *id }),
            Command::Put { .. } | Command::Delete { .. } | Command::NoOp => None,
        }
    }

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
            // Membership is not key-value state; the node reads it from the log.
            Command::NoOp | Command::AddNode { .. } | Command::RemoveNode { .. } => None,
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

    #[test]
    fn membership_commands_round_trip() {
        let add = Command::AddNode { id: 4, addr: "node4:7000".into() };
        let decoded = Command::decode(&add.encode().unwrap()).unwrap();
        assert_eq!(
            decoded.config_change(),
            Some(ConfigChange::Add { id: 4, addr: "node4:7000".into() })
        );
        let remove = Command::RemoveNode { id: 2 };
        let decoded = Command::decode(&remove.encode().unwrap()).unwrap();
        assert_eq!(decoded.config_change(), Some(ConfigChange::Remove { id: 2 }));
    }

    #[test]
    fn only_membership_commands_are_config_changes() {
        assert!(Command::NoOp.config_change().is_none());
        assert!(Command::Put { key: "k".into(), value: vec![] }.config_change().is_none());
        assert!(Command::Delete { key: "k".into() }.config_change().is_none());
    }

    #[test]
    fn membership_commands_leave_the_key_value_state_alone() {
        let mut store = KvStore::new();
        store.apply(&Command::Put { key: "k".into(), value: b"v".to_vec() });
        assert!(store.apply(&Command::AddNode { id: 4, addr: "x:1".into() }).is_none());
        assert!(store.apply(&Command::RemoveNode { id: 4 }).is_none());
        assert_eq!(store.len(), 1);
    }

    /// Existing logs on disk must still decode: the original variants keep their
    /// positions in the encoding. (bincode writes the variant index first.)
    #[test]
    fn the_original_command_encodings_are_unchanged() {
        assert_eq!(Command::Put { key: String::new(), value: vec![] }.encode().unwrap()[..4], [0, 0, 0, 0]);
        assert_eq!(Command::Delete { key: String::new() }.encode().unwrap()[..4], [1, 0, 0, 0]);
        assert_eq!(Command::NoOp.encode().unwrap(), vec![2, 0, 0, 0]);
        assert_eq!(Command::AddNode { id: 0, addr: String::new() }.encode().unwrap()[..4], [3, 0, 0, 0]);
        assert_eq!(Command::RemoveNode { id: 0 }.encode().unwrap()[..4], [4, 0, 0, 0]);
    }
}

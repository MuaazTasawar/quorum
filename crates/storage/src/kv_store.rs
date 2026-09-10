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

    /// Applies a single committed command. This is only ever called with commands
    /// that have already reached Raft's commit_index (i.e. replicated to a
    /// majority) - applying an uncommitted command would let a node expose data
    /// that could still be rolled back if that leader fails before committing.
    pub fn apply(&mut self, command: &Command) -> Option<Vec<u8>> {
        match command {
            Command::Put { key, value } => self.data.insert(key.clone(), value.clone()),
            Command::Delete { key } => self.data.remove(key),
        }
    }

    /// Linearizable reads in Raft require going through the leader (or a
    /// read-index/lease-read protocol) - this method is just the local lookup;
    /// the *linearizability* guarantee is enforced by the caller in the client
    /// crate, not here.
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
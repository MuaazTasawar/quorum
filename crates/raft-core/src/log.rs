use crate::state::Term;
use serde::{Deserialize, Serialize};

/// A single entry in the replicated log. `command` is opaque bytes at this layer -
/// raft-core doesn't know or care that it's a KV Put/Delete; that's the storage
/// crate's state machine's job to interpret once the entry is committed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// The term in which this entry was created by a leader. Used for the
    /// log-matching property: two entries with the same (index, term) are
    /// guaranteed to hold the same command, and every entry up to that point
    /// in both logs is identical.
    pub term: Term,
    /// 1-indexed position in the log. Index 0 is reserved as a sentinel
    /// "before the log starts" entry, never actually stored.
    pub index: u64,
    pub command: Vec<u8>,
}

/// In-memory view of the replicated log. The storage crate is responsible for
/// durability (WAL); this type is the logical structure raft-core's election
/// and replication logic reasons about.
#[derive(Debug, Default)]
pub struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Reconstructs a Log from a fully-ordered list of entries, used at node
    /// startup after WAL replay. The WAL's on-disk order already IS the log
    /// order (entries are appended in order, truncated in order), so this is
    /// just wrapping already-correct recovered data, not re-deriving anything.
    pub fn from_entries(entries: Vec<LogEntry>) -> Self {
        Self { entries }
    }

    pub fn last_index(&self) -> u64 {
        self.entries.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    pub fn term_at(&self, index: u64) -> Option<Term> {
        self.get(index).map(|e| e.term)
    }

    /// Appends a new entry created locally by this node as leader.
    pub fn append(&mut self, term: Term, command: Vec<u8>) -> u64 {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry { term, index, command });
        index
    }

    /// Overwrites the log from `from_index` onward with `new_entries`, used when
    /// a Follower receives AppendEntries and must discard conflicting entries.
    /// Raft safety rule: never truncate entries that are already committed -
    /// callers must ensure `from_index > commit_index` before calling this.
    pub fn truncate_and_append(&mut self, from_index: u64, new_entries: Vec<LogEntry>) {
        let keep = (from_index.saturating_sub(1)) as usize;
        self.entries.truncate(keep);
        self.entries.extend(new_entries);
    }

    /// Entries strictly after `after_index`, used by the leader to figure out
    /// what to send a follower whose `next_index` is behind.
    pub fn entries_after(&self, after_index: u64) -> Vec<LogEntry> {
        self.entries
            .iter()
            .filter(|e| e.index > after_index)
            .cloned()
            .collect()
    }

    /// The log-matching check from the Raft paper (§5.3): does this log contain
    /// an entry at `prev_index` with term `prev_term`? Empty log + prev_index 0
    /// trivially matches (start of log).
    pub fn matches(&self, prev_index: u64, prev_term: Term) -> bool {
        if prev_index == 0 {
            return true;
        }
        self.term_at(prev_index) == Some(prev_term)
    }
}
use std::time::{Duration, Instant};

/// A monotonically increasing Raft term. Every RPC and vote is scoped to a term;
/// a node that sees a higher term than its own immediately steps down to Follower.
pub type Term = u64;

/// A stable identifier for a node in the cluster. Must be unique and consistent
/// across restarts (used as the key in peer address maps and vote tallies).
pub type NodeId = u32;

/// The three roles a Raft node can occupy. Transitions are strictly:
/// Follower -> Candidate -> Leader, or any state -> Follower (on higher term).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// Persistent state that MUST survive a crash/restart before responding to any RPC.
/// This is the subset of Raft state that goes to the WAL before being acted on -
/// losing `current_term` or `voted_for` after a crash can violate Raft's safety
/// guarantees (a node could vote twice in the same term after restarting).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

/// Volatile state, reset on every restart, reconstructed from persistent state + log.
#[derive(Debug)]
pub struct VolatileState {
    pub role: Role,
    /// Index of highest log entry known to be committed (replicated to a majority).
    pub commit_index: u64,
    /// Index of highest log entry applied to the local state machine.
    pub last_applied: u64,
    /// When this node last heard from a leader (or granted a vote). Used to detect
    /// election timeout - if `now - last_heartbeat > timeout`, a Follower becomes
    /// a Candidate and starts an election.
    pub last_heartbeat: Instant,
}

/// Leader-only volatile state, reinitialized every time a node becomes Leader.
/// Tracks per-follower replication progress so the leader knows what to send next
/// and when an entry has reached a majority (and can be committed).
#[derive(Debug, Default)]
pub struct LeaderState {
    /// For each follower: index of the next log entry to send them.
    /// Optimistically initialized to leader's (last log index + 1), decremented
    /// on AppendEntries rejection until a matching prefix is found.
    pub next_index: std::collections::HashMap<NodeId, u64>,
    /// For each follower: highest log index known to be replicated on that follower.
    /// Used to compute commit_index (index replicated on a majority).
    pub match_index: std::collections::HashMap<NodeId, u64>,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            last_heartbeat: Instant::now(),
        }
    }

    /// True if enough time has passed without a heartbeat/vote grant that this
    /// Follower should become a Candidate and start an election.
    pub fn election_timed_out(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() > timeout
    }
}
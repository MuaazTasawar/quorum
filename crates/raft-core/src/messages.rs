use crate::log::LogEntry;
use crate::state::{NodeId, Term};
use serde::{Deserialize, Serialize};

/// Sent by a Candidate to every other node when starting an election.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: Term,
    pub candidate_id: NodeId,
    /// Used by the recipient's "at least as up-to-date" check (Raft paper §5.4.1):
    /// a candidate only gets a vote if its log is at least as current as the voter's,
    /// preventing a node with stale log entries from becoming leader.
    pub last_log_index: u64,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: Term,
    pub vote_granted: bool,
}

/// Sent by the Leader to replicate log entries and as a heartbeat (empty `entries`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: Term,
    pub leader_id: NodeId,
    /// Index/term of the log entry immediately preceding `entries`, used for the
    /// log-matching consistency check on the follower side.
    pub prev_log_index: u64,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    /// Leader's commit_index, so followers know they can safely apply entries
    /// up to this point to their own state machine.
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    /// On rejection, the follower's own last log index - lets the leader jump
    /// `next_index` back efficiently instead of decrementing one at a time.
    pub conflict_index: Option<u64>,
}

/// Top-level envelope for everything sent over the wire between nodes.
/// The `transport` crate frames and deserializes into this; raft-core only
/// ever sees `RpcMessage` variants, never raw bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcMessage {
    /// Sent as the first frame on any new connection so the accepting side
    /// knows who just dialed in - raw TCP carries no identity on its own.
    Hello(NodeId),
    RequestVote(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntries(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

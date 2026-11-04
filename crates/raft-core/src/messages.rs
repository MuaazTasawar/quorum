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

/// Sent by a node whose election timer fired, BEFORE it changes anything, to ask
/// "would you vote for me if I started an election?". Nothing about the sender's
/// or receiver's term or vote changes as a result (Raft dissertation §9.6), so a
/// node that is cut off from the cluster can ask forever without inflating its
/// term and then deposing a healthy leader when it reconnects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteRequest {
    /// The term the sender WOULD use (its current term + 1). It is not adopted
    /// by anyone who receives it.
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreVoteResponse {
    /// The responder's own current term. A higher one than the asker's tells the
    /// asker it is out of date.
    pub term: Term,
    pub vote_granted: bool,
    /// Echo of `PreVoteRequest::term`, so the asker can tell an answer to the
    /// current round from a late answer to an earlier one.
    pub request_term: Term,
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
    /// Which heartbeat round of this leader's term this request belongs to. The
    /// leader numbers every broadcast it sends and the follower echoes the number
    /// back, so the leader can tell exactly which round an answer refers to. That
    /// is what lets it prove, for a read, that a majority recognised it as leader
    /// AFTER the read arrived (ReadIndex, Raft dissertation §6.4).
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub success: bool,
    /// On rejection, the follower's own last log index - lets the leader jump
    /// `next_index` back efficiently instead of decrementing one at a time.
    pub conflict_index: Option<u64>,
    /// On success: the highest log index the follower has just verified matches
    /// the leader's log (`prev_log_index + entries.len()` of the request being
    /// answered). Echoing it makes every response self-describing, so the leader
    /// never has to guess what a response refers to by looking at its own
    /// (possibly newer) log tail. Always 0 on rejection.
    pub match_index: u64,
    /// Echo of `AppendEntriesRequest::seq`: the heartbeat round this answers.
    pub seq: u64,
}

/// One chunk of a snapshot, sent by a leader to a follower that has fallen behind the
/// part of the log the leader still holds (Raft paper §7). The snapshot travels as the
/// bytes of the snapshot file, which already carry their own format version and
/// checksum, split into chunks so no single message is large.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: Term,
    pub leader_id: NodeId,
    /// The last log entry the snapshot covers. Identifies the transfer: a chunk
    /// that belongs to a different snapshot than the one being received restarts it.
    pub last_included_index: u64,
    pub last_included_term: Term,
    /// Where `data` starts within the snapshot.
    pub offset: u64,
    /// Size of the whole snapshot in bytes.
    pub total_len: u64,
    pub data: Vec<u8>,
    /// True on the final chunk.
    pub done: bool,
    /// The heartbeat round this belongs to, echoed back like `AppendEntriesRequest::seq`.
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: Term,
    /// The next byte the follower expects. The leader continues from here, so a lost
    /// or repeated chunk, or a follower that restarted mid-transfer, just means the
    /// transfer resumes from where the follower really is.
    pub next_offset: u64,
    /// True once the follower's state includes everything through
    /// `last_included_index`: it was installed by this transfer, or it already had it.
    pub installed: bool,
    /// Echo of the request's `last_included_index`: which transfer this answers.
    pub last_included_index: u64,
    pub seq: u64,
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
    PreVote(PreVoteRequest),
    PreVoteResponse(PreVoteResponse),
    // New variants are only ever added at the END: the position of a variant is part
    // of the wire encoding, and every node in a cluster must run the same build.
    InstallSnapshot(InstallSnapshotRequest),
    InstallSnapshotResponse(InstallSnapshotResponse),
}
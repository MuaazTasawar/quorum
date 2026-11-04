use crate::messages::InstallSnapshotRequest;
use crate::state::{LeaderState, NodeId, PersistentState, Term, VolatileState};
use crate::state::Role;
use std::sync::Arc;

/// The largest snapshot a node will agree to receive. A transfer that announces more
/// is refused, so a bug or a hostile peer cannot make a node allocate without limit.
pub const MAX_SNAPSHOT_BYTES: u64 = 1 << 30;

/// The sending side of one snapshot transfer to one follower: which snapshot, and how
/// much of it the follower has confirmed. Pure bookkeeping: it builds the next chunk
/// to send and moves its position from the follower's answers.
///
/// It owns its bytes (shared by every follower being sent the same snapshot) for the
/// whole transfer, so a newer snapshot taken on the leader meanwhile cannot change
/// what is being sent half way through.
#[derive(Debug, Clone)]
pub struct SnapshotSender {
    last_included_index: u64,
    last_included_term: Term,
    data: Arc<Vec<u8>>,
    offset: u64,
}

impl SnapshotSender {
    pub fn new(last_included_index: u64, last_included_term: Term, data: Arc<Vec<u8>>) -> Self {
        Self { last_included_index, last_included_term, data, offset: 0 }
    }

    pub fn last_included_index(&self) -> u64 {
        self.last_included_index
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn total_len(&self) -> u64 {
        self.data.len() as u64
    }

    /// The chunk starting at the follower's confirmed position, at most `chunk_bytes`
    /// long. Asking again without an answer in between returns the same chunk, which
    /// is exactly what retransmission needs.
    pub fn next_chunk(
        &self,
        chunk_bytes: usize,
        term: Term,
        leader_id: NodeId,
        seq: u64,
    ) -> InstallSnapshotRequest {
        let total = self.data.len();
        let start = (self.offset as usize).min(total);
        let end = start.saturating_add(chunk_bytes.max(1)).min(total);
        InstallSnapshotRequest {
            term,
            leader_id,
            last_included_index: self.last_included_index,
            last_included_term: self.last_included_term,
            offset: start as u64,
            total_len: total as u64,
            data: self.data[start..end].to_vec(),
            done: end == total,
            seq,
        }
    }

    /// Takes the follower's reported position. Returns true if it differs from where
    /// the sender was, in either direction: forward means a chunk was accepted and the
    /// next should go out, backward means the follower lost what it had. Returns false
    /// for a repeated answer, which must not trigger another send (or every duplicate
    /// would multiply the traffic).
    pub fn acknowledge(&mut self, next_offset: u64) -> bool {
        let clamped = next_offset.min(self.total_len());
        let changed = clamped != self.offset;
        self.offset = clamped;
        changed
    }
}

/// What the receiving side made of one chunk.
#[derive(Debug, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// Stored; the transfer continues. `next_offset` is where the next chunk must start.
    Accepted { next_offset: u64 },
    /// That was the last chunk and the whole snapshot is now assembled.
    Complete(Vec<u8>),
    /// The chunk does not start where the next one should (lost, repeated or reordered).
    /// Nothing was stored; `expected` is where the sender must continue from.
    Misaligned { expected: u64 },
    /// The chunk contradicts the transfer (runs past the announced size, or the last
    /// chunk leaves the snapshot short). The transfer is abandoned and starts over.
    Invalid,
}

/// The receiving side of one snapshot transfer: reassembles chunks in order.
#[derive(Debug)]
pub struct SnapshotAssembler {
    last_included_index: u64,
    last_included_term: Term,
    total_len: u64,
    buffer: Vec<u8>,
}

impl SnapshotAssembler {
    /// Starts receiving a snapshot, or `None` if its announced size is refused.
    pub fn start(last_included_index: u64, last_included_term: Term, total_len: u64) -> Option<Self> {
        if total_len > MAX_SNAPSHOT_BYTES {
            return None;
        }
        Some(Self { last_included_index, last_included_term, total_len, buffer: Vec::new() })
    }

    /// True if this assembler is receiving the snapshot `request` belongs to.
    pub fn is_receiving(&self, request: &InstallSnapshotRequest) -> bool {
        self.last_included_index == request.last_included_index
            && self.last_included_term == request.last_included_term
            && self.total_len == request.total_len
    }

    pub fn received(&self) -> u64 {
        self.buffer.len() as u64
    }

    pub fn accept(&mut self, offset: u64, data: &[u8], done: bool) -> ChunkOutcome {
        let have = self.buffer.len() as u64;
        if offset != have {
            return ChunkOutcome::Misaligned { expected: have };
        }
        if have + data.len() as u64 > self.total_len {
            return ChunkOutcome::Invalid;
        }
        self.buffer.extend_from_slice(data);
        let have = self.buffer.len() as u64;
        if done {
            if have != self.total_len {
                return ChunkOutcome::Invalid;
            }
            return ChunkOutcome::Complete(std::mem::take(&mut self.buffer));
        }
        ChunkOutcome::Accepted { next_offset: have }
    }
}

/// How a node treats an InstallSnapshot request's term. Returns `None` if the sender
/// is a stale leader (its term is below ours: refuse, and the answer carries our term
/// so it steps down). Otherwise the sender is the legitimate leader: its term is
/// adopted, this node becomes (or stays) a follower, and `Some(is_new_term)` says
/// whether the term moved. Mirrors what `handle_append_entries` does first.
pub fn accept_leader_term(
    persistent: &mut PersistentState,
    volatile: &mut VolatileState,
    term: Term,
) -> Option<bool> {
    if term < persistent.current_term {
        return None;
    }
    let is_new_term = term > persistent.current_term;
    if is_new_term {
        persistent.current_term = term;
        persistent.voted_for = None;
    }
    volatile.role = Role::Follower;
    volatile.last_heartbeat = std::time::Instant::now();
    Some(is_new_term)
}

/// The leader's bookkeeping once a follower reports its state now includes everything
/// through `last_included_index`: that prefix matches, and replication resumes from
/// the entry after it. Monotonic, like every other update to `match_index`.
pub fn apply_snapshot_installed(
    leader_state: &mut LeaderState,
    follower_id: NodeId,
    last_included_index: u64,
) {
    let known_match = leader_state.match_index.get(&follower_id).copied().unwrap_or(0);
    let new_match = known_match.max(last_included_index);
    leader_state.match_index.insert(follower_id, new_match);
    leader_state.next_index.insert(follower_id, new_match + 1);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: usize) -> Arc<Vec<u8>> {
        Arc::new((0..n).map(|i| (i % 251) as u8).collect())
    }

    #[test]
    fn a_snapshot_is_cut_into_chunks_that_reassemble_exactly() {
        let data = bytes(1000);
        let mut sender = SnapshotSender::new(7, 2, data.clone());
        let mut assembler = SnapshotAssembler::start(7, 2, 1000).unwrap();
        let mut chunks = 0;
        let assembled = loop {
            let chunk = sender.next_chunk(300, 3, 1, 9);
            assert!(chunk.data.len() <= 300);
            chunks += 1;
            match assembler.accept(chunk.offset, &chunk.data, chunk.done) {
                ChunkOutcome::Accepted { next_offset } => {
                    assert!(sender.acknowledge(next_offset));
                }
                ChunkOutcome::Complete(all) => break all,
                other => panic!("unexpected {other:?}"),
            }
        };
        assert_eq!(chunks, 4);
        assert_eq!(assembled, *data);
    }

    #[test]
    fn only_the_last_chunk_is_marked_done() {
        let sender = SnapshotSender::new(1, 1, bytes(10));
        assert!(!sender.next_chunk(4, 1, 1, 0).done);
        assert!(sender.next_chunk(10, 1, 1, 0).done);
        assert!(sender.next_chunk(1000, 1, 1, 0).done);
    }

    #[test]
    fn an_empty_snapshot_is_one_empty_final_chunk() {
        let sender = SnapshotSender::new(1, 1, Arc::new(Vec::new()));
        let chunk = sender.next_chunk(64, 1, 1, 0);
        assert!(chunk.done && chunk.data.is_empty() && chunk.offset == 0);
        let mut assembler = SnapshotAssembler::start(1, 1, 0).unwrap();
        assert_eq!(assembler.accept(0, &[], true), ChunkOutcome::Complete(vec![]));
    }

    #[test]
    fn asking_again_without_an_answer_repeats_the_same_chunk() {
        let sender = SnapshotSender::new(1, 1, bytes(100));
        let a = sender.next_chunk(30, 1, 1, 5);
        let b = sender.next_chunk(30, 1, 1, 6);
        assert_eq!((a.offset, &a.data), (b.offset, &b.data));
    }

    #[test]
    fn a_repeated_acknowledgement_does_not_ask_for_another_send() {
        let mut sender = SnapshotSender::new(1, 1, bytes(100));
        assert!(sender.acknowledge(30));
        assert!(!sender.acknowledge(30), "a duplicate answer must not multiply the traffic");
        assert!(sender.acknowledge(0), "a follower that lost its progress sends us back");
        assert_eq!(sender.offset(), 0);
    }

    #[test]
    fn an_acknowledgement_past_the_end_is_clamped() {
        let mut sender = SnapshotSender::new(1, 1, bytes(10));
        sender.acknowledge(500);
        assert_eq!(sender.offset(), 10);
    }

    #[test]
    fn a_repeated_chunk_is_not_stored_twice() {
        let mut assembler = SnapshotAssembler::start(1, 1, 100).unwrap();
        assert_eq!(assembler.accept(0, &[1; 30], false), ChunkOutcome::Accepted { next_offset: 30 });
        assert_eq!(assembler.accept(0, &[1; 30], false), ChunkOutcome::Misaligned { expected: 30 });
        assert_eq!(assembler.received(), 30);
    }

    #[test]
    fn a_chunk_that_skips_ahead_is_refused_and_says_where_to_continue() {
        let mut assembler = SnapshotAssembler::start(1, 1, 100).unwrap();
        assembler.accept(0, &[1; 30], false);
        assert_eq!(assembler.accept(60, &[1; 10], false), ChunkOutcome::Misaligned { expected: 30 });
    }

    #[test]
    fn data_running_past_the_announced_size_invalidates_the_transfer() {
        let mut assembler = SnapshotAssembler::start(1, 1, 50).unwrap();
        assert_eq!(assembler.accept(0, &[1; 60], false), ChunkOutcome::Invalid);
    }

    #[test]
    fn a_final_chunk_that_leaves_the_snapshot_short_is_invalid() {
        let mut assembler = SnapshotAssembler::start(1, 1, 50).unwrap();
        assert_eq!(assembler.accept(0, &[1; 40], true), ChunkOutcome::Invalid);
    }

    #[test]
    fn a_snapshot_larger_than_the_limit_is_refused_up_front() {
        assert!(SnapshotAssembler::start(1, 1, MAX_SNAPSHOT_BYTES + 1).is_none());
        assert!(SnapshotAssembler::start(1, 1, MAX_SNAPSHOT_BYTES).is_some());
    }

    #[test]
    fn a_chunk_is_recognised_as_belonging_to_the_transfer_in_progress() {
        let assembler = SnapshotAssembler::start(7, 2, 100).unwrap();
        let same = SnapshotSender::new(7, 2, bytes(100)).next_chunk(10, 1, 1, 0);
        let other_index = SnapshotSender::new(8, 2, bytes(100)).next_chunk(10, 1, 1, 0);
        let other_len = SnapshotSender::new(7, 2, bytes(101)).next_chunk(10, 1, 1, 0);
        assert!(assembler.is_receiving(&same));
        assert!(!assembler.is_receiving(&other_index));
        assert!(!assembler.is_receiving(&other_len));
    }

    #[test]
    fn a_stale_leaders_snapshot_is_refused_and_changes_nothing() {
        let mut persistent = PersistentState::default();
        persistent.current_term = 5;
        persistent.voted_for = Some(3);
        let mut volatile = VolatileState::new();
        assert_eq!(accept_leader_term(&mut persistent, &mut volatile, 4), None);
        assert_eq!((persistent.current_term, persistent.voted_for), (5, Some(3)));
    }

    #[test]
    fn a_newer_leaders_snapshot_moves_the_term_and_clears_the_vote() {
        let mut persistent = PersistentState::default();
        persistent.current_term = 5;
        persistent.voted_for = Some(3);
        let mut volatile = VolatileState::new();
        volatile.role = Role::Candidate;
        assert_eq!(accept_leader_term(&mut persistent, &mut volatile, 6), Some(true));
        assert_eq!((persistent.current_term, persistent.voted_for), (6, None));
        assert_eq!(volatile.role, Role::Follower);
        assert_eq!(accept_leader_term(&mut persistent, &mut volatile, 6), Some(false));
    }

    #[test]
    fn an_installed_snapshot_moves_the_followers_progress_forward_only() {
        let mut state = LeaderState::default();
        state.match_index.insert(2, 3);
        state.next_index.insert(2, 1);
        apply_snapshot_installed(&mut state, 2, 40);
        assert_eq!((state.match_index[&2], state.next_index[&2]), (40, 41));
        apply_snapshot_installed(&mut state, 2, 10);
        assert_eq!((state.match_index[&2], state.next_index[&2]), (40, 41), "never backwards");
    }
}

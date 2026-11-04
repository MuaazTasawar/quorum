use raft_core::RpcMessage;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode/decode error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("frame exceeds max size ({0} bytes) - possible corrupt stream or malicious peer")]
    FrameTooLarge(u32),
}

/// Hard ceiling on a single frame's size. Without this, a corrupt length prefix
/// (or a malicious/buggy peer) could claim a multi-gigabyte frame and cause an
/// unbounded allocation - this turns that into a clean error instead of an OOM.
const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024; // 64 MiB, generous for log-entry batches

/// Writes one `RpcMessage` to the stream as [4-byte LE length][bincode payload],
/// the same framing scheme as the WAL on-disk format (storage::wal) - same
/// reasoning applies: a fixed-width length prefix lets the reader know exactly
/// how many bytes to pull for one message without needing a delimiter.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &RpcMessage,
) -> Result<(), CodecError> {
    let encoded = bincode::serialize(msg)?;
    let len = encoded.len() as u32;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

/// Reads exactly one `RpcMessage` from the stream, blocking (asynchronously)
/// until a full frame has arrived. Returns `Ok(None)` on clean EOF (peer closed
/// the connection between frames, not mid-frame), so callers can distinguish
/// "peer hung up" from "the stream is actually broken".
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Option<RpcMessage>, CodecError> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(CodecError::Io(e)),
    }

    let len = u32::from_le_bytes(len_buf);
    if len > MAX_FRAME_BYTES {
        return Err(CodecError::FrameTooLarge(len));
    }

    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;
    let msg = bincode::deserialize(&payload)?;
    Ok(Some(msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_core::{RequestVoteRequest, RpcMessage};
    use std::io::Cursor;

    #[tokio::test]
    async fn a_snapshot_chunk_and_its_answer_round_trip_through_the_codec() {
        use raft_core::{InstallSnapshotRequest, InstallSnapshotResponse};
        let data: Vec<u8> = (0..=255u8).cycle().take(100_000).collect();
        let request = RpcMessage::InstallSnapshot(InstallSnapshotRequest {
            term: 4,
            leader_id: 2,
            last_included_index: 900,
            last_included_term: 3,
            offset: 65_536,
            total_len: 165_536,
            data: data.clone(),
            done: false,
            seq: 12,
        });
        let response = RpcMessage::InstallSnapshotResponse(InstallSnapshotResponse {
            term: 4,
            next_offset: 165_536,
            installed: true,
            last_included_index: 900,
            seq: 12,
        });

        let mut buf = Vec::new();
        write_frame(&mut buf, &request).await.unwrap();
        write_frame(&mut buf, &response).await.unwrap();
        let mut cursor = Cursor::new(buf);

        match read_frame(&mut cursor).await.unwrap().unwrap() {
            RpcMessage::InstallSnapshot(req) => {
                assert_eq!((req.last_included_index, req.offset, req.seq), (900, 65_536, 12));
                assert_eq!(req.data, data, "the chunk's bytes arrive untouched");
                assert!(!req.done);
            }
            other => panic!("decoded wrong variant: {other:?}"),
        }
        match read_frame(&mut cursor).await.unwrap().unwrap() {
            RpcMessage::InstallSnapshotResponse(resp) => {
                assert!(resp.installed);
                assert_eq!(resp.next_offset, 165_536);
            }
            other => panic!("decoded wrong variant: {other:?}"),
        }
    }

    /// Every node in a cluster must agree on the encoding, and new messages are only
    /// ever added at the end of the enum. Pin the positions of the ones that exist.
    #[test]
    fn the_message_variants_keep_their_positions_in_the_encoding() {
        use raft_core::{
            InstallSnapshotRequest, PreVoteResponse, RequestVoteResponse,
        };
        let first_u32 = |msg: &RpcMessage| {
            let bytes = bincode::serialize(msg).unwrap();
            u32::from_le_bytes(bytes[..4].try_into().unwrap())
        };
        assert_eq!(first_u32(&RpcMessage::Hello(1)), 0);
        assert_eq!(
            first_u32(&RpcMessage::RequestVoteResponse(RequestVoteResponse { term: 1, vote_granted: true })),
            2
        );
        assert_eq!(
            first_u32(&RpcMessage::PreVoteResponse(PreVoteResponse {
                term: 1,
                vote_granted: true,
                request_term: 1
            })),
            6
        );
        assert_eq!(
            first_u32(&RpcMessage::InstallSnapshot(InstallSnapshotRequest {
                term: 1,
                leader_id: 1,
                last_included_index: 1,
                last_included_term: 1,
                offset: 0,
                total_len: 0,
                data: vec![],
                done: true,
                seq: 0
            })),
            7
        );
    }

    #[tokio::test]
    async fn round_trips_a_message_through_the_codec() {
        let msg = RpcMessage::RequestVote(RequestVoteRequest {
            term: 7,
            candidate_id: 3,
            last_log_index: 42,
            last_log_term: 6,
        });

        let mut buf = Vec::new();
        write_frame(&mut buf, &msg).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let decoded = read_frame(&mut cursor).await.unwrap().unwrap();

        match decoded {
            RpcMessage::RequestVote(req) => {
                assert_eq!(req.term, 7);
                assert_eq!(req.candidate_id, 3);
                assert_eq!(req.last_log_index, 42);
            }
            _ => panic!("decoded wrong variant"),
        }
    }

    #[tokio::test]
    async fn read_frame_returns_none_on_clean_eof() {
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let result = read_frame(&mut cursor).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn rejects_frame_claiming_size_over_the_limit() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(MAX_FRAME_BYTES + 1).to_le_bytes());
        let mut cursor = Cursor::new(buf);
        let result = read_frame(&mut cursor).await;
        assert!(matches!(result, Err(CodecError::FrameTooLarge(_))));
    }
}

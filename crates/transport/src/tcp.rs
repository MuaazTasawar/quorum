use crate::codec::{read_frame, write_frame, CodecError};
use raft_core::{NodeId, RpcMessage};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("codec error: {0}")]
    Codec(#[from] CodecError),
    #[error("connection closed by peer")]
    ConnectionClosed,
}

/// A single peer connection. Raft is chatty (heartbeats on every election
/// timeout interval, from every node to every other node) so connections are
/// meant to be held open and reused, not established fresh per-RPC - the node
/// crate owns a long-lived `Connection` per peer rather than dialing per message.
pub struct Connection {
    stream: TcpStream,
    /// The peer this connection talks to, mainly for logging/debugging -
    /// nothing in the wire protocol itself depends on it.
    pub peer_id: Option<NodeId>,
}

impl Connection {
    /// Dials out to a peer. `peer_id` is optional at connect time since the
    /// caller may be dialing by address before formal cluster membership is
    /// confirmed (e.g. during initial cluster bootstrap).
    pub async fn connect(addr: &str, peer_id: Option<NodeId>) -> Result<Self, TransportError> {
        let stream = TcpStream::connect(addr).await?;
        // Disable Nagle's algorithm: Raft RPCs are small and latency-sensitive
        // (an election or a client write is blocked on this round-trip), so we
        // want frames sent immediately rather than batched with a delay.
        stream.set_nodelay(true)?;
        Ok(Self { stream, peer_id })
    }

    /// Wraps an already-accepted inbound `TcpStream` (from a listener) as a
    /// `Connection`, symmetric with `connect` for outbound.
    pub fn from_stream(stream: TcpStream, peer_id: Option<NodeId>) -> Result<Self, TransportError> {
        stream.set_nodelay(true)?;
        Ok(Self { stream, peer_id })
    }

    pub async fn send(&mut self, msg: &RpcMessage) -> Result<(), TransportError> {
        write_frame(&mut self.stream, msg).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<RpcMessage, TransportError> {
        read_frame(&mut self.stream)
            .await?
            .ok_or(TransportError::ConnectionClosed)
    }
}

/// Binds a listener for incoming peer connections. The node crate's cluster
/// wiring (Phase 6) owns the accept loop; this just provides the bound socket,
/// keeping `transport` free of any Raft state/logic - it only ever moves bytes.
pub async fn bind(addr: &str) -> Result<TcpListener, TransportError> {
    let listener = TcpListener::bind(addr).await?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_core::{RequestVoteRequest, RpcMessage};

    #[tokio::test]
    async fn connection_round_trips_a_message_over_real_tcp() {
        let listener = bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut conn = Connection::from_stream(stream, None).unwrap();
            conn.recv().await.unwrap()
        });

        let mut client = Connection::connect(&addr, Some(1)).await.unwrap();
        let msg = RpcMessage::RequestVote(RequestVoteRequest {
            term: 3,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        });
        client.send(&msg).await.unwrap();

        let received = server.await.unwrap();
        match received {
            RpcMessage::RequestVote(req) => assert_eq!(req.term, 3),
            _ => panic!("wrong variant received"),
        }
    }
}
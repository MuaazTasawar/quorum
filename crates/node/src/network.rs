use raft_core::{NodeId, RpcMessage};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use transport::Connection;

/// Spawns one long-lived task per outbound peer connection. Retries with a
/// fixed backoff if the peer isn't reachable yet (common at cluster startup,
/// when nodes race to come up) or if the connection later drops. Returns a
/// sender the rest of the node uses to queue outbound RpcMessages - the task
/// owns the actual socket and handles reconnects transparently, so callers
/// never need to know a reconnect happened.
pub fn spawn_outbound(
    self_id: NodeId,
    peer_id: NodeId,
    addr: String,
    inbox_tx: mpsc::UnboundedSender<(NodeId, RpcMessage)>,
) -> mpsc::UnboundedSender<RpcMessage> {
    let (tx, mut rx) = mpsc::unbounded_channel::<RpcMessage>();

    tokio::spawn(async move {
        loop {
            let conn = loop {
                match Connection::connect(&addr, Some(peer_id)).await {
                    Ok(mut c) => {
                        if c.send(&RpcMessage::Hello(self_id)).await.is_ok() {
                            info!(peer_id, %addr, "connected to peer");
                            break c;
                        }
                    }
                    Err(e) => {
                        debug!(peer_id, %addr, error = %e, "peer unreachable, retrying");
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            };

            if let Err(e) = drive_outbound(conn, &mut rx, peer_id, &inbox_tx).await {
                warn!(peer_id, error = %e, "outbound connection to peer lost, reconnecting");
            }
        }
    });

    tx
}

/// Drives one live connection: forwards queued outbound messages to the peer
/// AND reads whatever the peer sends back on the same duplex socket, tagging
/// incoming messages with `peer_id` (known for certain, since we dialed them).
async fn drive_outbound(
    mut conn: Connection,
    rx: &mut mpsc::UnboundedReceiver<RpcMessage>,
    peer_id: NodeId,
    inbox_tx: &mpsc::UnboundedSender<(NodeId, RpcMessage)>,
) -> anyhow::Result<()> {
    loop {
        tokio::select! {
            outgoing = rx.recv() => {
                match outgoing {
                    Some(msg) => conn.send(&msg).await?,
                    None => return Ok(()), // channel closed - node shutting down
                }
            }
            incoming = conn.recv() => {
                inbox_tx.send((peer_id, incoming?)).ok();
            }
        }
    }
}

/// Accept loop for inbound peer connections. Each connection must send a
/// `Hello(NodeId)` as its first frame - that's the only way we learn who's
/// calling, since raw TCP carries no identity. After the handshake, every
/// further frame from that connection is tagged with the now-known peer_id
/// and forwarded into the shared inbox.
pub async fn run_accept_loop(listener: TcpListener, inbox_tx: mpsc::UnboundedSender<(NodeId, RpcMessage)>) {
    loop {
        let (stream, peer_addr) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                warn!(error = %e, "accept failed");
                continue;
            }
        };
        let inbox_tx = inbox_tx.clone();
        tokio::spawn(async move {
            let mut conn = match Connection::from_stream(stream, None) {
                Ok(c) => c,
                Err(e) => {
                    warn!(%peer_addr, error = %e, "failed to wrap accepted stream");
                    return;
                }
            };

            let peer_id = match conn.recv().await {
                Ok(RpcMessage::Hello(id)) => id,
                _ => {
                    warn!(%peer_addr, "connection did not send Hello handshake, dropping");
                    return;
                }
            };
            info!(peer_id, %peer_addr, "accepted connection from peer");

            loop {
                match conn.recv().await {
                    Ok(msg) => {
                        inbox_tx.send((peer_id, msg)).ok();
                    }
                    Err(e) => {
                        warn!(peer_id, error = %e, "inbound connection closed");
                        return;
                    }
                }
            }
        });
    }
}
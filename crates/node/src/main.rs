use node::{
    cluster::{Cluster, PeerConnector},
    config::{parse_max_value_bytes, request_timeout_from_env, NodeConfig},
    metrics_api, network,
};
use std::collections::HashMap;
use storage::Persistence;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .json()
        .init();

    let config = NodeConfig::from_env()?;
    tracing::info!(
        node_id = config.id,
        read_mode = ?config.read_mode,
        join = config.join,
        "starting quorum node"
    );

    // Recovers the log AND the hard state (current_term / voted_for) from disk.
    // A node that restarts without its vote could vote twice in one term, so a
    // corrupt hard-state file stops startup here instead of silently resetting.
    let recovered = Persistence::open(&config.storage_dir)?;
    tracing::info!(
        term = recovered.hard_state.current_term,
        voted_for = ?recovered.hard_state.voted_for,
        recovered_entries = recovered.entries.len(),
        "recovered state from disk"
    );
    if recovered.discarded_wal_bytes > 0 {
        // The previous run died part-way through writing a log entry. That write
        // never finished, so nothing acknowledged was lost; the partial tail was cut off.
        tracing::warn!(
            discarded_bytes = recovered.discarded_wal_bytes,
            "dropped an unfinished write from the end of the log left by a crash"
        );
    }

    tracing::info!(
        channel_capacity = config.limits.channel_capacity,
        max_batch = config.limits.max_batch,
        "queue limits"
    );
    let (inbox_tx, inbox_rx) = mpsc::channel(config.limits.channel_capacity);

    // How this node dials a peer. Used now for the configured peers, and by the
    // cluster later for any member it learns about from the log.
    let connector: PeerConnector = {
        let self_id = config.id;
        let inbox_tx = inbox_tx.clone();
        let capacity = config.limits.channel_capacity;
        Box::new(move |peer_id, addr| {
            network::spawn_outbound(self_id, peer_id, addr.to_string(), inbox_tx.clone(), capacity)
        })
    };

    let mut peer_outboxes = HashMap::new();
    for (&peer_id, addr) in &config.peers {
        peer_outboxes.insert(peer_id, connector(peer_id, addr));
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tokio::spawn(network::run_accept_loop(listener, inbox_tx));

    let metrics_addr = config.metrics_addr.clone();
    let request_timeout = request_timeout_from_env()?;
    tracing::info!(timeout_ms = request_timeout.as_millis() as u64, "request timeout");
    let (cluster, handle) = Cluster::new(config, recovered, peer_outboxes, inbox_rx, connector);

    let max_value_bytes = parse_max_value_bytes(std::env::var("QUORUM_MAX_VALUE_BYTES").ok().as_deref())?;
    tracing::info!(max_value_bytes, "largest accepted value");
    let app_state = metrics_api::AppState::from_handle(handle, request_timeout)
        .with_max_value_bytes(max_value_bytes);
    let router = metrics_api::router(app_state);
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(&metrics_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "failed to bind metrics API listener");
                return;
            }
        };
        tracing::info!(%metrics_addr, "metrics API listening");
        if let Err(e) = axum::serve(listener, router).await {
            tracing::error!(error = %e, "metrics API server error");
        }
    });

    cluster.run().await;

    Ok(())
}
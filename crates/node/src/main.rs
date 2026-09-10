mod cluster;
mod config;
mod errors;
mod metrics_api;
mod network;

use cluster::Cluster;
use config::NodeConfig;
use metrics_api::AppState;
use raft_core::{Log, PersistentState};
use storage::{KvStore, Wal};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .json()
        .init();

    let config = NodeConfig::from_env()?;
    tracing::info!(node_id = config.id, "starting quorum node");

    std::fs::create_dir_all(&config.storage_dir)?;
    let wal_path = config.storage_dir.join("wal.log");

    let recovered_entries = Wal::replay(&wal_path)?;
    let wal = Wal::open(&wal_path)?;
    let log = Log::from_entries(recovered_entries);

    // TODO Phase 9.x: persist current_term/voted_for across restarts.
    let persistent = PersistentState::default();
    let kv_store = KvStore::new();

    let (inbox_tx, inbox_rx) = mpsc::unbounded_channel();

    let mut peer_outboxes = HashMap::new();
    for (&peer_id, addr) in &config.peers {
        let tx = network::spawn_outbound(config.id, peer_id, addr.clone(), inbox_tx.clone());
        peer_outboxes.insert(peer_id, tx);
    }

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tokio::spawn(network::run_accept_loop(listener, inbox_tx));

    let metrics_addr = config.metrics_addr.clone();
    let (cluster, handle) =
        Cluster::new(config, wal, log, kv_store, persistent, peer_outboxes, inbox_rx);

    // Metrics/dashboard API runs as its own task, talking to the cluster
    // loop only through the channels in ClusterHandle - it never touches
    // Cluster's internals directly.
    let app_state = AppState::from_handle(handle);
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
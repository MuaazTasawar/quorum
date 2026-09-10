mod cluster;
mod config;
mod network;

use cluster::Cluster;
use config::NodeConfig;
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

    // Recover: replay the WAL to reconstruct the log as it stood before any
    // crash/restart. No snapshot-load wired in yet (Phase 2 built save/load,
    // but bootstrap doesn't call it until periodic compaction lands) -
    // correct for now, just not compacted.
    let recovered_entries = Wal::replay(&wal_path)?;
    let wal = Wal::open(&wal_path)?;
    let log = Log::from_entries(recovered_entries);

    // TODO Phase 6.1: persist current_term/voted_for across restarts - right
    // now a restarted node starts at term 0, which is safe (it'll just lose
    // an election to anyone with a higher term) but not optimal.
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

    // _handle (command_tx + snapshot_rx) will be handed to the Axum metrics
    // API in Phase 7 so client requests and the dashboard can reach the
    // cluster loop without touching its internals directly.
    let (cluster, _handle) =
        Cluster::new(config, wal, log, kv_store, persistent, peer_outboxes, inbox_rx);
    cluster.run().await;

    Ok(())
}
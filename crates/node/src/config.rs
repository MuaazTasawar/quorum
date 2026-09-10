use raft_core::NodeId;
use std::collections::HashMap;
use std::path::PathBuf;

/// Static configuration for one node. Loaded from environment variables
/// rather than a config file so docker-compose (Phase 11) can set each
/// node up purely through its `environment:` block.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub listen_addr: String,
    pub metrics_addr: String,
    pub peers: HashMap<NodeId, String>,
    pub storage_dir: PathBuf,
}

impl NodeConfig {
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Expected vars:
    ///   QUORUM_NODE_ID=1
    ///   QUORUM_LISTEN_ADDR=0.0.0.0:7000
    ///   QUORUM_METRICS_ADDR=0.0.0.0:8000
    ///   QUORUM_PEERS=2=node2:7000,3=node3:7000
    ///   QUORUM_STORAGE_DIR=/data
    pub fn from_env() -> anyhow::Result<Self> {
        let id: NodeId = std::env::var("QUORUM_NODE_ID")
            .map_err(|_| anyhow::anyhow!("QUORUM_NODE_ID not set"))?
            .parse()?;
        let listen_addr =
            std::env::var("QUORUM_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:7000".to_string());
        let metrics_addr =
            std::env::var("QUORUM_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_string());
        let storage_dir: PathBuf =
            std::env::var("QUORUM_STORAGE_DIR").unwrap_or_else(|_| "./data".to_string()).into();

        let mut peers = HashMap::new();
        if let Ok(raw) = std::env::var("QUORUM_PEERS") {
            for entry in raw.split(',').filter(|s| !s.is_empty()) {
                let (id_str, addr) = entry
                    .split_once('=')
                    .ok_or_else(|| anyhow::anyhow!("bad QUORUM_PEERS entry: {entry}"))?;
                peers.insert(id_str.parse()?, addr.to_string());
            }
        }

        Ok(Self { id, listen_addr, metrics_addr, peers, storage_dir })
    }
}
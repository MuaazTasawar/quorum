pub mod leader_tracking;

use leader_tracking::LeaderTracker;
use raft_core::NodeId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("no reachable node responded as leader after trying the whole cluster")]
    NoLeaderReachable,
}

#[derive(Deserialize)]
struct GetResponse {
    is_leader: bool,
    value: Option<String>,
    leader_hint: Option<u32>,
}

#[derive(Serialize)]
struct PutBody {
    value: String,
}

#[derive(Deserialize)]
struct WriteResponse {
    is_leader: bool,
    #[allow(dead_code)] // present in the wire format; not currently branched on separately from is_leader
    applied: bool,
    leader_hint: Option<u32>,
}

/// A client for a Quorum cluster. Handles leader discovery and redirect
/// transparently - callers just call `get`/`put`/`delete` with a key (and a
/// value for `put`); the client figures out which node is actually the
/// leader, retrying against a hinted or fallback node when its current guess
/// is wrong, without the caller ever seeing a `NotLeader` response directly.
pub struct QuorumClient {
    http: reqwest::Client,
    tracker: LeaderTracker,
}

impl QuorumClient {
    /// `nodes` maps each node's id to its metrics API address, e.g.
    /// `{1: "127.0.0.1:8001", 2: "127.0.0.1:8002", 3: "127.0.0.1:8003"}`.
    pub fn new(nodes: HashMap<NodeId, String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("reqwest client builds with a static config");
        Self { http, tracker: LeaderTracker::new(nodes) }
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, ClientError> {
        for (node_id, addr) in self.candidate_order() {
            let url = format!("http://{addr}/kv/{key}");
            let resp = match self.http.get(&url).send().await {
                Ok(r) => r,
                Err(_) => continue, // node unreachable - try the next candidate
            };
            let body: GetResponse = match resp.json().await {
                Ok(b) => b,
                Err(_) => continue,
            };

            if body.is_leader {
                self.tracker.record_confirmed_leader(node_id);
                return Ok(body.value);
            }
            self.tracker.record_not_leader(body.leader_hint.map(|h| h as NodeId));
        }
        Err(ClientError::NoLeaderReachable)
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<(), ClientError> {
        for (node_id, addr) in self.candidate_order() {
            let url = format!("http://{addr}/kv/{key}");
            let resp = match self
                .http
                .put(&url)
                .json(&PutBody { value: value.to_string() })
                .send()
                .await
            {
                Ok(r) => r,
                Err(_) => continue,
            };
            let body: WriteResponse = match resp.json().await {
                Ok(b) => b,
                Err(_) => continue,
            };

            if body.is_leader {
                self.tracker.record_confirmed_leader(node_id);
                return Ok(());
            }
            self.tracker.record_not_leader(body.leader_hint.map(|h| h as NodeId));
        }
        Err(ClientError::NoLeaderReachable)
    }

    pub async fn delete(&self, key: &str) -> Result<(), ClientError> {
        for (node_id, addr) in self.candidate_order() {
            let url = format!("http://{addr}/kv/{key}");
            let resp = match self.http.delete(&url).send().await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let body: WriteResponse = match resp.json().await {
                Ok(b) => b,
                Err(_) => continue,
            };

            if body.is_leader {
                self.tracker.record_confirmed_leader(node_id);
                return Ok(());
            }
            self.tracker.record_not_leader(body.leader_hint.map(|h| h as NodeId));
        }
        Err(ClientError::NoLeaderReachable)
    }

    /// Current best guess first (if any), then every other node as fallback -
    /// so a wrong guess costs at most one wasted round-trip before the client
    /// finds the real leader.
    fn candidate_order(&self) -> Vec<(NodeId, String)> {
        let mut order = Vec::with_capacity(self.tracker.node_count());
        let guess = self.tracker.best_guess();
        if let Some((id, addr)) = &guess {
            order.push((*id, addr.clone()));
        }
        order.extend(self.tracker.fallback_order(guess.map(|(id, _)| id)));
        order
    }
}
use anyhow::{bail, Context};
use std::collections::HashMap;
use std::process::Command as ProcessCommand;

/// Node registry: maps a node id to (process image name for taskkill, metrics
/// API port for health checks, raft TCP port for firewall rules). Hardcoded
/// to the 3-node local topology docker-compose (Phase 11) will also use -
/// intentionally simple rather than a general-purpose process manager, since
/// this CLI exists to demo ONE cluster, not manage arbitrary ones.
pub struct NodeInfo {
    pub metrics_port: u16,
    pub raft_port: u16,
}

pub fn node_registry() -> HashMap<u32, NodeInfo> {
    HashMap::from([
        (1, NodeInfo { metrics_port: 8001, raft_port: 7001 }),
        (2, NodeInfo { metrics_port: 8002, raft_port: 7002 }),
        (3, NodeInfo { metrics_port: 8003, raft_port: 7003 }),
    ])
}

/// Kills a node's OS process outright - the real "power loss / OOM kill /
/// host died" failure mode, not a simulation of one. Relies on each node
/// being started with a distinguishable window title (see docker-compose
/// setup / run scripts in Phase 11) OR, for local (non-Docker) testing,
/// on the caller supplying the actual PID via `--pid`.
pub fn kill_node(pid: u32) -> anyhow::Result<()> {
    let status = ProcessCommand::new("taskkill")
        .args(["/F", "/PID", &pid.to_string()])
        .status()
        .context("failed to invoke taskkill")?;
    if !status.success() {
        bail!("taskkill exited with {status} - is PID {pid} actually running?");
    }
    println!("Killed process {pid}. Watch the dashboard/logs for the remaining nodes to elect a new leader.");
    Ok(())
}

/// Blocks inbound/outbound traffic on a node's Raft TCP port using a Windows
/// Firewall rule - simulates a network partition (process alive, but
/// unreachable) without the node itself needing any awareness of chaos
/// testing. Requires an elevated (Administrator) terminal - `netsh advfirewall`
/// write operations fail silently otherwise.
pub fn partition_node(node_id: u32) -> anyhow::Result<()> {
    let registry = node_registry();
    let info = registry
        .get(&node_id)
        .ok_or_else(|| anyhow::anyhow!("unknown node id {node_id} - expected 1, 2, or 3"))?;

    let rule_name = format!("quorum-partition-node{node_id}");
    let status = ProcessCommand::new("netsh")
        .args([
            "advfirewall", "firewall", "add", "rule",
            &format!("name={rule_name}"),
            "dir=in", "action=block", "protocol=TCP",
            &format!("localport={}", info.raft_port),
        ])
        .status()
        .context("failed to invoke netsh - is this terminal running as Administrator?")?;
    if !status.success() {
        bail!("netsh exited with {status}");
    }
    println!(
        "Partitioned node {node_id} (blocked inbound TCP :{}). It can no longer send/receive Raft RPCs.",
        info.raft_port
    );
    Ok(())
}

/// Removes a partition rule previously added by `partition_node`, restoring
/// connectivity. The node process was never touched - this is purely a
/// network-layer change, so healing it should let the node rejoin the
/// cluster and catch up via normal AppendEntries replication, no restart needed.
pub fn heal_node(node_id: u32) -> anyhow::Result<()> {
    let rule_name = format!("quorum-partition-node{node_id}");
    let status = ProcessCommand::new("netsh")
        .args(["advfirewall", "firewall", "delete", "rule", &format!("name={rule_name}")])
        .status()
        .context("failed to invoke netsh - is this terminal running as Administrator?")?;
    if !status.success() {
        bail!("netsh exited with {status} - was node {node_id} actually partitioned?");
    }
    println!("Healed node {node_id}. It should reconnect and catch up on the next heartbeat.");
    Ok(())
}

/// Polls every known node's /health endpoint and prints a one-line cluster
/// status summary - the fastest way to see current term/role/commit_index
/// across the whole cluster without opening the dashboard.
pub fn status() -> anyhow::Result<()> {
    let registry = node_registry();
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()?;

    let mut ids: Vec<&u32> = registry.keys().collect();
    ids.sort();

    for &id in ids {
        let info = &registry[&id];
        let url = format!("http://127.0.0.1:{}/health", info.metrics_port);
        match client.get(&url).send() {
            Ok(resp) => match resp.json::<serde_json::Value>() {
                Ok(body) => println!(
                    "node {id}: role={} term={} commit_index={} last_applied={} log_len={}",
                    body["role"], body["current_term"], body["commit_index"],
                    body["last_applied"], body["log_len"]
                ),
                Err(_) => println!("node {id}: reachable but returned invalid JSON"),
            },
            Err(_) => println!("node {id}: UNREACHABLE"),
        }
    }
    Ok(())
}
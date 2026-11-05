//! The client SDK against a real cluster over real HTTP: what a caller actually
//! experiences, including a leader crashing and a leader being cut off mid-write.

mod common;

use client::{ClientConfig, ClientError, QuorumClient};
use common::TestCluster;
use raft_core::NodeId;
use std::collections::HashMap;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(10);

/// A cluster of three with each node's HTTP API served, and the address map a client needs.
async fn serve(cluster: &TestCluster, ids: &[NodeId], request_timeout: Duration, max_value: usize) -> HashMap<NodeId, String> {
    let mut map = HashMap::new();
    for &id in ids {
        let addr = cluster.serve_http_limited(id, request_timeout, max_value).await;
        map.insert(id, addr.to_string());
    }
    map
}

fn patient() -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_secs(5),
        max_attempts: 12,
        initial_backoff: Duration::from_millis(30),
        max_backoff: Duration::from_millis(300),
        retry_unknown_writes: false,
    }
}

#[tokio::test]
async fn binary_values_and_awkward_keys_round_trip_through_a_real_cluster() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    let nodes = serve(&cluster, &[1, 2, 3], Duration::from_secs(3), 1000).await;
    let client = QuorumClient::with_config(nodes, patient());

    // Every byte value, including the ones that are not valid UTF-8.
    let blob: Vec<u8> = (0..=255u8).collect();
    client.put_bytes("blob", &blob).await.unwrap();
    assert_eq!(client.get_bytes("blob").await.unwrap(), Some(blob.clone()));
    assert!(matches!(client.get("blob").await, Err(ClientError::NotUtf8)));

    // Text, the empty value, and a missing key are three different things.
    client.put("text", "héllo 世界").await.unwrap();
    assert_eq!(client.get("text").await.unwrap(), Some("héllo 世界".to_string()));
    client.put("empty", "").await.unwrap();
    assert_eq!(client.get("empty").await.unwrap(), Some(String::new()));
    assert_eq!(client.get("never-written").await.unwrap(), None);

    // Keys with characters that mean something in a URL each name a distinct key.
    let keys = ["a?b", "a/b", "a#b", "100%", "with space", "été", "鍵🔑", "a+b", "a", "...", "x/.."];
    for key in keys {
        client.put(key, &format!("value-of:{key}")).await.unwrap();
    }
    for key in keys {
        assert_eq!(client.get(key).await.unwrap(), Some(format!("value-of:{key}")), "{key:?}");
    }

    // Delete, and the limits.
    client.delete("text").await.unwrap();
    assert_eq!(client.get("text").await.unwrap(), None);
    client.put_bytes("at-limit", vec![1u8; 1000]).await.unwrap();
    match client.put_bytes("too-big", vec![1u8; 1001]).await.unwrap_err() {
        ClientError::Rejected { status: 413, message } => assert!(message.contains("too large"), "{message}"),
        other => panic!("{other}"),
    }
    assert_eq!(client.get("too-big").await.unwrap(), None, "nothing was stored");
    assert!(matches!(client.put("", "v").await, Err(ClientError::InvalidKey(_))));
}

#[tokio::test]
async fn the_sdk_keeps_working_when_the_leader_crashes() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let nodes = serve(&cluster, &[1, 2, 3], Duration::from_secs(3), 1 << 20).await;
    let client = QuorumClient::with_config(nodes, patient());

    for i in 0..20 {
        client.put(&format!("before-{i}"), &format!("v{i}")).await.unwrap();
    }
    cluster.crash(leader).await;

    // The first request after the crash may find no leader yet; the client waits it out.
    for i in 0..20 {
        assert_eq!(
            client.get(&format!("before-{i}")).await.unwrap(),
            Some(format!("v{i}")),
            "acknowledged writes survive the leader"
        );
    }
    for i in 0..20 {
        client.put(&format!("after-{i}"), &format!("w{i}")).await.unwrap();
    }
    assert_eq!(client.get("after-19").await.unwrap(), Some("w19".to_string()));
}

#[tokio::test]
async fn a_write_to_a_leader_that_is_cut_off_is_reported_unknown_and_not_resent() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader(SETTLE).await;
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != old_leader).collect();
    let nodes = serve(&cluster, &[1, 2, 3], Duration::from_secs(2), 1 << 20).await;
    let client = QuorumClient::with_config(nodes.clone(), patient());

    client.put("warmup", "1").await.unwrap(); // the client now believes `old_leader` leads
    cluster.isolate(old_leader);
    cluster.wait_for_leader_among(&others, SETTLE).await;

    // The write goes to the leader the client knew. It takes it, cannot commit it, and
    // loses leadership: it cannot say whether the write survived. The client says so,
    // and does not quietly send it to the new leader as well.
    let error = client.put("contested", "first").await.unwrap_err();
    assert!(matches!(error, ClientError::OutcomeUnknown { .. }), "{error}");

    cluster.heal();
    cluster.wait_until(SETTLE, "one leader again", |c| c.leaders().len() == 1).await;
    assert_eq!(
        client.get("contested").await.unwrap(),
        None,
        "it never left the isolated node, so it was lost: unknown was the honest answer"
    );

    // The caller can decide to try again, now that it knows.
    client.put("contested", "second").await.unwrap();
    assert_eq!(client.get("contested").await.unwrap(), Some("second".to_string()));
}

#[tokio::test]
async fn resending_unknown_writes_succeeds_when_the_caller_opts_in() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader(SETTLE).await;
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != old_leader).collect();
    let nodes = serve(&cluster, &[1, 2, 3], Duration::from_secs(2), 1 << 20).await;
    let client = QuorumClient::with_config(nodes, ClientConfig { retry_unknown_writes: true, ..patient() });

    client.put("warmup", "1").await.unwrap();
    cluster.isolate(old_leader);
    cluster.wait_for_leader_among(&others, SETTLE).await;

    // Same situation, but the caller has said that sending the write twice is harmless.
    client.put("idempotent", "v").await.unwrap();
    assert_eq!(client.get("idempotent").await.unwrap(), Some("v".to_string()));
}

#[tokio::test]
async fn membership_can_be_managed_through_the_sdk() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    let nodes = serve(&cluster, &[1, 2, 3], Duration::from_secs(3), 1 << 20).await;
    let client = QuorumClient::with_config(nodes, patient());
    client.put("k", "v").await.unwrap();

    cluster.start_joiner(4);
    assert_eq!(client.add_member(4, "node4:7000").await.unwrap(), vec![1, 2, 3, 4]);
    assert_eq!(client.members().await.unwrap(), vec![1, 2, 3, 4]);

    // A second add is a conflict the caller is told about, not retried into the ground.
    match client.add_member(4, "node4:7000").await.unwrap_err() {
        ClientError::Rejected { status: 409, message } => assert!(message.contains("already a member"), "{message}"),
        other => panic!("{other}"),
    }

    assert_eq!(client.remove_member(4).await.unwrap(), vec![1, 2, 3]);
    assert!(matches!(client.remove_member(4).await, Err(ClientError::Rejected { status: 409, .. })));
    assert_eq!(client.get("k").await.unwrap(), Some("v".to_string()));
}

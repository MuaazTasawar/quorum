//! Linearizable reads (ReadIndex) against real multi-node clusters.
//!
//! A read must never return a value older than a write the client has already been
//! told succeeded, even when the node answering it is a leader that has just been
//! cut off from the cluster or has only just been elected.

mod common;

use common::TestCluster;
use node::cluster::{ClientCommandResult, QueryResult};
use node::config::ReadMode;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A minimal HTTP/1.1 client over a raw socket (no extra dependencies).
/// Returns (status code, response body).
async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("server should accept connections");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).to_string();

    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response: {text:?}"));
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn applied(result: &ClientCommandResult) -> bool {
    matches!(result, ClientCommandResult::Applied(_))
}

#[tokio::test]
async fn a_read_returns_the_latest_committed_write() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;

    assert!(applied(&cluster.put(leader, "k", "v1").await));
    assert!(matches!(cluster.get(leader, "k").await, QueryResult::Value(Some(v)) if v == b"v1".to_vec()));

    assert!(applied(&cluster.put(leader, "k", "v2").await));
    assert!(matches!(cluster.get(leader, "k").await, QueryResult::Value(Some(v)) if v == b"v2".to_vec()));

    assert!(matches!(cluster.get(leader, "never-written").await, QueryResult::Value(None)));
}

/// A node that was elected a moment ago has not yet learned what the previous
/// leader committed. A read sent the instant it becomes leader must still see the
/// committed write: the read waits for the new leader's first commit instead of
/// answering from a state machine that has not caught up.
#[tokio::test]
async fn a_read_on_a_freshly_elected_leader_still_sees_earlier_committed_writes() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    assert!(applied(&cluster.put(old_leader, "critical", "committed").await));

    cluster.crash(old_leader).await;
    let new_leader = cluster.wait_for_leader(Duration::from_secs(5)).await;

    // No sleep: ask the very moment the new leader reports itself leader.
    match cluster.get(new_leader, "critical").await {
        QueryResult::Value(Some(v)) => assert_eq!(v, b"committed".to_vec()),
        other => panic!("a committed write must be visible on the new leader, got {other:?}"),
    }
}

/// Writes and reads interleaved across a leader failure: every read must return
/// at least the last write that was acknowledged, never an older value.
#[tokio::test]
async fn reads_never_go_backwards_across_a_leader_change() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let mut leader = cluster.wait_for_leader(Duration::from_secs(2)).await;

    for round in 1..=12u64 {
        if round == 6 {
            cluster.crash(leader).await;
            leader = cluster.wait_for_leader(Duration::from_secs(5)).await;
        }

        let result = cluster.put(leader, "counter", &round.to_string()).await;
        assert!(applied(&result), "round {round}: write should apply, got {result:?}");
        let last_acknowledged = round;

        match cluster.get(leader, "counter").await {
            QueryResult::Value(Some(bytes)) => {
                let seen: u64 = String::from_utf8(bytes).unwrap().parse().unwrap();
                assert!(
                    seen >= last_acknowledged,
                    "round {round}: read returned {seen} after {last_acknowledged} was acknowledged"
                );
            }
            other => panic!("round {round}: expected a value, got {other:?}"),
        }
    }
}

/// The scenario ReadIndex exists for. A leader that has been cut off from the
/// majority cannot prove it is still the leader, so it must not answer from its
/// own state (which a newer leader may already have overwritten). It answers
/// "not the leader" so the client tries elsewhere.
#[tokio::test]
async fn a_cut_off_leader_redirects_reads_instead_of_answering_from_stale_state() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    assert!(applied(&cluster.put(leader, "k", "old-value").await));
    // A deadline far beyond the test: any answer comes from the protocol, not a timer.
    let addr = cluster.serve_http(leader, Duration::from_secs(60)).await;

    // Sanity: while connected, the read returns the value.
    let (status, body) = http(addr, "GET", "/kv/k", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"value\":\"old-value\""), "{body}");

    cluster.isolate(leader);

    let started = Instant::now();
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), http(addr, "GET", "/kv/k", None))
        .await
        .expect("the read must be answered, not left hanging");

    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"is_leader\":false"), "must redirect, got {body}");
    assert!(!body.contains("old-value"), "must not leak possibly stale state: {body}");
    assert!(started.elapsed() < Duration::from_secs(3), "took {:?}", started.elapsed());
}

/// The opt-out: with `QUORUM_LOCAL_READS=true` the same cut-off leader keeps
/// answering from local state straight away. Faster, and exactly the behaviour
/// that can return stale data, which is why it is not the default.
#[tokio::test]
async fn local_read_mode_still_answers_from_a_cut_off_leader() {
    let cluster = TestCluster::start_with_read_mode(&[1, 2, 3], ReadMode::Local);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    assert!(applied(&cluster.put(leader, "k", "local").await));
    let addr = cluster.serve_http(leader, Duration::from_secs(60)).await;

    cluster.isolate(leader);

    let started = Instant::now();
    let (status, body) = http(addr, "GET", "/kv/k", None).await;

    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"value\":\"local\""), "{body}");
    assert!(body.contains("\"is_leader\":true"), "{body}");
    assert!(started.elapsed() < Duration::from_millis(200), "took {:?}", started.elapsed());
}
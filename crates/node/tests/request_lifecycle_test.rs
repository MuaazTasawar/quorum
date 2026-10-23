//! What a client actually experiences when a leader cannot finish its write.
//!
//! These tests drive the real HTTP API (a real Axum router on a real socket)
//! over a real multi-node cluster, and use partitions to put the leader in the
//! situations that used to leave a request hanging forever.

mod common;

use common::TestCluster;
use node::cluster::QueryResult;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A minimal HTTP/1.1 client over a raw socket, so the tests need no extra
/// dependencies. Returns (status code, response body).
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

const PUT_BODY: &str = r#"{"value":"x"}"#;

#[tokio::test]
async fn http_round_trip_against_a_healthy_cluster() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let follower = cluster.live_ids().into_iter().find(|&id| id != leader).unwrap();

    let leader_addr = cluster.serve_http(leader, Duration::from_secs(3)).await;
    let follower_addr = cluster.serve_http(follower, Duration::from_secs(3)).await;

    let (status, body) = http(leader_addr, "PUT", "/kv/greeting", Some(r#"{"value":"hello"}"#)).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"is_leader\":true") && body.contains("\"applied\":true"), "{body}");

    let (status, body) = http(leader_addr, "GET", "/kv/greeting", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"value\":\"hello\""), "{body}");

    // A follower still answers with a redirect hint, not an error.
    let (status, body) = http(follower_addr, "PUT", "/kv/greeting", Some(PUT_BODY)).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"is_leader\":false"), "{body}");
}

/// The case that used to hang: the leader accepts a write, is cut off, and the
/// rest of the cluster moves on without it. The old leader loses leadership (it
/// notices it has no majority, or hears of the newer term when the partition
/// heals) and the client must be told right away, long before any deadline,
/// rather than being left waiting on an entry that will never commit.
#[tokio::test]
async fn a_write_waiting_on_a_deposed_leader_is_failed_promptly() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let others: Vec<u32> = cluster.live_ids().into_iter().filter(|&id| id != old_leader).collect();

    // A deadline far longer than the test allows: if the request is answered at
    // all, it is because leadership was lost, not because a timer fired.
    let addr = cluster.serve_http(old_leader, Duration::from_secs(60)).await;

    cluster.isolate(old_leader);
    let write = tokio::spawn(http(addr, "PUT", "/kv/doomed", Some(PUT_BODY)));

    // Let the isolated leader append the entry and fail to replicate it.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(!write.is_finished(), "the write cannot complete without a majority");

    let new_leader = cluster.wait_for_leader_among(&others, Duration::from_secs(3)).await;
    cluster.heal();

    let (status, body) = tokio::time::timeout(Duration::from_secs(5), write)
        .await
        .expect("the client must be answered once leadership is lost, not left hanging")
        .unwrap();
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("\"outcome_unknown\":true"), "{body}");

    // The honest part: the write really was lost, because it never left the
    // isolated leader. "Unknown" is the most a client can be told at that moment.
    cluster
        .wait_until(Duration::from_secs(5), "exactly one leader after healing", |c| {
            c.leaders().len() == 1
        })
        .await;
    let leader = cluster.leaders()[0];
    assert!(matches!(cluster.get(leader, "doomed").await, QueryResult::Value(None)));
    let _ = new_leader;
}

/// A deadline shorter than the leader's own check-quorum window (an election
/// timeout, 300 ms) must still produce an answer: a 504 at the deadline, not
/// silence. The deadline is set well inside that window so this test exercises
/// the timeout path rather than racing the step-down.
#[tokio::test]
async fn a_write_that_cannot_finish_before_its_deadline_times_out_with_504() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let addr = cluster.serve_http(leader, Duration::from_millis(100)).await;

    cluster.isolate(leader);

    let started = Instant::now();
    let (status, body) = http(addr, "PUT", "/kv/stuck", Some(PUT_BODY)).await;
    let elapsed = started.elapsed();

    assert_eq!(status, 504, "{body}");
    assert!(body.contains("\"outcome_unknown\":true"), "{body}");
    assert!(
        elapsed >= Duration::from_millis(80) && elapsed < Duration::from_secs(2),
        "should answer at the deadline, took {elapsed:?}"
    );
}

/// The other half of the picture: with a generous deadline, a leader that has been
/// cut off from the majority does not make the client wait for it. Check-quorum
/// makes the leader give up on its own, and the waiting write is failed within an
/// election timeout or so, not after 60 seconds.
#[tokio::test]
async fn a_leader_cut_off_from_the_majority_fails_waiting_writes_quickly() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let addr = cluster.serve_http(leader, Duration::from_secs(60)).await;

    cluster.isolate(leader);

    let started = Instant::now();
    let (status, body) = tokio::time::timeout(
        Duration::from_secs(5),
        http(addr, "PUT", "/kv/stuck", Some(PUT_BODY)),
    )
    .await
    .expect("check-quorum must free the client long before the 60 s deadline");
    let elapsed = started.elapsed();

    assert_eq!(status, 503, "{body}");
    assert!(body.contains("\"outcome_unknown\":true"), "{body}");
    assert!(elapsed < Duration::from_secs(2), "took {elapsed:?}");
}

/// A node that is cut off from the cluster must still answer its own `/health`
/// endpoint, because that is how an operator finds out what is wrong. Here that
/// answer shows the leader noticing it has no majority and stepping down.
#[tokio::test]
async fn health_stays_available_and_shows_a_cut_off_leader_stepping_down() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let addr = cluster.serve_http(leader, Duration::from_millis(300)).await;

    let (status, body) = http(addr, "GET", "/health", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"role\":\"Leader\""), "{body}");

    cluster.isolate(leader);
    cluster
        .wait_until(Duration::from_secs(3), "the cut-off leader to step down", |c| {
            c.snapshot(leader).role != "Leader"
        })
        .await;

    let (status, body) = http(addr, "GET", "/health", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"role\":\"Follower\""), "{body}");
}
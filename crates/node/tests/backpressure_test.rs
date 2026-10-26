//! Backpressure and group commit against real nodes.
//!
//! A node under more load than it can carry must say so, promptly, instead of
//! queueing without limit; and requests that arrive together must share the cost of
//! a disk sync instead of each paying for one.

mod common;

use common::TestCluster;
use node::cluster::{
    ClientCommand, ClientCommandResult, ClientQuery, ClusterSnapshot,
};
use node::config::Limits;
use node::metrics_api::{router, AppState};
use std::net::SocketAddr;
use std::time::Duration;
use storage::Wal;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};

/// Minimal HTTP/1.1 client over a raw socket. Returns (status, the whole raw
/// response) so tests can inspect headers as well as the body.
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
    (status, text)
}

const PUT_BODY: &str = r#"{"value":"x"}"#;

fn idle_snapshot() -> ClusterSnapshot {
    ClusterSnapshot {
        node_id: 1,
        role: "Leader".to_string(),
        current_term: 1,
        commit_index: 0,
        last_applied: 0,
        log_len: 0,
        leader_hint: Some(1),
        wal_syncs: 0,
        dropped_messages: 0,
    }
}

/// An API whose cluster never answers and whose queues hold ONE request each, so
/// "full" is easy to reach. Returns the receiving ends so a test can drain them.
async fn stuck_api(
    timeout: Duration,
) -> (SocketAddr, mpsc::Receiver<ClientCommand>, mpsc::Receiver<ClientQuery>) {
    let (command_tx, command_rx) = mpsc::channel(1);
    let (query_tx, query_rx) = mpsc::channel(1);
    let (snapshot_tx, snapshot_rx) = watch::channel(idle_snapshot());
    std::mem::forget(snapshot_tx); // keep the snapshot channel open for the whole test
    let state = AppState { command_tx, query_tx, snapshot_rx, request_timeout: timeout };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    });
    (addr, command_rx, query_rx)
}

#[tokio::test]
async fn a_full_write_queue_is_turned_away_with_503_and_retry_after() {
    let (addr, mut command_rx, _query_rx) = stuck_api(Duration::from_millis(600)).await;

    // The first write takes the only slot and waits for an answer nobody gives.
    let first = tokio::spawn(http(addr, "PUT", "/kv/a", Some(PUT_BODY)));
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The second finds the queue full and is refused straight away.
    let started = std::time::Instant::now();
    let (status, raw) = http(addr, "PUT", "/kv/b", Some(PUT_BODY)).await;
    assert_eq!(status, 503, "{raw}");
    assert!(raw.to_lowercase().contains("retry-after: 1"), "must tell the client to back off: {raw}");
    assert!(raw.contains("\"outcome_unknown\":false"), "a refused request took no effect: {raw}");
    assert!(started.elapsed() < Duration::from_millis(300), "refusal must be immediate");

    // The first one eventually times out, as before.
    let (status, _) = first.await.unwrap();
    assert_eq!(status, 504);

    // Once the queue is drained there is room again: the next request is accepted
    // into the queue (and then times out like the first), not refused.
    assert!(command_rx.recv().await.is_some());
    let (status, raw) = http(addr, "PUT", "/kv/c", Some(PUT_BODY)).await;
    assert_eq!(status, 504, "accepted this time, so it waits for an answer: {raw}");
}

#[tokio::test]
async fn a_full_read_queue_is_turned_away_with_503_and_retry_after() {
    let (addr, _command_rx, _query_rx) = stuck_api(Duration::from_millis(600)).await;

    let first = tokio::spawn(http(addr, "GET", "/kv/a", None));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (status, raw) = http(addr, "GET", "/kv/b", None).await;
    assert_eq!(status, 503, "{raw}");
    assert!(raw.to_lowercase().contains("retry-after: 1"), "{raw}");

    let (status, _) = first.await.unwrap();
    assert_eq!(status, 504);
}

#[tokio::test]
async fn health_is_not_affected_by_a_full_request_queue() {
    let (addr, _command_rx, _query_rx) = stuck_api(Duration::from_millis(600)).await;
    let _blocker = tokio::spawn(http(addr, "PUT", "/kv/a", Some(PUT_BODY)));
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (status, raw) = http(addr, "GET", "/health", None).await;

    assert_eq!(status, 200, "an operator must still be able to see what is wrong: {raw}");
    assert!(raw.contains("\"wal_syncs\":0") && raw.contains("\"dropped_messages\":0"), "{raw}");
}

/// Group commit under real load: hundreds of writes queued together are made
/// durable with a handful of fsyncs, not one each.
#[tokio::test]
async fn many_queued_writes_share_a_few_disk_syncs() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    // Let the election's own writes settle so they are not counted below.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let syncs_before = cluster.snapshot(leader).wal_syncs;

    const WRITES: usize = 300;
    let results = cluster.put_many(leader, WRITES).await;

    assert_eq!(results.len(), WRITES);
    assert!(
        results.iter().all(|r| matches!(r, ClientCommandResult::Applied(_))),
        "every queued write must still be applied"
    );
    let syncs = cluster.snapshot(leader).wal_syncs - syncs_before;
    assert!(
        syncs * 10 <= WRITES as u64,
        "{WRITES} writes should need far fewer than {WRITES} fsyncs, but took {syncs}"
    );

    // Nothing was lost on the way: the last write is readable.
    match cluster.get(leader, "key-299").await {
        node::cluster::QueryResult::Value(Some(v)) => assert_eq!(v, b"value".to_vec()),
        other => panic!("expected the last write to be readable, got {other:?}"),
    }
}

/// A leader that cannot commit (cut off from its followers) must not accept an
/// unbounded pile of writes. Past its limit it refuses them at once, and the ones
/// it did accept are failed when it steps down.
#[tokio::test]
async fn a_leader_that_cannot_commit_refuses_writes_beyond_its_limit() {
    let cluster = TestCluster::start_with_limits(
        &[1, 2, 3],
        Limits { channel_capacity: 4, max_batch: 128 },
    );
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    cluster.isolate(leader);

    let mut receivers = Vec::new();
    for i in 0..12 {
        receivers.push(cluster.submit(leader, &format!("k{i}"), "v").await);
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut refused = 0;
    let mut waiting = Vec::new();
    for mut receiver in receivers {
        match receiver.try_recv() {
            Ok(ClientCommandResult::Overloaded) => refused += 1,
            Ok(other) => panic!("unexpected immediate answer: {other:?}"),
            Err(_) => waiting.push(receiver),
        }
    }
    assert_eq!(waiting.len(), 4, "exactly the limit is held");
    assert_eq!(refused, 8, "the rest are refused at once, not queued");

    // The held writes are failed (outcome unknown) once the cut-off leader steps down.
    for receiver in waiting {
        let result = tokio::time::timeout(Duration::from_secs(3), receiver)
            .await
            .expect("held writes must be answered when leadership is lost")
            .unwrap();
        assert!(matches!(result, ClientCommandResult::LeadershipLost), "got {result:?}");
    }

    // And the cluster recovers: after the partition heals, writes flow again.
    cluster.heal();
    cluster
        .wait_until(Duration::from_secs(5), "a single leader after healing", |c| {
            c.leaders().len() == 1
        })
        .await;
    let leader = cluster.leaders()[0];
    assert!(matches!(
        cluster.put(leader, "after", "healing").await,
        ClientCommandResult::Applied(_)
    ));
}

/// A node killed in the middle of appending to its log must come back, not refuse
/// to start. The unfinished write was never acknowledged, so dropping it loses
/// nothing that was promised.
#[tokio::test]
async fn a_node_that_died_mid_write_restarts_and_catches_up() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let follower = cluster.live_ids().into_iter().find(|&id| id != leader).unwrap();
    for i in 0..5 {
        assert!(matches!(
            cluster.put(leader, &format!("k{i}"), "v").await,
            ClientCommandResult::Applied(_)
        ));
    }
    cluster
        .wait_until(Duration::from_secs(5), "follower to replicate everything", |c| {
            c.snapshot(follower).log_len == c.snapshot(leader).log_len
        })
        .await;
    let log_len = cluster.snapshot(leader).log_len;

    cluster.crash(follower).await;

    // Simulate the process dying half-way through writing the next record: a length
    // prefix promising 64 bytes, followed by only a few of them.
    {
        use std::io::Write;
        let mut torn = Vec::new();
        torn.extend_from_slice(&64u32.to_le_bytes());
        torn.extend_from_slice(&[0xAB; 9]);
        std::fs::OpenOptions::new()
            .append(true)
            .open(cluster.wal_path(follower))
            .unwrap()
            .write_all(&torn)
            .unwrap();
    }
    assert!(Wal::replay(cluster.wal_path(follower)).is_err(), "the log really is torn");

    cluster.restart(follower).await;

    // It started, recovered every complete record, and the log is clean again.
    assert_eq!(cluster.snapshot(follower).log_len, log_len, "all complete records recovered");
    assert_eq!(Wal::replay(cluster.wal_path(follower)).unwrap().len() as u64, log_len);

    // And it rejoins normally.
    assert!(matches!(
        cluster.put(leader, "after-restart", "ok").await,
        ClientCommandResult::Applied(_)
    ));
    cluster
        .wait_until(Duration::from_secs(5), "restarted follower to catch up", |c| {
            c.snapshot(follower).log_len == c.snapshot(leader).log_len
        })
        .await;
}
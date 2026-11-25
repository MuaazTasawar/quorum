//! API authentication against real clusters over real HTTP: who is let in, who is turned away,
//! and that a turned-away request has no effect at all.

mod common;

use client::{ClientError, QuorumClient};
use common::TestCluster;
use node::auth::{Auth, Role};
use serde_json::Value;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SETTLE: Duration = Duration::from_secs(10);
const READ: &str = "read-token-0123456789abcdef";
const WRITE: &str = "write-token-0123456789abcdef";
const ADMIN: &str = "admin-token-0123456789abcdef";
fn octets() -> (&'static str, String) {
    ("Content-Type", "application/octet-stream".into())
}

fn auth() -> Auth {
    Auth::from_entries(vec![
        (Role::Read, READ.into()),
        (Role::Write, WRITE.into()),
        (Role::Admin, ADMIN.into()),
    ])
    .unwrap()
}

fn bearer(token: &str) -> (&'static str, String) {
    ("Authorization", format!("Bearer {token}"))
}

struct Reply {
    status: u16,
    headers: String,
    body: Vec<u8>,
}

impl Reply {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.text()))
    }
    fn header(&self, name: &str) -> Option<String> {
        self.headers.lines().skip(1).find_map(|line| {
            let (n, v) = line.split_once(':')?;
            n.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_string())
        })
    }
}

/// Sends one request. A server that answers before it has read a large body may close the
/// connection while the body is still being sent, so a failed write is not an error here: the
/// answer is what matters.
async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: &[u8],
) -> Reply {
    let mut stream = TcpStream::connect(addr).await.expect("server accepts");
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body).await;
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw).await;
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .unwrap_or_else(|| panic!("no complete response: {:?}", String::from_utf8_lossy(&raw)));
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut body = raw[split + 4..].to_vec();
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body);
    }
    Reply {
        status,
        headers: head,
        body,
    }
}

fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let Some(line_end) = data.windows(2).position(|w| w == b"\r\n") else {
            return out;
        };
        let size =
            usize::from_str_radix(std::str::from_utf8(&data[..line_end]).unwrap().trim(), 16)
                .unwrap();
        data = &data[line_end + 2..];
        if size == 0 || data.len() < size {
            return out;
        }
        out.extend_from_slice(&data[..size]);
        data = &data[(size + 2).min(data.len())..];
    }
}

/// A started 3-node cluster with its leader serving HTTP under `auth`.
async fn leader_with(auth: Auth) -> (TestCluster, SocketAddr) {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let addr = cluster
        .serve_http_with_auth(leader, Duration::from_secs(3), auth)
        .await;
    (cluster, addr)
}

// ---- tests ---------------------------------------------------------------------

#[tokio::test]
async fn without_tokens_everything_is_open_as_before() {
    let (_cluster, addr) = leader_with(Auth::disabled()).await;
    let put = request(addr, "PUT", "/kv/a", &[octets()], b"1").await;
    assert_eq!(put.status, 200, "{}", put.text());
    assert_eq!(put.json()["applied"], true);
    let get = request(addr, "GET", "/kv/a", &[], b"").await;
    assert_eq!(get.json()["found"], true);
    // The admin API is reachable: a bad body is a 4xx from the handler, not a 401 or 403.
    let admin = request(addr, "POST", "/admin/members", &[], b"{}").await;
    assert!(
        admin.status != 401 && admin.status != 403,
        "{}",
        admin.status
    );
    let del = request(addr, "DELETE", "/kv/a", &[], b"").await;
    assert_eq!(del.status, 200);
}

#[tokio::test]
async fn every_protected_route_turns_away_a_request_without_a_token() {
    let (_cluster, addr) = leader_with(auth()).await;
    let calls: [(&str, &str, &[u8]); 5] = [
        ("GET", "/kv/a", b""),
        ("PUT", "/kv/a", b"1"),
        ("DELETE", "/kv/a", b""),
        (
            "POST",
            "/admin/members",
            b"{\"id\":9,\"addr\":\"127.0.0.1:1\"}",
        ),
        ("DELETE", "/admin/members/9", b""),
    ];
    for (method, path, body) in calls {
        let reply = request(addr, method, path, &[octets()], body).await;
        assert_eq!(reply.status, 401, "{method} {path}: {}", reply.text());
        assert_eq!(
            reply.header("www-authenticate").as_deref(),
            Some("Bearer realm=\"quorum\""),
            "{method} {path}"
        );
        let json = reply.json();
        assert_eq!(json["error"], "missing or invalid API token");
        assert_eq!(json["outcome_unknown"], false);
    }
}

#[tokio::test]
async fn the_open_routes_need_no_token_and_an_unknown_path_is_still_a_404() {
    let (_cluster, addr) = leader_with(auth()).await;
    for path in ["/health", "/metrics", "/dashboard"] {
        let reply = request(addr, "GET", path, &[], b"").await;
        assert_eq!(reply.status, 200, "{path}");
    }
    let reply = request(addr, "GET", "/no/such/route", &[], b"").await;
    assert_eq!(
        reply.status, 404,
        "an unknown path reveals nothing either way"
    );
}

#[tokio::test]
async fn a_wrong_or_malformed_token_gets_exactly_the_answer_of_no_token() {
    let (_cluster, addr) = leader_with(auth()).await;
    let none = request(addr, "GET", "/kv/a", &[], b"").await;
    assert_eq!(none.status, 401);
    let attempts: Vec<Vec<(&str, String)>> = vec![
        vec![bearer("wrong-token-0123456789abcdef")],
        vec![bearer(&READ[..READ.len() - 1])],
        vec![bearer(&format!("{READ}x"))],
        vec![("Authorization", "Basic dXNlcjpwYXNzd29yZA==".into())],
        vec![("Authorization", "Bearer".into())],
        vec![("Authorization", format!("Bearer {READ} {READ}"))],
        vec![("Authorization", READ.into())],
        // Two headers, one of them valid: neither counts.
        vec![bearer(READ), bearer(READ)],
    ];
    for headers in attempts {
        let reply = request(addr, "GET", "/kv/a", &headers, b"").await;
        assert_eq!(reply.status, 401, "{headers:?}");
        assert_eq!(
            reply.body, none.body,
            "{headers:?}: the body must not differ"
        );
        assert_eq!(
            reply.header("www-authenticate"),
            none.header("www-authenticate"),
            "{headers:?}"
        );
    }
}

#[tokio::test]
async fn a_token_works_exactly_as_far_as_its_role_goes() {
    let (_cluster, addr) = leader_with(auth()).await;

    // read: may read, nothing else.
    let h = [bearer(READ), octets()];
    assert_eq!(request(addr, "GET", "/kv/a", &h, b"").await.status, 200);
    for (method, path) in [("PUT", "/kv/a"), ("DELETE", "/kv/a")] {
        let r = request(addr, method, path, &h, b"1").await;
        assert_eq!(r.status, 403, "{method} {path}");
        assert!(r.json()["error"].as_str().unwrap().contains("write role"));
        assert_eq!(r.json()["outcome_unknown"], false);
    }
    for (method, path) in [("POST", "/admin/members"), ("DELETE", "/admin/members/9")] {
        let r = request(addr, method, path, &h, b"{}").await;
        assert_eq!(r.status, 403, "{method} {path}");
        assert!(r.json()["error"].as_str().unwrap().contains("admin role"));
        assert!(
            r.header("www-authenticate").is_none(),
            "403 is not a challenge"
        );
    }

    // write: may read, write and delete, but not reconfigure.
    let h = [bearer(WRITE), octets()];
    let put = request(addr, "PUT", "/kv/a", &h, b"1").await;
    assert_eq!(
        (put.status, put.json()["applied"].clone()),
        (200, true.into())
    );
    assert_eq!(
        request(addr, "GET", "/kv/a", &h, b"").await.json()["found"],
        true
    );
    assert_eq!(request(addr, "DELETE", "/kv/a", &h, b"").await.status, 200);
    let r = request(addr, "POST", "/admin/members", &h, b"{}").await;
    assert_eq!(r.status, 403);

    // admin: gets past the check everywhere (a bad body is then the handler's 4xx).
    let h = [bearer(ADMIN), octets()];
    assert_eq!(request(addr, "PUT", "/kv/b", &h, b"2").await.status, 200);
    assert_eq!(request(addr, "GET", "/kv/b", &h, b"").await.status, 200);
    let r = request(addr, "POST", "/admin/members", &h, b"{}").await;
    assert!(r.status != 401 && r.status != 403, "{}", r.status);
}

#[tokio::test]
async fn a_refused_write_or_delete_changes_nothing() {
    let (_cluster, addr) = leader_with(auth()).await;
    let writer = [bearer(WRITE), octets()];
    let reader = [bearer(READ)];
    assert_eq!(
        request(addr, "PUT", "/kv/k", &writer, b"keep").await.status,
        200
    );

    // No token, and a token that may only read: both turned away.
    let no_token = request(addr, "PUT", "/kv/k", &[octets()], b"overwritten").await;
    assert_eq!(no_token.status, 401);
    let read_only = [bearer(READ), octets()];
    assert_eq!(
        request(addr, "PUT", "/kv/k", &read_only, b"overwritten")
            .await
            .status,
        403
    );
    assert_eq!(
        request(addr, "DELETE", "/kv/k", &reader, b"").await.status,
        403
    );
    assert_eq!(
        request(addr, "PUT", "/kv/new", &read_only, b"x")
            .await
            .status,
        403
    );

    // Still exactly as the writer left it, and the refused key never appeared.
    let kept = request(
        addr,
        "GET",
        "/kv/k",
        &[bearer(READ), ("Accept", "application/octet-stream".into())],
        b"",
    )
    .await;
    assert_eq!((kept.status, kept.text()), (200, "keep".to_string()));
    let new = request(addr, "GET", "/kv/new", &reader, b"").await;
    assert_eq!(new.json()["found"], false);
}

#[tokio::test]
async fn a_request_is_refused_before_its_body_is_read() {
    // A tiny value limit, so the body is over the limit without being large to send.
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let addr = cluster
        .serve_http_full(leader, Duration::from_secs(3), 1024, auth())
        .await;
    let body = vec![b'x'; 64 * 1024];

    // Without a token the answer is 401: the size is never looked at.
    let refused = request(addr, "PUT", "/kv/big", &[octets()], &body).await;
    assert_eq!(refused.status, 401);
    // With a token the same body gets as far as the size check: 413. So it was the missing
    // token, and nothing else, that decided the first answer.
    let allowed = request(addr, "PUT", "/kv/big", &[bearer(WRITE), octets()], &body).await;
    assert_eq!(allowed.status, 413, "{}", allowed.text());
}

#[tokio::test]
async fn protecting_observability_closes_health_metrics_and_the_stream() {
    let (_cluster, addr) = leader_with(auth().with_protected_observability(true)).await;
    for path in ["/health", "/metrics", "/ws"] {
        let r = request(addr, "GET", path, &[], b"").await;
        assert_eq!(r.status, 401, "{path} without a token");
        let r = request(
            addr,
            "GET",
            path,
            &[bearer("wrong-token-0123456789abcdef")],
            b"",
        )
        .await;
        assert_eq!(r.status, 401, "{path} with a wrong token");
    }
    // A read token opens them (the /ws one then fails only for lack of an upgrade).
    let h = [bearer(READ)];
    assert_eq!(request(addr, "GET", "/health", &h, b"").await.status, 200);
    let metrics = request(addr, "GET", "/metrics", &h, b"").await;
    assert_eq!(metrics.status, 200);
    assert!(metrics.text().contains("quorum_is_leader"));
    let ws = request(addr, "GET", "/ws", &h, b"").await;
    assert!(ws.status != 401 && ws.status != 403, "{}", ws.status);
    // The page itself holds no data and stays open.
    assert_eq!(
        request(addr, "GET", "/dashboard", &[], b"").await.status,
        200
    );
}

#[tokio::test]
async fn refusals_are_counted_by_route_and_no_secret_or_key_reaches_the_metrics() {
    let (_cluster, addr) = leader_with(auth()).await;
    for i in 0..5 {
        let key = format!("/kv/secretkey-{i}");
        request(addr, "PUT", &key, &[octets()], b"x").await;
        request(addr, "PUT", &key, &[bearer(READ), octets()], b"x").await;
        request(
            addr,
            "GET",
            &key,
            &[bearer("wrong-token-0123456789abcdef")],
            b"",
        )
        .await;
    }
    let metrics = request(addr, "GET", "/metrics", &[], b"").await.text();
    let series = |method: &str, code: &str| -> f64 {
        let prefix = format!(
            "quorum_http_requests_total{{route=\"/kv/{{key}}\",method=\"{method}\",code=\"{code}\"}} "
        );
        metrics
            .lines()
            .find_map(|l| l.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("no series {prefix}"))
            .parse()
            .unwrap()
    };
    assert_eq!(series("PUT", "401"), 5.0);
    assert_eq!(series("PUT", "403"), 5.0);
    assert_eq!(series("GET", "401"), 5.0);
    for secret in [READ, WRITE, ADMIN, "wrong-token", "secretkey"] {
        assert!(!metrics.contains(secret), "the metrics repeat {secret}");
    }
}

#[tokio::test]
async fn the_sdk_sends_its_token_and_never_retries_a_refusal() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    let mut nodes = HashMap::new();
    for id in cluster.live_ids() {
        let addr = cluster
            .serve_http_with_auth(id, Duration::from_secs(3), auth())
            .await;
        nodes.insert(id, addr.to_string());
    }
    let client = |token: Option<&str>| {
        let client = QuorumClient::new(nodes.clone());
        match token {
            Some(t) => client.with_token(t).unwrap(),
            None => client,
        }
    };
    let rejected = |result: Result<(), ClientError>, expected: u16| match result {
        Err(ClientError::Rejected { status, .. }) => assert_eq!(status, expected),
        other => panic!("expected a {expected} rejection, got {other:?}"),
    };

    // No token: refused outright.
    rejected(client(None).put("k", "v").await, 401);
    // A read token reads, and is refused a write.
    let reader = client(Some(READ));
    assert_eq!(reader.get("k").await.unwrap(), None);
    rejected(reader.put("k", "v").await, 403);
    // A write token writes and reads back, and may not reconfigure.
    let writer = client(Some(WRITE));
    writer.put("k", "v").await.unwrap();
    assert_eq!(writer.get("k").await.unwrap().as_deref(), Some("v"));
    assert!(matches!(
        writer.add_member(9, "127.0.0.1:1").await,
        Err(ClientError::Rejected { status: 403, .. })
    ));
    // An admin token can ask the cluster who its members are.
    assert_eq!(client(Some(ADMIN)).members().await.unwrap().len(), 3);
    // A token that cannot be sent is refused before anything is.
    assert!(matches!(
        QuorumClient::new(nodes.clone()).with_token("has a space"),
        Err(ClientError::InvalidToken)
    ));

    // A refusal is final: exactly one request reached the cluster for the one refused write,
    // not one per node and attempt. (Counted on /metrics, which is open.)
    let mut refused_puts = 0.0;
    for addr in nodes.values() {
        let metrics = request(addr.parse().unwrap(), "GET", "/metrics", &[], b"")
            .await
            .text();
        for line in metrics.lines() {
            if line.starts_with(
                "quorum_http_requests_total{route=\"/kv/{key}\",method=\"PUT\",code=\"401\"}",
            ) {
                refused_puts += line.rsplit(' ').next().unwrap().parse::<f64>().unwrap();
            }
        }
    }
    assert_eq!(refused_puts, 1.0, "the client retried a refused request");
}

#[tokio::test]
async fn the_sdk_carries_its_token_to_health_when_health_is_protected() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    let mut nodes = HashMap::new();
    for id in cluster.live_ids() {
        let addr = cluster
            .serve_http_with_auth(
                id,
                Duration::from_secs(3),
                auth().with_protected_observability(true),
            )
            .await;
        nodes.insert(id, addr.to_string());
    }
    let with = QuorumClient::new(nodes.clone()).with_token(READ).unwrap();
    assert_eq!(with.members().await.unwrap().len(), 3);
    // Without the token every node answers 401, so no member list can be had.
    assert!(QuorumClient::new(nodes).members().await.is_err());
}

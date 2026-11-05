//! How the client behaves against nodes that answer in every way a node can, using
//! scripted servers so each case is exact and fast: what was sent, to whom, how many
//! times, and what the caller is told.

use client::{ClientConfig, ClientError, QuorumClient};
use raft_core::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.iter().find(|(n, _)| n.eq_ignore_ascii_case(name)).map(|(_, v)| v.as_str())
    }
}

#[derive(Clone)]
enum Reply {
    Json { status: u16, headers: Vec<(&'static str, &'static str)>, body: &'static str },
    Bytes(Vec<u8>),
    /// Accepts the request and never answers.
    Hang,
}

fn json(status: u16, body: &'static str) -> Reply {
    Reply::Json { status, headers: vec![], body }
}

fn busy() -> Reply {
    Reply::Json {
        status: 503,
        headers: vec![("Retry-After", "1")],
        body: r#"{"error":"overloaded","outcome_unknown":false}"#,
    }
}

fn unknown() -> Reply {
    json(503, r#"{"error":"lost leadership","outcome_unknown":true}"#)
}

const APPLIED: &str = r#"{"is_leader":true,"applied":true,"leader_hint":null}"#;

fn ok() -> Reply {
    json(200, APPLIED)
}

fn redirect_to(id: u32) -> Reply {
    Reply::Json {
        status: 200,
        headers: vec![],
        body: match id {
            2 => r#"{"is_leader":false,"applied":false,"leader_hint":2}"#,
            _ => r#"{"is_leader":false,"applied":false,"leader_hint":null}"#,
        },
    }
}

struct Node {
    addr: SocketAddr,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Node {
    fn requests(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
    fn count(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

/// A node that answers its n-th request with `script[n]`, and every request after the
/// script ends with its last reply.
async fn node(script: Vec<Reply>) -> Node {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let counter = Arc::new(AtomicUsize::new(0));
    let (seen2, script) = (seen.clone(), Arc::new(script));
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let (seen, counter, script) = (seen2.clone(), counter.clone(), script.clone());
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let head_end = loop {
                    let n = stream.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        break p;
                    }
                };
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let mut lines = head.lines();
                let mut first = lines.next().unwrap().split_whitespace();
                let (method, path) = (first.next().unwrap().to_string(), first.next().unwrap().to_string());
                let headers: Vec<(String, String)> = lines
                    .filter_map(|l| l.split_once(':'))
                    .map(|(n, v)| (n.trim().to_string(), v.trim().to_string()))
                    .collect();
                let length = headers
                    .iter()
                    .find(|(n, _)| n.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut body = buf[head_end + 4..].to_vec();
                while body.len() < length {
                    let n = stream.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    body.extend_from_slice(&chunk[..n]);
                }
                seen.lock().unwrap().push(Seen { method, path, headers, body });

                let index = counter.fetch_add(1, Ordering::SeqCst).min(script.len() - 1);
                match &script[index] {
                    Reply::Hang => tokio::time::sleep(Duration::from_secs(30)).await,
                    Reply::Bytes(data) => respond(&mut stream, 200, "application/octet-stream", &[], data).await,
                    Reply::Json { status, headers, body } => {
                        respond(&mut stream, *status, "application/json", headers, body.as_bytes()).await
                    }
                }
            });
        }
    });
    Node { addr, seen }
}

async fn respond(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) {
    let mut head = format!(
        "HTTP/1.1 {status} X\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(body).await;
}

/// An address nothing is listening on: connecting fails at once.
async fn dead_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    drop(listener);
    addr
}

fn fast() -> ClientConfig {
    ClientConfig {
        request_timeout: Duration::from_secs(3),
        max_attempts: 4,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(5),
        retry_unknown_writes: false,
    }
}

fn cluster(nodes: &[&Node], config: ClientConfig) -> QuorumClient {
    let map: HashMap<NodeId, String> =
        nodes.iter().enumerate().map(|(i, n)| (i as NodeId + 1, n.addr.to_string())).collect();
    QuorumClient::with_config(map, config)
}

// ---- what is sent ---------------------------------------------------------------

#[tokio::test]
async fn keys_are_percent_encoded_so_each_names_exactly_one_key() {
    let n = node(vec![ok()]).await;
    let client = cluster(&[&n], fast());
    for key in ["a?b", "a/b", "a#b", "100%", "with space", "été", "鍵", "a+b", "../escape", "...", "a/..", "%2e%2e"] {
        client.put(key, "v").await.unwrap();
    }
    let paths: Vec<String> = n.requests().into_iter().map(|r| r.path).collect();
    assert_eq!(
        paths,
        vec![
            "/kv/a%3Fb", "/kv/a%2Fb", "/kv/a%23b", "/kv/100%25", "/kv/with%20space",
            "/kv/%C3%A9t%C3%A9", "/kv/%E9%8D%B5", "/kv/a+b", "/kv/..%2Fescape", "/kv/...",
            "/kv/a%2F..", "/kv/%252e%252e",
        ]
    );
}

#[tokio::test]
async fn a_write_sends_the_exact_bytes_as_octet_stream() {
    let n = node(vec![ok()]).await;
    let client = cluster(&[&n], fast());
    let value: Vec<u8> = (0..=255u8).collect();
    client.put_bytes("blob", &value).await.unwrap();
    client.put("text", "héllo").await.unwrap();

    let seen = n.requests();
    assert_eq!((seen[0].method.as_str(), seen[0].path.as_str()), ("PUT", "/kv/blob"));
    assert_eq!(seen[0].header("content-type"), Some("application/octet-stream"));
    assert_eq!(seen[0].body, value);
    assert_eq!(seen[1].body, "héllo".as_bytes());
}

#[tokio::test]
async fn a_read_asks_for_raw_bytes_and_returns_them_untouched() {
    let data: Vec<u8> = vec![0, 159, 146, 150, 255, 10, 13];
    let n = node(vec![Reply::Bytes(data.clone())]).await;
    let client = cluster(&[&n], fast());
    assert_eq!(client.get_bytes("k").await.unwrap(), Some(data));
    assert_eq!(n.requests()[0].header("accept"), Some("application/octet-stream"));
}

#[tokio::test]
async fn reading_text_that_is_not_text_is_an_error_not_corruption() {
    let n = node(vec![Reply::Bytes(vec![0xff, 0xfe])]).await;
    let client = cluster(&[&n], fast());
    assert!(matches!(client.get("k").await, Err(ClientError::NotUtf8)));
    assert_eq!(client.get_bytes("k").await.unwrap(), Some(vec![0xff, 0xfe]));
}

#[tokio::test]
async fn a_missing_key_is_none_and_an_empty_value_is_some() {
    let missing = node(vec![json(404, r#"{"error":"key not found","outcome_unknown":false}"#)]).await;
    assert_eq!(cluster(&[&missing], fast()).get("k").await.unwrap(), None);
    let empty = node(vec![Reply::Bytes(vec![])]).await;
    assert_eq!(cluster(&[&empty], fast()).get("k").await.unwrap(), Some(String::new()));
}

#[tokio::test]
async fn a_bad_key_never_reaches_the_network() {
    let n = node(vec![ok()]).await;
    let client = cluster(&[&n], fast());
    assert!(matches!(client.put("", "v").await, Err(ClientError::InvalidKey(_))));
    assert!(matches!(client.get("a\nb").await, Err(ClientError::InvalidKey(_))));
    assert!(matches!(client.delete(&"k".repeat(1000)).await, Err(ClientError::InvalidKey(_))));
    // "." and ".." would be swallowed by URL normalisation and address no key at all.
    for key in [".", ".."] {
        assert!(matches!(client.put(key, "v").await, Err(ClientError::InvalidKey(_))), "{key}");
        assert!(matches!(client.get(key).await, Err(ClientError::InvalidKey(_))), "{key}");
    }
    assert_eq!(n.count(), 0);
}

// ---- finding the leader ----------------------------------------------------------

#[tokio::test]
async fn a_redirect_is_followed_and_the_leader_is_remembered() {
    let follower = node(vec![redirect_to(2)]).await;
    let leader = node(vec![ok()]).await;
    let client = cluster(&[&follower, &leader], fast());

    client.put("a", "1").await.unwrap();
    assert_eq!((follower.count(), leader.count()), (1, 1), "one wasted hop, then the leader");
    client.put("b", "2").await.unwrap();
    assert_eq!((follower.count(), leader.count()), (1, 2), "the second goes straight to the leader");
}

#[tokio::test]
async fn with_no_hint_the_client_probes_and_then_remembers_who_answered() {
    // A follower that has no idea who the leader is: the client must find it by trying
    // the others, and must not have to do that again for the next request.
    let follower = node(vec![redirect_to(0)]).await;
    let leader = node(vec![ok()]).await;
    let client = cluster(&[&follower, &leader], fast());

    client.put("a", "1").await.unwrap();
    assert_eq!((follower.count(), leader.count()), (1, 1), "probed past the follower once");
    client.put("b", "2").await.unwrap();
    client.put("c", "3").await.unwrap();
    assert_eq!(
        (follower.count(), leader.count()),
        (1, 3),
        "it remembers who answered, so the follower is not asked again"
    );
}

#[tokio::test]
async fn a_raw_read_on_a_follower_is_redirected_too() {
    let follower = node(vec![json(421, r#"{"error":"not leader","outcome_unknown":false,"is_leader":false,"leader_hint":2}"#)]).await;
    let leader = node(vec![Reply::Bytes(b"found".to_vec())]).await;
    let client = cluster(&[&follower, &leader], fast());
    assert_eq!(client.get("k").await.unwrap(), Some("found".to_string()));
}

#[tokio::test]
async fn a_node_that_is_down_is_skipped_even_for_writes() {
    let dead = dead_address().await;
    let leader = node(vec![ok()]).await;
    let mut nodes = HashMap::new();
    nodes.insert(1, dead);
    nodes.insert(2, leader.addr.to_string());
    let client = QuorumClient::with_config(nodes, fast());
    // The connection to node 1 fails before anything is sent, so trying node 2 cannot
    // apply the write twice.
    client.put("k", "v").await.unwrap();
    assert_eq!(leader.count(), 1);
}

// ---- retrying what is safe to retry ---------------------------------------------

#[tokio::test]
async fn an_overloaded_node_is_retried_until_it_has_room() {
    let n = node(vec![busy(), busy(), ok()]).await;
    let client = cluster(&[&n], fast());
    client.put("k", "v").await.unwrap();
    assert_eq!(n.count(), 3);
}

#[tokio::test]
async fn reads_are_retried_through_an_election_too() {
    let n = node(vec![
        json(503, r#"{"error":"shutting down","outcome_unknown":false}"#),
        json(504, r#"{"error":"timed out","outcome_unknown":false}"#),
        Reply::Bytes(b"v".to_vec()),
    ])
    .await;
    assert_eq!(cluster(&[&n], fast()).get("k").await.unwrap(), Some("v".to_string()));
}

#[tokio::test]
async fn giving_up_on_a_node_that_stays_busy_says_unavailable_not_no_leader() {
    let n = node(vec![busy()]).await;
    let client = cluster(&[&n], fast());
    let error = client.put("k", "v").await.unwrap_err();
    assert!(matches!(error, ClientError::Unavailable { .. }), "{error}");
    assert_eq!(n.count(), 4, "exactly max_attempts passes, no more");
}

#[tokio::test]
async fn a_cluster_nobody_answers_for_is_no_leader_reachable() {
    let mut nodes = HashMap::new();
    nodes.insert(1, dead_address().await);
    nodes.insert(2, dead_address().await);
    let client = QuorumClient::with_config(nodes, fast());
    assert!(matches!(client.get("k").await, Err(ClientError::NoLeaderReachable { .. })));
    assert!(matches!(client.put("k", "v").await, Err(ClientError::NoLeaderReachable { .. })));
}

#[tokio::test]
async fn garbage_from_one_node_just_moves_on_to_the_next() {
    let broken = node(vec![json(200, "this is not json"), json(500, "boom")]).await;
    let leader = node(vec![ok()]).await;
    let client = cluster(&[&broken, &leader], fast());
    client.put("k", "v").await.unwrap();
    assert_eq!((broken.count(), leader.count()), (1, 1));
}

// ---- never retrying what is not safe to retry -------------------------------------

#[tokio::test]
async fn a_write_with_an_unknown_outcome_is_reported_not_resent() {
    let first = node(vec![unknown()]).await;
    let second = node(vec![ok()]).await;
    let client = cluster(&[&first, &second], fast());

    let error = client.put("k", "v").await.unwrap_err();
    assert!(matches!(error, ClientError::OutcomeUnknown { .. }), "{error}");
    assert_eq!(first.count(), 1);
    assert_eq!(second.count(), 0, "it was not tried on another node: it might have applied");

    // Same for a delete and a 504.
    let slow = node(vec![json(504, r#"{"error":"timed out","outcome_unknown":true}"#)]).await;
    let other = node(vec![ok()]).await;
    let client = cluster(&[&slow, &other], fast());
    assert!(matches!(client.delete("k").await, Err(ClientError::OutcomeUnknown { .. })));
    assert_eq!(other.count(), 0);
}

#[tokio::test]
async fn resending_unknown_writes_is_possible_but_must_be_asked_for() {
    let first = node(vec![unknown(), ok()]).await;
    let config = ClientConfig { retry_unknown_writes: true, ..fast() };
    cluster(&[&first], config).put("k", "v").await.unwrap();
    assert_eq!(first.count(), 2);
}

#[tokio::test]
async fn a_write_that_got_no_answer_in_time_has_an_unknown_outcome() {
    let hung = node(vec![Reply::Hang]).await;
    let other = node(vec![ok()]).await;
    let config = ClientConfig { request_timeout: Duration::from_millis(150), ..fast() };
    let client = cluster(&[&hung, &other], config);

    let error = client.put("k", "v").await.unwrap_err();
    assert!(matches!(error, ClientError::OutcomeUnknown { .. }), "{error}");
    assert_eq!(other.count(), 0, "the write may have reached the first node");
}

#[tokio::test]
async fn a_read_that_got_no_answer_in_time_simply_asks_another_node() {
    let hung = node(vec![Reply::Hang]).await;
    let other = node(vec![Reply::Bytes(b"v".to_vec())]).await;
    let config = ClientConfig { request_timeout: Duration::from_millis(150), ..fast() };
    let client = cluster(&[&hung, &other], config);
    assert_eq!(client.get("k").await.unwrap(), Some("v".to_string()));
}

#[tokio::test]
async fn requests_that_can_never_succeed_are_not_retried_or_tried_elsewhere() {
    for (status, body) in [
        (413, r#"{"error":"the value is too large: 5 bytes, and the limit is 4","outcome_unknown":false}"#),
        (400, r#"{"error":"invalid key: nope","outcome_unknown":false}"#),
        (415, r#"{"error":"unsupported content type","outcome_unknown":false}"#),
        (409, r#"{"error":"that node is already a member of the cluster","outcome_unknown":false}"#),
    ] {
        let first = node(vec![json(status, body)]).await;
        let second = node(vec![ok()]).await;
        let client = cluster(&[&first, &second], fast());
        match client.put("k", "v").await.unwrap_err() {
            ClientError::Rejected { status: s, message } => {
                assert_eq!(s, status);
                assert!(message.len() > 5, "the server's reason is passed on: {message}");
            }
            other => panic!("{status}: {other}"),
        }
        assert_eq!((first.count(), second.count()), (1, 0), "{status}");
    }
}

// ---- membership -------------------------------------------------------------------

#[tokio::test]
async fn adding_and_removing_members_return_the_new_membership() {
    let n = node(vec![
        json(200, r#"{"is_leader":true,"applied":true,"leader_hint":null,"members":[1,2,3,4]}"#),
        json(200, r#"{"is_leader":true,"applied":true,"leader_hint":null,"members":[1,2,3]}"#),
    ])
    .await;
    let client = cluster(&[&n], fast());
    assert_eq!(client.add_member(4, "10.0.0.4:7000").await.unwrap(), vec![1, 2, 3, 4]);
    assert_eq!(client.remove_member(4).await.unwrap(), vec![1, 2, 3]);

    let seen = n.requests();
    assert_eq!((seen[0].method.as_str(), seen[0].path.as_str()), ("POST", "/admin/members"));
    let body: serde_json::Value = serde_json::from_slice(&seen[0].body).unwrap();
    assert_eq!((body["id"].as_u64(), body["addr"].as_str()), (Some(4), Some("10.0.0.4:7000")));
    assert_eq!((seen[1].method.as_str(), seen[1].path.as_str()), ("DELETE", "/admin/members/4"));
}

#[tokio::test]
async fn a_conflicting_membership_change_is_rejected_not_retried() {
    let n = node(vec![json(409, r#"{"error":"another membership change is still in progress","outcome_unknown":false}"#)]).await;
    let client = cluster(&[&n], fast());
    assert!(matches!(
        client.add_member(4, "x:1").await,
        Err(ClientError::Rejected { status: 409, .. })
    ));
    assert_eq!(n.count(), 1);
}

#[tokio::test]
async fn a_membership_change_with_an_unknown_outcome_says_so_and_members_resolves_it() {
    let n = node(vec![
        unknown(),
        json(200, r#"{"members":[1,2,3,4],"is_leader":true}"#),
    ])
    .await;
    let client = cluster(&[&n], fast());
    assert!(matches!(client.add_member(4, "x:1").await, Err(ClientError::OutcomeUnknown { .. })));
    // The way out: look at what the cluster says its membership is now.
    assert_eq!(client.members().await.unwrap(), vec![1, 2, 3, 4]);
    assert_eq!(n.requests()[1].path, "/health");
}


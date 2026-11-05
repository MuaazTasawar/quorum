//! The HTTP key API end to end: real requests into a real cluster. Covers what a value
//! may contain (any bytes), what a key may be, how big things may get, and how every
//! kind of failure is reported, so a client can tell a request that can never succeed
//! from one that is worth repeating.

mod common;

use common::TestCluster;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SETTLE: Duration = Duration::from_secs(10);

/// A response, with the body kept as raw bytes.
struct Reply {
    status: u16,
    headers: String,
    body: Vec<u8>,
}

impl Reply {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
}

/// HTTP/1.1 over a raw socket, so tests control every byte of the request.
async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Reply {
    let mut stream = TcpStream::connect(addr).await.expect("server should accept connections");
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();

    let split = raw.windows(4).position(|w| w == b"\r\n\r\n").expect("a complete response");
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let status = head.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut body = raw[split + 4..].to_vec();
    if head.to_ascii_lowercase().contains("transfer-encoding: chunked") {
        body = dechunk(&body);
    }
    Reply { status, headers: head, body }
}

fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let line_end = data.windows(2).position(|w| w == b"\r\n").expect("chunk header");
        let size = usize::from_str_radix(std::str::from_utf8(&data[..line_end]).unwrap().trim(), 16).unwrap();
        data = &data[line_end + 2..];
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size + 2..];
    }
}

const JSON: (&str, &str) = ("Content-Type", "application/json");
const OCTETS: (&str, &str) = ("Content-Type", "application/octet-stream");
const WANT_RAW: (&str, &str) = ("Accept", "application/octet-stream");

struct Fixture {
    cluster: TestCluster,
    leader: SocketAddr,
    follower: SocketAddr,
}

async fn fixture(max_value_bytes: usize) -> Fixture {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader_id = cluster.wait_for_leader(SETTLE).await;
    let follower_id = cluster.live_ids().into_iter().find(|&id| id != leader_id).unwrap();
    let leader = cluster.serve_http_limited(leader_id, Duration::from_secs(3), max_value_bytes).await;
    let follower = cluster.serve_http_limited(follower_id, Duration::from_secs(3), max_value_bytes).await;
    Fixture { cluster, leader, follower }
}

/// Every possible byte value, including the ones that are not valid UTF-8.
fn all_bytes() -> Vec<u8> {
    (0..=255u8).cycle().take(1000).collect()
}

#[tokio::test]
async fn a_binary_value_written_raw_comes_back_byte_for_byte() {
    let f = fixture(1 << 20).await;
    let value = all_bytes();
    let put = request(f.leader, "PUT", "/kv/blob", &[OCTETS], &value).await;
    assert_eq!(put.status, 200, "{}", put.text());
    assert!(put.text().contains("\"applied\":true"), "{}", put.text());

    let raw = request(f.leader, "GET", "/kv/blob", &[WANT_RAW], b"").await;
    assert_eq!(raw.status, 200);
    assert!(raw.headers.to_ascii_lowercase().contains("content-type: application/octet-stream"));
    assert_eq!(raw.body, value, "raw read returns exactly what was written");

    // The JSON read carries the same bytes as base64 and does not pretend to be text.
    let json = request(f.leader, "GET", "/kv/blob", &[], b"").await;
    let body = json.text();
    assert!(body.contains("\"found\":true"), "{body}");
    assert!(body.contains("\"value\":null"), "binary data is not passed off as text: {body}");
    assert!(body.contains(&format!("\"value_b64\":\"{}\"", node::base64::encode(&value))), "{body}");
}

#[tokio::test]
async fn a_binary_value_written_as_base64_json_is_the_same_value() {
    let f = fixture(1 << 20).await;
    let value = all_bytes();
    let body = format!("{{\"value_b64\":\"{}\"}}", node::base64::encode(&value));
    let put = request(f.leader, "PUT", "/kv/blob", &[JSON], body.as_bytes()).await;
    assert_eq!(put.status, 200, "{}", put.text());
    let raw = request(f.leader, "GET", "/kv/blob", &[WANT_RAW], b"").await;
    assert_eq!(raw.body, value);
}

#[tokio::test]
async fn text_values_keep_working_exactly_as_before() {
    let f = fixture(1 << 20).await;
    let put = request(f.leader, "PUT", "/kv/greeting", &[JSON], br#"{"value":"h\u00e9llo \u4e16\u754c"}"#).await;
    assert_eq!(put.status, 200);
    let json = request(f.leader, "GET", "/kv/greeting", &[], b"").await.text();
    assert!(json.contains("\"value\":\"héllo 世界\""), "{json}");
    assert!(json.contains("\"found\":true"), "{json}");
    // ...and the same bytes are readable raw.
    let raw = request(f.leader, "GET", "/kv/greeting", &[WANT_RAW], b"").await;
    assert_eq!(raw.body, "héllo 世界".as_bytes());
}

#[tokio::test]
async fn an_empty_value_is_a_value_and_a_missing_key_is_not() {
    let f = fixture(1 << 20).await;
    assert_eq!(request(f.leader, "PUT", "/kv/empty", &[OCTETS], b"").await.status, 200);

    let present = request(f.leader, "GET", "/kv/empty", &[WANT_RAW], b"").await;
    assert_eq!((present.status, present.body.len()), (200, 0), "empty, but there");
    let json = request(f.leader, "GET", "/kv/empty", &[], b"").await.text();
    assert!(json.contains("\"found\":true") && json.contains("\"value\":\"\""), "{json}");

    let missing = request(f.leader, "GET", "/kv/nothing-here", &[WANT_RAW], b"").await;
    assert_eq!(missing.status, 404, "{}", missing.text());
    let json = request(f.leader, "GET", "/kv/nothing-here", &[], b"").await.text();
    assert!(json.contains("\"found\":false") && json.contains("\"is_leader\":true"), "{json}");
}

#[tokio::test]
async fn a_raw_read_on_a_follower_says_so_and_names_the_leader() {
    let f = fixture(1 << 20).await;
    let reply = request(f.follower, "GET", "/kv/anything", &[WANT_RAW], b"").await;
    assert_eq!(reply.status, 421, "{}", reply.text());
    let body = reply.text();
    assert!(body.contains("\"is_leader\":false") && body.contains("\"leader_hint\":"), "{body}");
    // The JSON form of the same read still answers 200 with the redirect fields.
    let json = request(f.follower, "GET", "/kv/anything", &[], b"").await;
    assert_eq!(json.status, 200);
    assert!(json.text().contains("\"is_leader\":false"));
}

#[tokio::test]
async fn awkward_keys_are_stored_under_exactly_the_name_given() {
    let f = fixture(1 << 20).await;
    // Each of these must be a distinct key, written and read back by percent-encoding.
    let keys = [
        ("a%3Fb", "a?b"),
        ("a%2Fb", "a/b"),
        ("a", "a"),
        ("a%23b", "a#b"),
        ("100%25", "100%"),
        ("with%20space", "with space"),
        ("%C3%A9t%C3%A9", "été"),
        ("%E9%8D%B5", "鍵"),
        ("plus%2Bsign", "plus+sign"),
    ];
    for (encoded, _) in keys {
        let put = request(f.leader, "PUT", &format!("/kv/{encoded}"), &[OCTETS], encoded.as_bytes()).await;
        assert_eq!(put.status, 200, "{encoded}: {}", put.text());
    }
    for (encoded, _) in keys {
        let got = request(f.leader, "GET", &format!("/kv/{encoded}"), &[WANT_RAW], b"").await;
        assert_eq!(got.status, 200, "{encoded}");
        assert_eq!(got.body, encoded.as_bytes(), "key {encoded} holds its own value, not another's");
    }
    // They really are different keys: the store holds one entry per name.
    let leader = f.cluster.wait_for_leader(SETTLE).await;
    for (_, name) in keys {
        assert!(
            matches!(f.cluster.get(leader, name).await, node::cluster::QueryResult::Value(Some(_))),
            "the store holds a key named {name:?}"
        );
    }
}

#[tokio::test]
async fn keys_that_break_the_rules_are_refused_with_400_and_nothing_is_stored() {
    let f = fixture(1 << 20).await;
    let leader = f.cluster.wait_for_leader(SETTLE).await;
    let before = f.cluster.snapshot(leader).log_len;

    let too_long = "k".repeat(storage::MAX_KEY_BYTES + 1);
    for bad in [
        "/kv/a%00b".to_string(),      // NUL
        "/kv/line%0Abreak".to_string(), // newline
        "/kv/esc%1B".to_string(),     // escape
        "/kv/%2E%2E".to_string(),     // ".." (and "." below) cannot be addressed over HTTP
        "/kv/%2E".to_string(),
        "/kv/%FF%FE".to_string(),     // not UTF-8 once decoded
        format!("/kv/{too_long}"),
    ] {
        for method in ["PUT", "GET", "DELETE"] {
            let reply = request(f.leader, method, &bad, &[OCTETS], b"x").await;
            assert_eq!(reply.status, 400, "{method} {}: {}", &bad[..bad.len().min(30)], reply.text());
            assert!(reply.text().contains("\"outcome_unknown\":false"), "{}", reply.text());
        }
    }
    assert_eq!(f.cluster.snapshot(leader).log_len, before, "a refused key never reaches the log");

    // The longest allowed key is fine.
    let longest = "k".repeat(storage::MAX_KEY_BYTES);
    assert_eq!(request(f.leader, "PUT", &format!("/kv/{longest}"), &[OCTETS], b"ok").await.status, 200);
}

#[tokio::test]
async fn a_value_over_the_limit_is_refused_with_413_and_nothing_is_stored() {
    let f = fixture(1000).await;
    let leader = f.cluster.wait_for_leader(SETTLE).await;
    let before = f.cluster.snapshot(leader).log_len;

    // Exactly at the limit is fine, in either form.
    assert_eq!(request(f.leader, "PUT", "/kv/fits", &[OCTETS], &vec![7u8; 1000]).await.status, 200);
    let b64 = format!("{{\"value_b64\":\"{}\"}}", node::base64::encode(&vec![7u8; 1000]));
    assert_eq!(request(f.leader, "PUT", "/kv/fits-b64", &[JSON], b64.as_bytes()).await.status, 200);
    let before = before.max(f.cluster.snapshot(leader).log_len);

    // One byte over, raw and as base64 JSON: refused after decoding.
    let raw = request(f.leader, "PUT", "/kv/big", &[OCTETS], &vec![7u8; 1001]).await;
    assert_eq!(raw.status, 413, "{}", raw.text());
    let b64 = format!("{{\"value_b64\":\"{}\"}}", node::base64::encode(&vec![7u8; 1001]));
    assert_eq!(request(f.leader, "PUT", "/kv/big", &[JSON], b64.as_bytes()).await.status, 413);

    // Far over: refused before it is even read in full.
    let huge = request(f.leader, "PUT", "/kv/huge", &[OCTETS], &vec![7u8; 100_000]).await;
    assert_eq!(huge.status, 413, "{}", huge.text());
    assert!(huge.text().contains("\"outcome_unknown\":false"));

    assert_eq!(f.cluster.snapshot(leader).log_len, before, "nothing oversized was logged");
    assert_eq!(request(f.leader, "GET", "/kv/big", &[WANT_RAW], b"").await.status, 404);
}

#[tokio::test]
async fn malformed_writes_are_400_and_say_what_is_wrong() {
    let f = fixture(1 << 20).await;
    for (body, needle) in [
        (&b"{not json"[..], "not valid"),
        (&br#"{}"#[..], "needs a value"),
        (&br#"{"value":"a","value_b64":"YQ=="}"#[..], "not both"),
        (&br#"{"value_b64":"not base64!"}"#[..], "base64"),
        (&br#"{"value_b64":"Zh=="}"#[..], "base64"),
        (&br#"{"value":5}"#[..], "not valid"),
        (&br#"{"value":"a","surprise":1}"#[..], "not valid"),
    ] {
        let reply = request(f.leader, "PUT", "/kv/k", &[JSON], body).await;
        assert_eq!(reply.status, 400, "{}: {}", String::from_utf8_lossy(body), reply.text());
        assert!(reply.text().contains(needle), "{}", reply.text());
    }
}

#[tokio::test]
async fn a_content_type_the_api_does_not_understand_is_415() {
    let f = fixture(1 << 20).await;
    for headers in [&[("Content-Type", "text/plain")][..], &[("Content-Type", "application/x-www-form-urlencoded")], &[]] {
        let reply = request(f.leader, "PUT", "/kv/k", headers, b"value").await;
        assert_eq!(reply.status, 415, "{headers:?}: {}", reply.text());
    }
    // Parameters and case do not matter.
    let ok = request(f.leader, "PUT", "/kv/k", &[("Content-Type", "Application/JSON; charset=utf-8")], br#"{"value":"v"}"#).await;
    assert_eq!(ok.status, 200);
    let ok = request(f.leader, "PUT", "/kv/k", &[("Content-Type", "application/octet-stream; foo=bar")], b"v").await;
    assert_eq!(ok.status, 200);
}

#[tokio::test]
async fn a_delete_removes_a_binary_value_too() {
    let f = fixture(1 << 20).await;
    request(f.leader, "PUT", "/kv/gone", &[OCTETS], &all_bytes()).await;
    assert_eq!(request(f.leader, "DELETE", "/kv/gone", &[], b"").await.status, 200);
    assert_eq!(request(f.leader, "GET", "/kv/gone", &[WANT_RAW], b"").await.status, 404);
}

#[tokio::test]
async fn the_accept_header_may_list_several_types() {
    let f = fixture(1 << 20).await;
    request(f.leader, "PUT", "/kv/k", &[OCTETS], b"bytes").await;
    let raw = request(f.leader, "GET", "/kv/k", &[("Accept", "text/html, application/octet-stream;q=0.9")], b"").await;
    assert_eq!(raw.body, b"bytes");
    let json = request(f.leader, "GET", "/kv/k", &[("Accept", "application/json")], b"").await;
    assert!(json.text().contains("\"found\":true"));
    let any = request(f.leader, "GET", "/kv/k", &[("Accept", "*/*")], b"").await;
    assert!(any.text().contains("\"found\":true"), "*/* keeps the JSON default");
}

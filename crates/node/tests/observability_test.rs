//! `/metrics`, `/dashboard` and the cross-origin rules, against real clusters over real
//! HTTP. The exposition is parsed by a strict reader in this file, so a malformed
//! scrape fails here rather than in someone's Prometheus.

mod common;

use common::TestCluster;
use raft_core::NodeId;
use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SETTLE: Duration = Duration::from_secs(10);

struct Reply {
    status: u16,
    headers: String,
    body: Vec<u8>,
}

impl Reply {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
    /// Value of a response header (case-insensitive name), if present.
    fn header(&self, name: &str) -> Option<String> {
        self.headers.lines().skip(1).find_map(|line| {
            let (n, v) = line.split_once(':')?;
            n.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_string())
        })
    }
}

async fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Reply {
    let mut stream = TcpStream::connect(addr).await.expect("server accepts");
    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a complete response");
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
        let line_end = data.windows(2).position(|w| w == b"\r\n").unwrap();
        let size =
            usize::from_str_radix(std::str::from_utf8(&data[..line_end]).unwrap().trim(), 16)
                .unwrap();
        data = &data[line_end + 2..];
        if size == 0 {
            return out;
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size + 2..];
    }
}

const OCTETS: (&str, &str) = ("Content-Type", "application/octet-stream");

// ---- a strict reader for Prometheus text format 0.0.4 --------------------------

/// A series' labels without `le`, as sorted pairs.
type LabelKey = Vec<(String, String)>;
/// (upper bound, cumulative count) of each bucket of one histogram series.
type Buckets = Vec<(f64, f64)>;

#[derive(Debug)]
struct Sample {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

#[derive(Debug, Default)]
struct Scrape {
    samples: Vec<Sample>,
    types: BTreeMap<String, String>,
    helps: HashSet<String>,
}

impl Scrape {
    /// Value of the one sample with this name and exactly these labels.
    fn get(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let found: Vec<_> = self
            .samples
            .iter()
            .filter(|s| {
                s.name == name
                    && labels
                        .iter()
                        .all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v))
            })
            .collect();
        assert!(
            found.len() <= 1,
            "{name} {labels:?} matched {} samples",
            found.len()
        );
        found.first().map(|s| s.value)
    }
    fn series(&self, name: &str) -> Vec<&Sample> {
        self.samples.iter().filter(|s| s.name == name).collect()
    }
}

fn parse_labels(text: &str) -> BTreeMap<String, String> {
    // text is the part between { and }; values are quoted with \\ \" \n escapes.
    let mut labels = BTreeMap::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let start = i;
        while chars[i] != '=' {
            i += 1;
        }
        let name: String = chars[start..i].iter().collect();
        assert!(
            !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "bad label name {name:?}"
        );
        i += 1;
        assert_eq!(chars[i], '"', "label value must be quoted");
        i += 1;
        let mut value = String::new();
        loop {
            match chars[i] {
                '\\' => {
                    value.push(match chars[i + 1] {
                        'n' => '\n',
                        '\\' => '\\',
                        '"' => '"',
                        other => panic!("bad escape \\{other}"),
                    });
                    i += 2;
                }
                '"' => {
                    i += 1;
                    break;
                }
                c => {
                    value.push(c);
                    i += 1;
                }
            }
        }
        assert!(
            labels.insert(name.clone(), value).is_none(),
            "label {name} repeated"
        );
        if i < chars.len() {
            assert_eq!(chars[i], ',', "labels must be comma separated");
            i += 1;
        }
    }
    labels
}

fn parse(text: &str) -> Scrape {
    assert!(text.ends_with('\n'), "exposition must end with a newline");
    let mut scrape = Scrape::default();
    let mut seen_series = HashSet::new();
    for line in text.lines() {
        assert!(!line.is_empty(), "no blank lines in an exposition");
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = rest.split(' ').next().unwrap();
            assert!(scrape.helps.insert(name.to_string()), "{name} HELP twice");
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split(' ');
            let name = parts.next().unwrap().to_string();
            let kind = parts.next().unwrap().to_string();
            assert!(
                ["counter", "gauge", "histogram"].contains(&kind.as_str()),
                "unknown type {kind}"
            );
            assert!(
                scrape.types.insert(name.clone(), kind).is_none(),
                "{name} TYPE twice"
            );
            continue;
        }
        assert!(!line.starts_with('#'), "stray comment {line:?}");
        let (series, value) = line.rsplit_once(' ').expect("name{labels} value");
        let (name, labels) = match series.split_once('{') {
            Some((name, rest)) => (
                name,
                parse_labels(rest.strip_suffix('}').expect("closing }")),
            ),
            None => (series, BTreeMap::new()),
        };
        assert!(
            !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':'),
            "bad metric name {name:?}"
        );
        let value: f64 = value
            .parse()
            .unwrap_or_else(|_| panic!("bad value {value:?}"));
        assert!(
            seen_series.insert((name.to_string(), labels.clone())),
            "duplicate series {name} {labels:?}"
        );
        // Every sample belongs to a family that declared HELP and TYPE first.
        let family = ["_bucket", "_sum", "_count", "_total"]
            .iter()
            .find_map(|suffix| {
                name.strip_suffix(suffix)
                    .filter(|base| scrape.types.get(*base).map(String::as_str) == Some("histogram"))
            })
            .unwrap_or(name);
        assert!(
            scrape.types.contains_key(family) && scrape.helps.contains(family),
            "{name} has no preceding HELP/TYPE"
        );
        scrape.samples.push(Sample {
            name: name.to_string(),
            labels,
            value,
        });
    }
    // Histograms: cumulative buckets, +Inf bucket == _count.
    for (family, kind) in &scrape.types {
        if kind != "histogram" {
            continue;
        }
        let mut groups: BTreeMap<LabelKey, Buckets> = BTreeMap::new();
        for s in scrape.series(&format!("{family}_bucket")) {
            let le = s.labels["le"].as_str();
            let bound = if le == "+Inf" {
                f64::INFINITY
            } else {
                le.parse().unwrap()
            };
            let key = s
                .labels
                .iter()
                .filter(|(k, _)| *k != "le")
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            groups.entry(key).or_default().push((bound, s.value));
        }
        for (key, buckets) in groups {
            assert!(
                buckets
                    .windows(2)
                    .all(|w| w[0].0 < w[1].0 && w[0].1 <= w[1].1),
                "{family}{key:?} buckets must be ordered and cumulative: {buckets:?}"
            );
            let (last_bound, inf) = *buckets.last().unwrap();
            assert!(last_bound.is_infinite(), "{family} must end with +Inf");
            let count = scrape
                .series(&format!("{family}_count"))
                .into_iter()
                .find(|s| {
                    s.labels
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Vec<_>>()
                        == key
                })
                .unwrap_or_else(|| panic!("{family}{key:?} has no _count"))
                .value;
            assert_eq!(inf, count, "{family}{key:?}: +Inf bucket must equal _count");
        }
    }
    scrape
}

async fn scrape(addr: SocketAddr) -> Scrape {
    let reply = request(addr, "GET", "/metrics", &[], b"").await;
    assert_eq!(reply.status, 200);
    assert_eq!(
        reply.header("content-type").as_deref(),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    parse(&reply.text())
}

async fn serve_all(cluster: &TestCluster) -> BTreeMap<NodeId, SocketAddr> {
    let mut out = BTreeMap::new();
    for id in cluster.live_ids() {
        out.insert(id, cluster.serve_http(id, Duration::from_secs(3)).await);
    }
    out
}

/// Waits until every live node has applied everything committed, so /health and
/// /metrics describe the same settled state.
async fn settle(cluster: &TestCluster) {
    cluster
        .wait_until(SETTLE, "every node applied all committed entries", |c| {
            let ids = c.live_ids();
            let top = ids
                .iter()
                .map(|&id| c.snapshot(id).commit_index)
                .max()
                .unwrap();
            ids.iter().all(|&id| {
                let s = c.snapshot(id);
                s.commit_index == top && s.last_applied == top
            })
        })
        .await;
}

// ---- tests ---------------------------------------------------------------------

#[tokio::test]
async fn metrics_is_well_formed_and_exactly_one_node_reports_leader() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    cluster.put(leader, "k", "v").await;
    settle(&cluster).await;
    let nodes = serve_all(&cluster).await;

    let mut leaders = 0.0;
    for (&id, &addr) in &nodes {
        let s = scrape(addr).await;
        assert_eq!(
            s.get("quorum_node_info", &[("node_id", &id.to_string())]),
            Some(1.0)
        );
        // One-hot role.
        let role_sum: f64 = s.series("quorum_role").iter().map(|x| x.value).sum();
        assert_eq!(role_sum, 1.0, "node {id}: exactly one role is 1");
        leaders += s.get("quorum_is_leader", &[]).unwrap();
        assert_eq!(s.get("quorum_members", &[]), Some(3.0));
        assert_eq!(s.get("quorum_known_leader_id", &[]), Some(leader as f64));
    }
    assert_eq!(leaders, 1.0, "exactly one leader gauge across the cluster");
}

#[tokio::test]
async fn metric_values_track_health() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    for i in 0..20 {
        cluster.put(leader, &format!("k{i}"), "v").await;
    }
    settle(&cluster).await;
    let nodes = serve_all(&cluster).await;

    for (&id, &addr) in &nodes {
        let health = request(addr, "GET", "/health", &[], b"").await;
        assert_eq!(health.status, 200);
        let h: serde_json::Value = serde_json::from_slice(&health.body).unwrap();
        let m = scrape(addr).await;
        let num = |k: &str| h[k].as_f64().unwrap();
        assert_eq!(m.get("quorum_current_term", &[]), Some(num("current_term")));
        assert_eq!(m.get("quorum_commit_index", &[]), Some(num("commit_index")));
        assert_eq!(
            m.get("quorum_last_applied_index", &[]),
            Some(num("last_applied"))
        );
        assert_eq!(m.get("quorum_last_log_index", &[]), Some(num("log_len")));
        assert_eq!(m.get("quorum_wal_syncs_total", &[]), Some(num("wal_syncs")));
        assert_eq!(m.get("quorum_apply_lag_entries", &[]), Some(0.0));
        assert!(m.get("quorum_commit_index", &[]).unwrap() >= 20.0);
        // The leader fsynced its log, so its WAL histogram has observations.
        if id == leader {
            assert!(
                m.get("quorum_wal_append_duration_seconds_count", &[])
                    .unwrap()
                    >= 1.0
            );
        }
    }
}

#[tokio::test]
async fn only_the_leader_reports_peers_and_they_are_caught_up() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    for i in 0..10 {
        cluster.put(leader, &format!("k{i}"), "v").await;
    }
    settle(&cluster).await;
    let nodes = serve_all(&cluster).await;
    // Let a heartbeat round carry the final commit index and acks.
    tokio::time::sleep(Duration::from_millis(600)).await;

    for (&id, &addr) in &nodes {
        let m = scrape(addr).await;
        let peers = m.series("quorum_peer_match_index");
        if id == leader {
            assert_eq!(peers.len(), 2, "a leader reports both followers");
            for p in peers {
                assert_eq!(p.value, m.get("quorum_last_log_index", &[]).unwrap());
            }
            for p in m.series("quorum_peer_replication_lag_entries") {
                assert_eq!(p.value, 0.0, "{:?}", p.labels);
            }
        } else {
            assert!(peers.is_empty(), "follower {id} must not report peers");
        }
    }
}

#[tokio::test]
async fn http_counters_move_and_keys_never_create_series() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let addr = cluster.serve_http(leader, Duration::from_secs(3)).await;

    let before = scrape(addr).await;
    let series_before = before.series("quorum_http_requests_total").len();

    // Many different keys, some hostile-looking, plus paths that match no route.
    for i in 0..40 {
        let key = format!("user-{i}-secretkey");
        let put = request(addr, "PUT", &format!("/kv/{key}"), &[OCTETS], b"x").await;
        assert_eq!(put.status, 200, "{}", put.text());
        let get = request(addr, "GET", &format!("/kv/{key}"), &[], b"").await;
        assert_eq!(get.status, 200);
    }
    for i in 0..10 {
        request(addr, "GET", &format!("/no/such/path/{i}"), &[], b"").await;
    }
    request(addr, "BREW", "/kv/coffee", &[], b"").await;

    let after = scrape(addr).await;
    let text = request(addr, "GET", "/metrics", &[], b"").await.text();
    assert!(!text.contains("secretkey"), "a key leaked into the scrape");
    assert!(!text.contains("/no/such/path"), "a raw path leaked");

    let puts = after
        .get(
            "quorum_http_requests_total",
            &[("route", "/kv/{key}"), ("method", "PUT"), ("code", "200")],
        )
        .unwrap();
    let gets = after
        .get(
            "quorum_http_requests_total",
            &[("route", "/kv/{key}"), ("method", "GET"), ("code", "200")],
        )
        .unwrap();
    assert_eq!((puts, gets), (40.0, 40.0));
    assert_eq!(
        after.get(
            "quorum_http_requests_total",
            &[("route", "unmatched"), ("code", "404")],
        ),
        Some(10.0)
    );
    assert!(
        after
            .series("quorum_http_requests_total")
            .iter()
            .any(|s| s.labels["method"] == "OTHER"),
        "an unknown method is folded into OTHER"
    );
    // 40 keys + 10 paths added at most a handful of series (PUT, GET, unmatched,
    // OTHER), never one per key or path.
    let added = after.series("quorum_http_requests_total").len() - series_before;
    assert!(added <= 6, "series grew by {added}");

    // The latency histogram for the PUT route counted every PUT.
    assert_eq!(
        after.get(
            "quorum_http_request_duration_seconds_count",
            &[("route", "/kv/{key}"), ("method", "PUT")]
        ),
        Some(40.0)
    );
}

#[tokio::test]
async fn cross_origin_reads_are_allowed_only_on_health_and_metrics() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let addr = cluster.serve_http(leader, Duration::from_secs(3)).await;
    let origin = ("Origin", "http://evil.example");

    for path in ["/health", "/metrics"] {
        let r = request(addr, "GET", path, &[origin], b"").await;
        assert_eq!(r.status, 200);
        assert_eq!(
            r.header("access-control-allow-origin").as_deref(),
            Some("*")
        );
    }
    // Nothing that reads data or changes it is opened up.
    let put = request(addr, "PUT", "/kv/a", &[OCTETS, origin], b"x").await;
    assert_eq!(put.status, 200);
    assert!(put.header("access-control-allow-origin").is_none());
    let get = request(addr, "GET", "/kv/a", &[origin], b"").await;
    assert!(get.header("access-control-allow-origin").is_none());
    let del = request(addr, "DELETE", "/kv/a", &[origin], b"").await;
    assert!(del.header("access-control-allow-origin").is_none());
    let admin = request(addr, "POST", "/admin/members", &[origin], b"{}").await;
    assert!(admin.header("access-control-allow-origin").is_none());
    let dash = request(addr, "GET", "/dashboard", &[origin], b"").await;
    assert!(dash.header("access-control-allow-origin").is_none());
    // Only GET: a POST to /health is not opened up either.
    let post = request(addr, "POST", "/health", &[origin], b"").await;
    assert!(post.header("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn dashboard_is_served_with_a_locked_down_policy() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let addr = cluster.serve_http(leader, Duration::from_secs(3)).await;

    let r = request(addr, "GET", "/dashboard", &[], b"").await;
    assert_eq!(r.status, 200);
    assert_eq!(
        r.header("content-type").as_deref(),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(r.header("cache-control").as_deref(), Some("no-store"));
    assert_eq!(
        r.header("x-content-type-options").as_deref(),
        Some("nosniff")
    );
    let csp = r.header("content-security-policy").expect("a CSP");
    assert!(csp.contains("default-src 'none'"), "{csp}");
    assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
    assert!(csp.contains("form-action 'none'"), "{csp}");
    assert!(!csp.contains("script-src *"), "{csp}");
    let body = r.text();
    assert!(body.contains("<html"), "an HTML page");
    // Nothing the page loads comes from outside it.
    for needle in ["src=\"http", "href=\"http", "src='http", "@import", "<link"] {
        assert!(!body.contains(needle), "page references {needle}");
    }
    assert!(
        body.contains("QuorumDashboard"),
        "pure functions exposed for tests"
    );
}

#[tokio::test]
async fn election_counters_record_a_leader_change() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let first = cluster.wait_for_leader(SETTLE).await;
    let nodes = serve_all(&cluster).await;

    // One election so far: the leader won it, and has not lost anything.
    let m = scrape(nodes[&first]).await;
    assert_eq!(m.get("quorum_leader_elections_won_total", &[]), Some(1.0));
    assert_eq!(m.get("quorum_leadership_lost_total", &[]), Some(0.0));
    assert!(m.get("quorum_elections_started_total", &[]).unwrap() >= 1.0);

    // Cut the leader off. It notices it has lost its majority and steps down; the
    // others elect a replacement.
    cluster.isolate(first);
    let others: Vec<NodeId> = cluster
        .live_ids()
        .into_iter()
        .filter(|&id| id != first)
        .collect();
    let second = cluster.wait_for_leader_among(&others, SETTLE).await;
    cluster
        .wait_until(SETTLE, "the cut-off leader stepped down", |c| {
            c.snapshot(first).role != "Leader"
        })
        .await;

    let old = scrape(nodes[&first]).await;
    assert_eq!(old.get("quorum_leadership_lost_total", &[]), Some(1.0));
    assert_eq!(old.get("quorum_is_leader", &[]), Some(0.0));
    let new = scrape(nodes[&second]).await;
    assert_eq!(new.get("quorum_leader_elections_won_total", &[]), Some(1.0));
    assert_eq!(new.get("quorum_is_leader", &[]), Some(1.0));
    assert!(
        new.get("quorum_current_term", &[]).unwrap() > m.get("quorum_current_term", &[]).unwrap(),
        "the new leader's term moved on"
    );
}

#[tokio::test]
async fn snapshot_metrics_follow_compaction() {
    // Snapshot after every 20 entries, keeping a short tail.
    let cluster = TestCluster::start_with_snapshots(&[1, 2, 3], 20, 5);
    let leader = cluster.wait_for_leader(SETTLE).await;
    for i in 0..60 {
        cluster.put(leader, &format!("k{i}"), "v").await;
    }
    cluster
        .wait_until(SETTLE, "the leader took a snapshot", |c| {
            c.snapshot(leader).snapshots_taken >= 1
        })
        .await;
    settle(&cluster).await;
    let addr = cluster.serve_http(leader, Duration::from_secs(3)).await;
    let m = scrape(addr).await;
    assert!(m.get("quorum_snapshots_taken_total", &[]).unwrap() >= 1.0);
    assert!(m.get("quorum_snapshot_index", &[]).unwrap() >= 20.0);
    assert!(m.get("quorum_log_base_index", &[]).unwrap() >= 1.0);
    assert!(
        m.get("quorum_snapshot_write_duration_seconds_count", &[])
            .unwrap()
            >= 1.0
    );
}

#[tokio::test]
async fn heartbeats_are_not_counted_as_wal_appends() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    for i in 0..10 {
        cluster.put(leader, &format!("k{i}"), "v").await;
    }
    settle(&cluster).await;
    let nodes = serve_all(&cluster).await;
    // Several heartbeat rounds with nothing to write.
    tokio::time::sleep(Duration::from_millis(800)).await;

    for (&id, &addr) in &nodes {
        let m = scrape(addr).await;
        let appends = m
            .get("quorum_wal_append_duration_seconds_count", &[])
            .unwrap();
        let syncs = m.get("quorum_wal_syncs_total", &[]).unwrap();
        assert!(appends >= 1.0, "node {id} wrote entries");
        assert!(
            appends <= syncs,
            "node {id}: {appends} timed appends but only {syncs} fsyncs; \
             heartbeats that write nothing are being timed"
        );
    }
}

//! Metrics in the Prometheus text exposition format (version 0.0.4).
//!
//! A node's state comes from the snapshot its event loop already publishes; what a
//! snapshot cannot hold (how long disk writes and HTTP requests take, how many requests
//! of each kind were answered each way) is collected here. Nothing on the request path
//! waits for the metrics: recording is a short lock around a map update.

use crate::cluster::ClusterSnapshot;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;
use std::time::Duration;

/// Upper bounds, in seconds, of the latency histogram buckets: from half a millisecond
/// (a fast SSD flush) to ten seconds (a disk in serious trouble).
pub const LATENCY_BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// A latency histogram with fixed buckets. Counts are kept per bucket and made
/// cumulative only when rendered, as Prometheus expects.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Histogram {
    /// Observations per bucket; the last slot is everything above the largest bound.
    buckets: [u64; LATENCY_BUCKETS.len() + 1],
    sum_seconds: f64,
    count: u64,
}

impl Histogram {
    pub fn observe(&mut self, elapsed: Duration) {
        self.observe_seconds(elapsed.as_secs_f64());
    }

    pub fn observe_seconds(&mut self, seconds: f64) {
        // A bucket's bound is inclusive: an observation equal to it belongs to it.
        let slot = LATENCY_BUCKETS
            .iter()
            .position(|&bound| seconds <= bound)
            .unwrap_or(LATENCY_BUCKETS.len());
        self.buckets[slot] += 1;
        self.sum_seconds += seconds;
        self.count += 1;
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn sum_seconds(&self) -> f64 {
        self.sum_seconds
    }

    /// `(upper bound label, cumulative count)` for every bucket, ending with `+Inf`.
    fn cumulative(&self) -> Vec<(String, u64)> {
        let mut running = 0;
        let mut out = Vec::with_capacity(self.buckets.len());
        for (slot, &n) in self.buckets.iter().enumerate() {
            running += n;
            let label = LATENCY_BUCKETS
                .get(slot)
                .map_or_else(|| "+Inf".to_string(), |bound| bound.to_string());
            out.push((label, running));
        }
        out
    }
}

/// What the HTTP layer has seen, grouped so the number of distinct series stays small
/// however clients behave: routes are the router's templates (`/kv/{key}`, never the key
/// itself) and unknown methods share one label.
#[derive(Default)]
pub struct HttpMetrics {
    inner: Mutex<HttpInner>,
}

#[derive(Default)]
struct HttpInner {
    /// (route, method, status code) -> requests answered.
    requests: BTreeMap<(String, String, u16), u64>,
    /// (route, method) -> how long they took.
    durations: BTreeMap<(String, String), Histogram>,
}

/// A copy of [`HttpMetrics`] taken for rendering.
#[derive(Default, Clone)]
pub struct HttpSnapshot {
    requests: BTreeMap<(String, String, u16), u64>,
    durations: BTreeMap<(String, String), Histogram>,
}

fn method_label(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "PUT" => "PUT",
        "POST" => "POST",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    }
}

impl HttpMetrics {
    pub fn record(&self, route: &str, method: &str, status: u16, elapsed: Duration) {
        let method = method_label(method).to_string();
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        *inner
            .requests
            .entry((route.to_string(), method.clone(), status))
            .or_default() += 1;
        inner
            .durations
            .entry((route.to_string(), method))
            .or_default()
            .observe(elapsed);
    }

    pub fn snapshot(&self) -> HttpSnapshot {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        HttpSnapshot {
            requests: inner.requests.clone(),
            durations: inner.durations.clone(),
        }
    }
}

/// Builds the text of one exposition: each metric family is written once, with its
/// HELP and TYPE lines, and its samples directly after.
struct Exposition {
    out: String,
}

fn escape_help(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\n', "\\n")
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

impl Exposition {
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.out, "# HELP {name} {}", escape_help(help));
        let _ = writeln!(self.out, "# TYPE {name} {kind}");
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl std::fmt::Display) {
        self.out.push_str(name);
        if !labels.is_empty() {
            self.out.push('{');
            for (i, (key, val)) in labels.iter().enumerate() {
                if i > 0 {
                    self.out.push(',');
                }
                let _ = write!(self.out, "{key}=\"{}\"", escape_label(val));
            }
            self.out.push('}');
        }
        let _ = writeln!(self.out, " {value}");
    }

    fn gauge(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.family(name, "gauge", help);
        self.sample(name, &[], value);
    }

    fn counter(&mut self, name: &str, help: &str, value: impl std::fmt::Display) {
        self.family(name, "counter", help);
        self.sample(name, &[], value);
    }

    /// The `_bucket`, `_sum` and `_count` samples of one histogram series. The family's
    /// HELP and TYPE lines are written separately, once, by the caller.
    fn histogram_series(&mut self, name: &str, labels: &[(&str, &str)], histogram: &Histogram) {
        for (bound, cumulative) in histogram.cumulative() {
            let mut with_le: Vec<(&str, &str)> = labels.to_vec();
            with_le.push(("le", &bound));
            self.sample(&format!("{name}_bucket"), &with_le, cumulative);
        }
        self.sample(&format!("{name}_sum"), labels, histogram.sum_seconds());
        self.sample(&format!("{name}_count"), labels, histogram.count());
    }

    fn histogram(&mut self, name: &str, help: &str, histogram: &Histogram) {
        self.family(name, "histogram", help);
        self.histogram_series(name, &[], histogram);
    }
}

/// Renders everything a scrape of this node returns.
pub fn render(snapshot: &ClusterSnapshot, http: &HttpSnapshot) -> String {
    let mut e = Exposition { out: String::new() };
    let id = snapshot.node_id.to_string();

    e.family(
        "quorum_node_info",
        "gauge",
        "Identifies this node. Always 1; the labels carry the information.",
    );
    e.sample(
        "quorum_node_info",
        &[("node_id", &id), ("version", env!("CARGO_PKG_VERSION"))],
        1,
    );

    let role = snapshot.role.to_ascii_lowercase();
    e.family(
        "quorum_role",
        "gauge",
        "1 for the role this node holds now (leader, follower or candidate), 0 for the others.",
    );
    for candidate in ["leader", "follower", "candidate"] {
        e.sample(
            "quorum_role",
            &[("role", candidate)],
            u8::from(role == candidate),
        );
    }
    e.gauge(
        "quorum_is_leader",
        "1 if this node is the leader, otherwise 0. Sum it over the cluster: it should be exactly 1.",
        u8::from(role == "leader"),
    );
    e.gauge(
        "quorum_known_leader_id",
        "The id of the node this one believes is the leader, or 0 if it does not know.",
        snapshot.leader_hint.unwrap_or(0),
    );
    e.gauge(
        "quorum_current_term",
        "The node's current Raft term.",
        snapshot.current_term,
    );
    e.gauge(
        "quorum_commit_index",
        "Index of the last log entry known to be committed.",
        snapshot.commit_index,
    );
    e.gauge(
        "quorum_last_applied_index",
        "Index of the last log entry applied to the key-value store.",
        snapshot.last_applied,
    );
    e.gauge(
        "quorum_apply_lag_entries",
        "Entries committed but not yet applied. Should be close to 0.",
        snapshot.commit_index.saturating_sub(snapshot.last_applied),
    );
    e.gauge(
        "quorum_last_log_index",
        "Index of the last entry in this node's log.",
        snapshot.log_len,
    );
    e.gauge(
        "quorum_log_base_index",
        "Index of the last log entry discarded by compaction; the log starts after it.",
        snapshot.log_base,
    );
    e.gauge(
        "quorum_snapshot_index",
        "Index the newest snapshot on disk reaches (0 if none).",
        snapshot.snapshot_index,
    );
    e.gauge(
        "quorum_members",
        "Voting members of the cluster, as this node sees them.",
        snapshot.members.len(),
    );
    e.gauge(
        "quorum_learners",
        "Nodes being caught up before they are admitted as voters (leader only).",
        snapshot.learners.len(),
    );

    e.counter(
        "quorum_wal_syncs_total",
        "Times the log has been forced to disk.",
        snapshot.wal_syncs,
    );
    e.counter(
        "quorum_peer_messages_dropped_total",
        "Outbound peer messages discarded because that peer's queue was full.",
        snapshot.dropped_messages,
    );
    e.counter(
        "quorum_snapshots_taken_total",
        "Snapshots this process has taken.",
        snapshot.snapshots_taken,
    );
    e.counter(
        "quorum_snapshots_installed_total",
        "Snapshots this process has received from a leader and installed.",
        snapshot.snapshots_installed,
    );
    e.counter(
        "quorum_elections_started_total",
        "Elections this node has started (a pre-vote that won and became a real election).",
        snapshot.elections_started,
    );
    e.counter(
        "quorum_leader_elections_won_total",
        "Times this node became leader.",
        snapshot.leader_elections_won,
    );
    e.counter(
        "quorum_leadership_lost_total",
        "Times this node stopped being leader. Frequent increases mean leadership is unstable.",
        snapshot.leadership_lost,
    );

    e.histogram(
        "quorum_wal_append_duration_seconds",
        "How long writing entries to the log, including the flush to disk, takes.",
        &snapshot.wal_append,
    );
    e.histogram(
        "quorum_snapshot_write_duration_seconds",
        "How long writing a snapshot to disk takes.",
        &snapshot.snapshot_write,
    );

    // Per-follower replication progress; only a leader has any.
    if !snapshot.peers.is_empty() {
        e.family(
            "quorum_peer_match_index",
            "gauge",
            "Highest log index known to be replicated on that peer (leader only).",
        );
        for p in &snapshot.peers {
            e.sample(
                "quorum_peer_match_index",
                &[("peer", &p.id.to_string())],
                p.match_index,
            );
        }
        e.family(
            "quorum_peer_replication_lag_entries",
            "gauge",
            "Entries the leader has that the peer has not yet confirmed (leader only).",
        );
        for p in &snapshot.peers {
            e.sample(
                "quorum_peer_replication_lag_entries",
                &[("peer", &p.id.to_string())],
                snapshot.log_len.saturating_sub(p.match_index),
            );
        }
        e.family(
            "quorum_peer_seconds_since_ack",
            "gauge",
            "Seconds since the peer last answered the leader; absent if it never has (leader only).",
        );
        for p in &snapshot.peers {
            if let Some(ms) = p.millis_since_ack {
                e.sample(
                    "quorum_peer_seconds_since_ack",
                    &[("peer", &p.id.to_string())],
                    ms as f64 / 1000.0,
                );
            }
        }
        e.family(
            "quorum_peer_snapshot_transfer_active",
            "gauge",
            "1 while the leader is sending that peer a snapshot (leader only).",
        );
        for p in &snapshot.peers {
            e.sample(
                "quorum_peer_snapshot_transfer_active",
                &[("peer", &p.id.to_string())],
                u8::from(p.snapshot_transfer),
            );
        }
    }

    if !http.requests.is_empty() {
        e.family(
            "quorum_http_requests_total",
            "counter",
            "HTTP requests answered, by route template, method and status code.",
        );
        for ((route, method, code), n) in &http.requests {
            e.sample(
                "quorum_http_requests_total",
                &[
                    ("route", route),
                    ("method", method),
                    ("code", &code.to_string()),
                ],
                n,
            );
        }
        e.family(
            "quorum_http_request_duration_seconds",
            "histogram",
            "How long HTTP requests take to answer, by route template and method.",
        );
        for ((route, method), histogram) in &http.durations {
            e.histogram_series(
                "quorum_http_request_duration_seconds",
                &[("route", route), ("method", method)],
                histogram,
            );
        }
    }

    e.out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::PeerSnapshot;
    use std::collections::{BTreeSet, HashMap};

    // ---- a small, strict reader of the exposition format, to check the output against ----

    #[derive(Debug, Clone)]
    pub(crate) struct Sample {
        pub name: String,
        pub labels: BTreeMap<String, String>,
        pub value: f64,
    }

    /// Parses exposition text, refusing anything a real Prometheus would. Returns the
    /// samples and the declared type of each family.
    pub(crate) fn parse(text: &str) -> Result<(Vec<Sample>, HashMap<String, String>), String> {
        let mut samples = Vec::new();
        let mut types: HashMap<String, String> = HashMap::new();
        let mut helps: BTreeSet<String> = BTreeSet::new();
        let mut seen_families_done: BTreeSet<String> = BTreeSet::new();
        let mut current: Option<String> = None;

        for (n, line) in text.lines().enumerate() {
            let at = |m: &str| format!("line {}: {m}: {line:?}", n + 1);
            if line.is_empty() {
                return Err(at("blank line"));
            }
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let name = rest.split(' ').next().unwrap_or("");
                if !helps.insert(name.to_string()) {
                    return Err(at("HELP written twice"));
                }
                continue;
            }
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let mut parts = rest.split(' ');
                let (name, kind) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
                if !["gauge", "counter", "histogram"].contains(&kind) {
                    return Err(at("unknown type"));
                }
                if types.insert(name.to_string(), kind.to_string()).is_some() {
                    return Err(at("TYPE written twice"));
                }
                if !helps.contains(name) {
                    return Err(at("TYPE without a preceding HELP"));
                }
                if let Some(prev) = current.replace(name.to_string()) {
                    seen_families_done.insert(prev);
                }
                continue;
            }
            if line.starts_with('#') {
                return Err(at("unknown comment"));
            }

            let (head, value) = line.rsplit_once(' ').ok_or_else(|| at("no value"))?;
            let value: f64 = value.parse().map_err(|_| at("value is not a number"))?;
            let (name, labels) = match head.split_once('{') {
                None => (head.to_string(), BTreeMap::new()),
                Some((name, rest)) => {
                    let body = rest
                        .strip_suffix('}')
                        .ok_or_else(|| at("unclosed labels"))?;
                    (name.to_string(), parse_labels(body).map_err(|m| at(&m))?)
                }
            };
            let valid_name = !name.is_empty()
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
                && !name.chars().next().unwrap().is_ascii_digit();
            if !valid_name {
                return Err(at("invalid metric name"));
            }
            let family = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|suffix| {
                    name.strip_suffix(suffix)
                        .filter(|base| types.get(*base).map(String::as_str) == Some("histogram"))
                })
                .unwrap_or(&name)
                .to_string();
            if !types.contains_key(&family) {
                return Err(at("sample for a family with no TYPE"));
            }
            if current.as_deref() != Some(family.as_str()) {
                return Err(at("samples of a family must follow its TYPE directly"));
            }
            if types[&family] == "counter" && !family.ends_with("_total") {
                return Err(at("counters must end in _total"));
            }
            if types[&family] == "counter" && value < 0.0 {
                return Err(at("negative counter"));
            }
            samples.push(Sample {
                name,
                labels,
                value,
            });
        }
        Ok((samples, types))
    }

    fn parse_labels(body: &str) -> Result<BTreeMap<String, String>, String> {
        let mut labels = BTreeMap::new();
        let mut chars = body.chars().peekable();
        while chars.peek().is_some() {
            let mut key = String::new();
            for c in chars.by_ref() {
                if c == '=' {
                    break;
                }
                key.push(c);
            }
            if chars.next() != Some('"') {
                return Err("label value must be quoted".into());
            }
            let mut value = String::new();
            loop {
                match chars.next() {
                    None => return Err("unterminated label value".into()),
                    Some('"') => break,
                    Some('\\') => match chars.next() {
                        Some('\\') => value.push('\\'),
                        Some('"') => value.push('"'),
                        Some('n') => value.push('\n'),
                        _ => return Err("bad escape".into()),
                    },
                    Some('\n') => return Err("raw newline in a label value".into()),
                    Some(c) => value.push(c),
                }
            }
            if labels.insert(key, value).is_some() {
                return Err("duplicate label".into());
            }
            match chars.next() {
                None | Some(',') => {}
                Some(_) => return Err("labels must be comma separated".into()),
            }
        }
        Ok(labels)
    }

    type Labels = BTreeMap<String, String>;
    type SeriesKey = (String, Labels);

    /// Checks every histogram: buckets are cumulative, end in +Inf, and +Inf equals the count.
    pub(crate) fn check_histograms(samples: &[Sample]) -> Result<(), String> {
        let mut series: BTreeMap<SeriesKey, Vec<(f64, f64)>> = BTreeMap::new();
        let mut counts: BTreeMap<SeriesKey, f64> = BTreeMap::new();
        for s in samples {
            let mut labels = s.labels.clone();
            if let Some(base) = s.name.strip_suffix("_bucket") {
                let le = labels.remove("le").ok_or("bucket without le")?;
                let bound = if le == "+Inf" {
                    f64::INFINITY
                } else {
                    le.parse().map_err(|_| "bad le")?
                };
                series
                    .entry((base.to_string(), labels))
                    .or_default()
                    .push((bound, s.value));
            } else if let Some(base) = s.name.strip_suffix("_count") {
                counts.insert((base.to_string(), labels), s.value);
            }
        }
        for (key, buckets) in &series {
            if !buckets
                .windows(2)
                .all(|w| w[0].0 < w[1].0 && w[0].1 <= w[1].1)
            {
                return Err(format!(
                    "{key:?}: buckets are not increasing and cumulative"
                ));
            }
            let (last_bound, last_count) = *buckets.last().unwrap();
            if last_bound != f64::INFINITY {
                return Err(format!("{key:?}: no +Inf bucket"));
            }
            if counts.get(key) != Some(&last_count) {
                return Err(format!("{key:?}: +Inf bucket differs from _count"));
            }
        }
        Ok(())
    }

    fn value_of(samples: &[Sample], name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        samples
            .iter()
            .find(|s| {
                s.name == name
                    && labels
                        .iter()
                        .all(|(k, v)| s.labels.get(*k).map(String::as_str) == Some(*v))
            })
            .map(|s| s.value)
    }

    fn follower() -> ClusterSnapshot {
        ClusterSnapshot {
            node_id: 2,
            role: "Follower".into(),
            current_term: 7,
            commit_index: 40,
            last_applied: 38,
            log_len: 41,
            leader_hint: Some(1),
            members: vec![1, 2, 3],
            wal_syncs: 12,
            ..ClusterSnapshot::default()
        }
    }

    fn leader() -> ClusterSnapshot {
        let mut wal_append = Histogram::default();
        for ms in [1, 3, 3, 40, 700] {
            wal_append.observe(Duration::from_millis(ms));
        }
        ClusterSnapshot {
            node_id: 1,
            role: "Leader".into(),
            current_term: 7,
            commit_index: 50,
            last_applied: 50,
            log_len: 52,
            leader_hint: Some(1),
            members: vec![1, 2, 3],
            learners: vec![4],
            leader_elections_won: 2,
            elections_started: 3,
            leadership_lost: 1,
            wal_append,
            peers: vec![
                PeerSnapshot {
                    id: 2,
                    match_index: 52,
                    next_index: 53,
                    millis_since_ack: Some(35),
                    snapshot_transfer: false,
                    learner: false,
                },
                PeerSnapshot {
                    id: 4,
                    match_index: 10,
                    next_index: 11,
                    millis_since_ack: None,
                    snapshot_transfer: true,
                    learner: true,
                },
            ],
            ..ClusterSnapshot::default()
        }
    }

    // ---- the histogram ----

    #[test]
    fn an_observation_goes_in_the_first_bucket_whose_bound_it_does_not_exceed() {
        let mut h = Histogram::default();
        h.observe_seconds(0.0005); // exactly the first bound: inclusive
        h.observe_seconds(0.0006);
        h.observe_seconds(99.0); // beyond every bound
        let cumulative = h.cumulative();
        assert_eq!(cumulative[0], ("0.0005".to_string(), 1));
        assert_eq!(cumulative[1], ("0.001".to_string(), 2));
        assert_eq!(
            cumulative[LATENCY_BUCKETS.len() - 1].1,
            2,
            "99s is in no finite bucket"
        );
        assert_eq!(cumulative.last().unwrap(), &("+Inf".to_string(), 3));
        assert_eq!(h.count(), 3);
        assert!((h.sum_seconds() - 99.0011).abs() < 1e-9);
    }

    #[test]
    fn an_empty_histogram_is_all_zeros_and_still_well_formed() {
        let h = Histogram::default();
        assert!(h.cumulative().iter().all(|(_, n)| *n == 0));
        assert_eq!((h.count(), h.sum_seconds()), (0, 0.0));
    }

    // ---- the exposition ----

    #[test]
    fn what_is_rendered_is_well_formed_for_a_follower_and_for_a_leader() {
        for snapshot in [follower(), leader()] {
            let text = render(&snapshot, &HttpSnapshot::default());
            let (samples, _) = parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
            check_histograms(&samples).unwrap_or_else(|e| panic!("{e}\n{text}"));
        }
    }

    #[test]
    fn exactly_one_role_is_set_and_it_matches_is_leader() {
        for (snapshot, role) in [(follower(), "follower"), (leader(), "leader")] {
            let text = render(&snapshot, &HttpSnapshot::default());
            let (samples, _) = parse(&text).unwrap();
            let set: Vec<&str> = ["leader", "follower", "candidate"]
                .into_iter()
                .filter(|r| value_of(&samples, "quorum_role", &[("role", r)]) == Some(1.0))
                .collect();
            assert_eq!(set, vec![role]);
            let is_leader = value_of(&samples, "quorum_is_leader", &[]).unwrap();
            assert_eq!(is_leader, if role == "leader" { 1.0 } else { 0.0 });
        }
    }

    #[test]
    fn the_state_gauges_carry_the_snapshots_numbers() {
        let text = render(&follower(), &HttpSnapshot::default());
        let (s, _) = parse(&text).unwrap();
        assert_eq!(value_of(&s, "quorum_current_term", &[]), Some(7.0));
        assert_eq!(value_of(&s, "quorum_commit_index", &[]), Some(40.0));
        assert_eq!(value_of(&s, "quorum_last_applied_index", &[]), Some(38.0));
        assert_eq!(value_of(&s, "quorum_apply_lag_entries", &[]), Some(2.0));
        assert_eq!(value_of(&s, "quorum_last_log_index", &[]), Some(41.0));
        assert_eq!(value_of(&s, "quorum_members", &[]), Some(3.0));
        assert_eq!(value_of(&s, "quorum_known_leader_id", &[]), Some(1.0));
        assert_eq!(value_of(&s, "quorum_wal_syncs_total", &[]), Some(12.0));
        assert_eq!(
            value_of(&s, "quorum_node_info", &[("node_id", "2")]),
            Some(1.0)
        );
    }

    #[test]
    fn a_node_with_no_known_leader_says_zero() {
        let mut snapshot = follower();
        snapshot.leader_hint = None;
        let (s, _) = parse(&render(&snapshot, &HttpSnapshot::default())).unwrap();
        assert_eq!(value_of(&s, "quorum_known_leader_id", &[]), Some(0.0));
    }

    #[test]
    fn apply_lag_never_goes_negative() {
        let mut snapshot = follower();
        snapshot.last_applied = 99;
        let (s, _) = parse(&render(&snapshot, &HttpSnapshot::default())).unwrap();
        assert_eq!(value_of(&s, "quorum_apply_lag_entries", &[]), Some(0.0));
    }

    #[test]
    fn a_leader_reports_each_peers_progress_and_a_follower_reports_none() {
        let (s, _) = parse(&render(&leader(), &HttpSnapshot::default())).unwrap();
        assert_eq!(
            value_of(&s, "quorum_peer_match_index", &[("peer", "2")]),
            Some(52.0)
        );
        assert_eq!(
            value_of(&s, "quorum_peer_replication_lag_entries", &[("peer", "2")]),
            Some(0.0)
        );
        assert_eq!(
            value_of(&s, "quorum_peer_replication_lag_entries", &[("peer", "4")]),
            Some(42.0),
            "52 entries in the log, the learner has 10"
        );
        assert_eq!(
            value_of(&s, "quorum_peer_seconds_since_ack", &[("peer", "2")]),
            Some(0.035)
        );
        assert_eq!(
            value_of(&s, "quorum_peer_seconds_since_ack", &[("peer", "4")]),
            None,
            "a peer that has never answered has no age"
        );
        assert_eq!(
            value_of(&s, "quorum_peer_snapshot_transfer_active", &[("peer", "4")]),
            Some(1.0)
        );

        let (f, _) = parse(&render(&follower(), &HttpSnapshot::default())).unwrap();
        let per_peer = [
            "quorum_peer_match_index",
            "quorum_peer_replication_lag_entries",
            "quorum_peer_seconds_since_ack",
            "quorum_peer_snapshot_transfer_active",
        ];
        assert!(
            f.iter().all(|s| !per_peer.contains(&s.name.as_str())),
            "only a leader has peers to report on"
        );
    }

    #[test]
    fn the_event_counters_and_the_disk_histogram_are_exported() {
        let (s, types) = parse(&render(&leader(), &HttpSnapshot::default())).unwrap();
        assert_eq!(
            value_of(&s, "quorum_leader_elections_won_total", &[]),
            Some(2.0)
        );
        assert_eq!(
            value_of(&s, "quorum_elections_started_total", &[]),
            Some(3.0)
        );
        assert_eq!(value_of(&s, "quorum_leadership_lost_total", &[]), Some(1.0));
        assert_eq!(types["quorum_wal_append_duration_seconds"], "histogram");
        assert_eq!(
            value_of(&s, "quorum_wal_append_duration_seconds_count", &[]),
            Some(5.0)
        );
        assert_eq!(
            value_of(
                &s,
                "quorum_wal_append_duration_seconds_bucket",
                &[("le", "0.005")]
            ),
            Some(3.0),
            "1ms, 3ms and 3ms are at most 5ms"
        );
        assert_eq!(
            value_of(
                &s,
                "quorum_wal_append_duration_seconds_bucket",
                &[("le", "+Inf")]
            ),
            Some(5.0)
        );
    }

    #[test]
    fn http_requests_are_counted_by_route_method_and_code() {
        let http = HttpMetrics::default();
        http.record("/kv/{key}", "PUT", 200, Duration::from_millis(3));
        http.record("/kv/{key}", "PUT", 200, Duration::from_millis(5));
        http.record("/kv/{key}", "PUT", 503, Duration::from_millis(1));
        http.record("/kv/{key}", "GET", 200, Duration::from_millis(1));
        let text = render(&follower(), &http.snapshot());
        let (s, types) = parse(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        check_histograms(&s).unwrap();
        let route = ("route", "/kv/{key}");
        assert_eq!(types["quorum_http_requests_total"], "counter");
        assert_eq!(
            value_of(
                &s,
                "quorum_http_requests_total",
                &[route, ("method", "PUT"), ("code", "200")]
            ),
            Some(2.0)
        );
        assert_eq!(
            value_of(
                &s,
                "quorum_http_requests_total",
                &[route, ("method", "PUT"), ("code", "503")]
            ),
            Some(1.0)
        );
        assert_eq!(
            value_of(
                &s,
                "quorum_http_request_duration_seconds_count",
                &[route, ("method", "PUT")]
            ),
            Some(3.0),
            "the histogram covers every code"
        );
    }

    #[test]
    fn unknown_methods_share_one_label_so_clients_cannot_multiply_series() {
        let http = HttpMetrics::default();
        for method in ["BREW", "PROPFIND", "X-ANYTHING", "PATCH"] {
            http.record("unmatched", method, 405, Duration::from_millis(1));
        }
        let (s, _) = parse(&render(&follower(), &http.snapshot())).unwrap();
        let other = value_of(
            &s,
            "quorum_http_requests_total",
            &[("method", "OTHER"), ("code", "405")],
        );
        assert_eq!(other, Some(4.0));
        let distinct: BTreeSet<_> = s
            .iter()
            .filter(|x| x.name == "quorum_http_requests_total")
            .map(|x| x.labels["method"].clone())
            .collect();
        assert_eq!(distinct.len(), 1);
    }

    #[test]
    fn label_values_are_escaped_so_they_cannot_break_the_format() {
        let mut e = Exposition { out: String::new() };
        e.family(
            "quorum_test",
            "gauge",
            "help with a \\ backslash\nand a newline",
        );
        e.sample("quorum_test", &[("path", "a\"b\\c\nd")], 1);
        let (s, _) = parse(&e.out).unwrap_or_else(|m| panic!("{m}\n{}", e.out));
        assert_eq!(
            s[0].labels["path"], "a\"b\\c\nd",
            "the value survives a round trip"
        );
        assert_eq!(
            e.out.lines().count(),
            3,
            "a newline in a value did not add a line"
        );
    }

    #[test]
    fn the_reader_used_to_check_the_output_really_rejects_malformed_text() {
        for bad in [
            "quorum_x 1\n",                                           // no TYPE
            "# HELP a h\n# TYPE a gauge\na{b=c} 1\n",                 // unquoted label
            "# HELP a h\n# TYPE a gauge\na 1\n\n",                    // blank line
            "# HELP a_total h\n# TYPE a_total counter\na_total -1\n", // negative counter
            "# HELP a h\n# TYPE a counter\na 1\n",                    // counter without _total
            "# HELP a h\n# TYPE a gauge\n# TYPE a gauge\na 1\n",      // TYPE twice
            "# TYPE a gauge\na 1\n",                                  // TYPE without HELP
        ] {
            assert!(parse(bad).is_err(), "accepted {bad:?}");
        }
        let broken_histogram = "# HELP h h\n# TYPE h histogram\n\
             h_bucket{le=\"1\"} 5\nh_bucket{le=\"+Inf\"} 3\nh_sum 1\nh_count 3\n";
        let (s, _) = parse(broken_histogram).unwrap();
        assert!(check_histograms(&s).is_err(), "buckets must be cumulative");
    }
}

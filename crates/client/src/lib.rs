//! A client for a Quorum cluster.
//!
//! It finds the leader on its own, follows redirects, and, unlike a bare HTTP call,
//! tells the caller exactly what is known about a request that did not succeed:
//!
//! * [`ClientError::Rejected`], [`ClientError::InvalidKey`]: the request can never
//!   succeed as sent. Nothing was applied. It is not retried.
//! * [`ClientError::Unavailable`], [`ClientError::NoLeaderReachable`]: nothing was
//!   applied, and trying again later may work. The client has already retried with
//!   backoff before reporting these.
//! * [`ClientError::OutcomeUnknown`]: a write or membership change was sent and the
//!   cluster did not say whether it took effect. It may have. The client does NOT
//!   resend it by default (see [`ClientConfig::retry_unknown_writes`]); the caller
//!   decides, because only the caller knows whether doing it twice is harmless.

pub mod leader_tracking;

use leader_tracking::LeaderTracker;
use raft_core::NodeId;
use reqwest::header::{ACCEPT, CONTENT_TYPE, RETRY_AFTER};
use reqwest::Url;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

const RAW: &str = "application/octet-stream";

/// How the client behaves when things go wrong.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// How long to wait for one node to answer one request. The cluster's own request
    /// timeout (3 seconds by default) is shorter, so a healthy node answers, if only
    /// with an error, before this fires. Membership changes get ten times as long.
    pub request_timeout: Duration,
    /// How many times to go through the list of nodes before giving up on a request
    /// that keeps meeting retryable conditions (an overloaded node, an election in
    /// progress, no node reachable).
    pub max_attempts: u32,
    /// The pause before the second pass; it doubles each time, up to `max_backoff`.
    pub initial_backoff: Duration,
    /// The longest pause between passes. A `Retry-After` from the cluster is honoured
    /// but never waited for longer than this.
    pub max_backoff: Duration,
    /// Whether to resend a write (or membership change) whose outcome is unknown.
    ///
    /// Off by default, and for a reason. The cluster does not recognise a repeated
    /// request: if the first copy did take effect and something else wrote the key since,
    /// resending puts the OLD value back on top of the newer one. Turn this on only when
    /// that cannot matter, for example when every write to a key carries the full value
    /// and nothing else writes it.
    pub retry_unknown_writes: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            max_attempts: 5,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(2),
            retry_unknown_writes: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The key breaks the store's rules. Nothing was sent.
    #[error("invalid key: {0}")]
    InvalidKey(String),
    /// The cluster refused the request and will refuse it again unchanged: a bad
    /// request (400), an oversized value (413), an unsupported content type (415) or a
    /// conflicting membership change (409). Nothing was applied.
    #[error("the cluster rejected the request ({status}): {message}")]
    Rejected { status: u16, message: String },
    /// The request was sent but the cluster did not say whether it took effect; it may
    /// or may not have. Check before doing it again (see `members` for membership
    /// changes), or read the key.
    #[error("the outcome is unknown, the request may or may not have taken effect: {message}")]
    OutcomeUnknown { message: String },
    /// The cluster is up but could not take the request (overloaded, still electing a
    /// leader, shutting down). Nothing was applied. The client retried with backoff
    /// before giving up.
    #[error("the cluster could not take the request right now: {message}")]
    Unavailable { message: String },
    /// No node answered usefully. Nothing was applied.
    #[error("no reachable node responded as leader after trying the whole cluster ({detail})")]
    NoLeaderReachable { detail: String },
    /// `get` was used on a value that is not valid UTF-8. Use `get_bytes`.
    #[error("the value is not valid UTF-8 text; read it with get_bytes")]
    NotUtf8,
}

/// What an operation asks of the cluster.
#[derive(Debug, Clone)]
enum Op {
    Get(String),
    Put(String, Vec<u8>),
    Delete(String),
    AddMember { id: NodeId, addr: String },
    RemoveMember(NodeId),
}

impl Op {
    /// Whether giving up on it half way leaves the outcome unknown.
    fn changes_state(&self) -> bool {
        !matches!(self, Op::Get(_))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Output {
    Value(Option<Vec<u8>>),
    Applied,
    Members(Vec<NodeId>),
}

/// An HTTP answer, before it is interpreted.
#[derive(Debug)]
struct Raw {
    status: u16,
    retry_after: Option<Duration>,
    body: Vec<u8>,
}

/// What one answer means for the request that got it.
#[derive(Debug)]
enum Verdict {
    /// The request is done.
    Done(Output),
    /// This node is not the leader; the hint, if any, says who is.
    NotLeader { hint: Option<NodeId> },
    /// The node did not take the request and nothing was applied: try again later.
    Busy { retry_after: Option<Duration>, message: String },
    /// The request may have taken effect.
    Unknown { message: String },
    /// The request can never succeed as sent.
    Fatal(ClientError),
    /// This answer is no use (garbage, a server error): try the next node.
    Skip { detail: String },
}

#[derive(Deserialize)]
struct ErrorBody {
    #[serde(default)]
    error: String,
    #[serde(default)]
    outcome_unknown: bool,
    #[serde(default)]
    leader_hint: Option<u32>,
}

#[derive(Deserialize)]
struct WriteBody {
    is_leader: bool,
    #[serde(default)]
    leader_hint: Option<u32>,
    #[serde(default)]
    members: Vec<NodeId>,
}

#[derive(Deserialize)]
struct HealthBody {
    members: Vec<NodeId>,
}

fn status_text(raw: &Raw) -> String {
    match serde_json::from_slice::<ErrorBody>(&raw.body) {
        Ok(body) if !body.error.is_empty() => body.error,
        _ => format!("HTTP {}", raw.status),
    }
}

/// Decides what an answer means. Pure, so every rule is testable without a network.
fn classify(op: &Op, raw: &Raw) -> Verdict {
    let hint = |raw: &Raw| {
        serde_json::from_slice::<ErrorBody>(&raw.body).ok().and_then(|b| b.leader_hint)
    };
    match raw.status {
        200 => match op {
            Op::Get(_) => Verdict::Done(Output::Value(Some(raw.body.clone()))),
            _ => match serde_json::from_slice::<WriteBody>(&raw.body) {
                Ok(body) if body.is_leader => Verdict::Done(match op {
                    Op::AddMember { .. } | Op::RemoveMember(_) => Output::Members(body.members),
                    _ => Output::Applied,
                }),
                Ok(body) => Verdict::NotLeader { hint: body.leader_hint },
                Err(e) => Verdict::Skip { detail: format!("an answer that is not understood: {e}") },
            },
        },
        // A raw read of a key that does not exist, from the leader.
        404 if matches!(op, Op::Get(_)) && status_text(raw).starts_with("key not found") => {
            Verdict::Done(Output::Value(None))
        }
        // A raw read sent to a node that is not the leader.
        421 => Verdict::NotLeader { hint: hint(raw).map(|h| h as NodeId) },
        503 | 504 => match serde_json::from_slice::<ErrorBody>(&raw.body) {
            Ok(body) if body.outcome_unknown && op.changes_state() => {
                Verdict::Unknown { message: body.error }
            }
            // Includes a read that timed out inside the cluster: reads change nothing.
            Ok(body) => Verdict::Busy { retry_after: raw.retry_after, message: body.error },
            Err(_) => Verdict::Busy { retry_after: raw.retry_after, message: format!("HTTP {}", raw.status) },
        },
        400 | 404 | 405 | 409 | 413 | 415 | 422 => Verdict::Fatal(ClientError::Rejected {
            status: raw.status,
            message: status_text(raw),
        }),
        other => Verdict::Skip { detail: format!("HTTP {other}") },
    }
}

/// How long to pause before pass number `attempt + 1`: the initial pause doubled each
/// time, capped, raised to the cluster's `Retry-After` if it gave one (still capped),
/// then spread by up to 20% either way so many clients do not retry in step.
/// `jitter` is a number in 0..1 supplied by the caller (random in real use).
fn backoff_delay(config: &ClientConfig, attempt: u32, hint: Option<Duration>, jitter: f64) -> Duration {
    let exponential = config
        .initial_backoff
        .saturating_mul(2u32.saturating_pow(attempt.min(16)))
        .min(config.max_backoff);
    let base = hint.map_or(exponential, |h| exponential.max(h)).min(config.max_backoff);
    let spread = 0.8 + 0.4 * jitter.clamp(0.0, 1.0);
    base.mul_f64(spread)
}

/// A cheap, good-enough source of jitter: the sub-second part of the clock.
fn jitter() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    f64::from(nanos % 1000) / 1000.0
}

/// Why a request never produced an answer.
struct SendError {
    /// True if the connection could not even be made, so the request certainly was not
    /// processed. False once it was sent (for example it timed out waiting), when a
    /// write may have taken effect.
    connect_failed: bool,
    message: String,
}

pub struct QuorumClient {
    http: reqwest::Client,
    tracker: LeaderTracker,
    config: ClientConfig,
}

impl QuorumClient {
    /// `nodes` maps each node's id to its metrics API address, e.g.
    /// `{1: "127.0.0.1:8001", 2: "127.0.0.1:8002", 3: "127.0.0.1:8003"}`.
    pub fn new(nodes: HashMap<NodeId, String>) -> Self {
        Self::with_config(nodes, ClientConfig::default())
    }

    pub fn with_config(nodes: HashMap<NodeId, String>, config: ClientConfig) -> Self {
        let http = reqwest::Client::builder()
            .build()
            .expect("reqwest client builds with a static config");
        Self { http, tracker: LeaderTracker::new(nodes), config }
    }

    /// Reads a value as text. A value that is not valid UTF-8 is an error
    /// ([`ClientError::NotUtf8`]), never silently altered: use [`get_bytes`](Self::get_bytes).
    pub async fn get(&self, key: &str) -> Result<Option<String>, ClientError> {
        match self.get_bytes(key).await? {
            None => Ok(None),
            Some(bytes) => String::from_utf8(bytes).map(Some).map_err(|_| ClientError::NotUtf8),
        }
    }

    /// Reads a value exactly as it was written: any bytes at all. `None` means the key
    /// does not exist.
    pub async fn get_bytes(&self, key: &str) -> Result<Option<Vec<u8>>, ClientError> {
        check_key(key)?;
        match self.execute(Op::Get(key.to_string())).await? {
            Output::Value(value) => Ok(value),
            _ => unreachable!("a read answers with a value"),
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> Result<(), ClientError> {
        self.put_bytes(key, value.as_bytes()).await
    }

    /// Writes any bytes. Returns once the write is committed on a majority.
    pub async fn put_bytes(&self, key: &str, value: impl AsRef<[u8]>) -> Result<(), ClientError> {
        check_key(key)?;
        self.execute(Op::Put(key.to_string(), value.as_ref().to_vec())).await.map(|_| ())
    }

    pub async fn delete(&self, key: &str) -> Result<(), ClientError> {
        check_key(key)?;
        self.execute(Op::Delete(key.to_string())).await.map(|_| ())
    }

    /// Adds a node to the cluster and returns the members once it has taken effect.
    /// The new node must already be running with `QUORUM_JOIN=true`. This can take a
    /// while: the node is brought up to date first.
    pub async fn add_member(&self, id: NodeId, addr: &str) -> Result<Vec<NodeId>, ClientError> {
        match self.execute(Op::AddMember { id, addr: addr.to_string() }).await? {
            Output::Members(members) => Ok(members),
            _ => unreachable!("a membership change answers with the members"),
        }
    }

    /// Removes a node from the cluster and returns the members once it has taken effect.
    pub async fn remove_member(&self, id: NodeId) -> Result<Vec<NodeId>, ClientError> {
        match self.execute(Op::RemoveMember(id)).await? {
            Output::Members(members) => Ok(members),
            _ => unreachable!("a membership change answers with the members"),
        }
    }

    /// The members according to the first node that answers. Useful to find out what
    /// became of a membership change whose outcome was unknown.
    pub async fn members(&self) -> Result<Vec<NodeId>, ClientError> {
        let mut detail = String::new();
        for (id, addr) in self.candidate_order() {
            let url = match Url::parse(&format!("http://{addr}/health")) {
                Ok(url) => url,
                Err(e) => {
                    detail = format!("node {id}: bad address: {e}");
                    continue;
                }
            };
            match self.http.get(url).timeout(self.config.request_timeout).send().await {
                Ok(resp) if resp.status().is_success() => match resp.json::<HealthBody>().await {
                    Ok(body) => return Ok(body.members),
                    Err(e) => detail = format!("node {id}: {e}"),
                },
                Ok(resp) => detail = format!("node {id}: HTTP {}", resp.status().as_u16()),
                Err(e) => detail = format!("node {id}: {e}"),
            }
        }
        Err(ClientError::NoLeaderReachable { detail })
    }

    /// Runs one operation against the cluster, following redirects and retrying what is
    /// safe to retry, until it is done or it is clear that it cannot be.
    async fn execute(&self, op: Op) -> Result<Output, ClientError> {
        let mut last_detail = String::from("no node was tried");
        let mut last_busy: Option<String> = None;

        for attempt in 0..self.config.max_attempts.max(1) {
            let mut wait_hint: Option<Duration> = None;
            last_busy = None;

            for (node_id, addr) in self.candidate_order() {
                let raw = match self.send(&op, &addr).await {
                    Ok(raw) => raw,
                    Err(e) if e.connect_failed || !op.changes_state() => {
                        // The request never reached the node, or it only reads: the next
                        // node is the right thing to try.
                        last_detail = format!("node {node_id}: {}", e.message);
                        continue;
                    }
                    Err(e) => {
                        // Sent, but no answer: a write may have taken effect.
                        let message = format!("node {node_id} did not answer in time: {}", e.message);
                        if self.config.retry_unknown_writes {
                            last_detail = message;
                            continue;
                        }
                        return Err(ClientError::OutcomeUnknown { message });
                    }
                };

                match classify(&op, &raw) {
                    Verdict::Done(output) => {
                        self.tracker.record_confirmed_leader(node_id);
                        return Ok(output);
                    }
                    Verdict::NotLeader { hint } => {
                        self.tracker.record_not_leader(hint.map(|h| h as NodeId));
                    }
                    Verdict::Busy { retry_after, message } => {
                        wait_hint = wait_hint.max(retry_after);
                        last_busy = Some(message);
                    }
                    Verdict::Unknown { message } => {
                        if self.config.retry_unknown_writes {
                            last_detail = message;
                            continue;
                        }
                        return Err(ClientError::OutcomeUnknown { message });
                    }
                    Verdict::Fatal(error) => return Err(error),
                    Verdict::Skip { detail } => last_detail = format!("node {node_id}: {detail}"),
                }
            }

            if attempt + 1 < self.config.max_attempts {
                tokio::time::sleep(backoff_delay(&self.config, attempt, wait_hint, jitter())).await;
            }
        }

        match last_busy {
            Some(message) => Err(ClientError::Unavailable { message }),
            None => Err(ClientError::NoLeaderReachable { detail: last_detail }),
        }
    }

    async fn send(&self, op: &Op, addr: &str) -> Result<Raw, SendError> {
        let fail = |message: String| SendError { connect_failed: false, message };
        let mut url = Url::parse(&format!("http://{addr}"))
            .map_err(|e| fail(format!("bad node address {addr:?}: {e}")))?;
        // `extend` percent-encodes each segment, so a key can hold `/`, `?`, `#`, `%`
        // or any text without changing which key is addressed.
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| fail(format!("bad node address {addr:?}")))?;
            match op {
                Op::Get(key) | Op::Put(key, _) | Op::Delete(key) => {
                    segments.extend(["kv", key.as_str()]);
                }
                Op::AddMember { .. } => {
                    segments.extend(["admin", "members"]);
                }
                Op::RemoveMember(id) => {
                    segments.extend(["admin", "members", &id.to_string()]);
                }
            }
        }

        let timeout = match op {
            Op::AddMember { .. } | Op::RemoveMember(_) => self.config.request_timeout * 10,
            _ => self.config.request_timeout,
        };
        let request = match op {
            Op::Get(_) => self.http.get(url).header(ACCEPT, RAW),
            Op::Put(_, value) => self.http.put(url).header(CONTENT_TYPE, RAW).body(value.clone()),
            Op::Delete(_) => self.http.delete(url),
            Op::AddMember { id, addr } => {
                self.http.post(url).json(&serde_json::json!({ "id": id, "addr": addr }))
            }
            Op::RemoveMember(_) => self.http.delete(url),
        };

        let response = request.timeout(timeout).send().await.map_err(|e| SendError {
            connect_failed: e.is_connect(),
            message: e.to_string(),
        })?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let body = response.bytes().await.map_err(|e| SendError {
            connect_failed: false,
            message: format!("the answer was cut short: {e}"),
        })?;
        Ok(Raw { status, retry_after, body: body.to_vec() })
    }

    /// Current best guess first (if any), then every other node as fallback -
    /// so a wrong guess costs at most one wasted round-trip before the client
    /// finds the real leader.
    fn candidate_order(&self) -> Vec<(NodeId, String)> {
        let mut order = Vec::with_capacity(self.tracker.node_count());
        let guess = self.tracker.best_guess();
        if let Some((id, addr)) = &guess {
            order.push((*id, addr.clone()));
        }
        order.extend(self.tracker.fallback_order(guess.map(|(id, _)| id)));
        order
    }
}

fn check_key(key: &str) -> Result<(), ClientError> {
    storage::validate_key(key).map_err(|e| ClientError::InvalidKey(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(status: u16, body: &str) -> Raw {
        Raw { status, retry_after: None, body: body.as_bytes().to_vec() }
    }

    fn get() -> Op {
        Op::Get("k".into())
    }

    fn put() -> Op {
        Op::Put("k".into(), b"v".to_vec())
    }

    #[test]
    fn a_successful_write_from_the_leader_is_done() {
        let verdict = classify(&put(), &raw(200, r#"{"is_leader":true,"applied":true,"leader_hint":null}"#));
        assert!(matches!(verdict, Verdict::Done(Output::Applied)));
    }

    #[test]
    fn a_write_answered_by_a_follower_follows_the_hint() {
        let verdict = classify(&put(), &raw(200, r#"{"is_leader":false,"applied":false,"leader_hint":3}"#));
        assert!(matches!(verdict, Verdict::NotLeader { hint: Some(3) }));
        let verdict = classify(&put(), &raw(200, r#"{"is_leader":false,"applied":false,"leader_hint":null}"#));
        assert!(matches!(verdict, Verdict::NotLeader { hint: None }));
    }

    #[test]
    fn a_raw_read_returns_the_body_untouched() {
        let verdict = classify(&get(), &Raw { status: 200, retry_after: None, body: vec![0, 255, 7] });
        assert!(matches!(verdict, Verdict::Done(Output::Value(Some(v))) if v == vec![0, 255, 7]));
    }

    #[test]
    fn a_missing_key_is_none_but_only_when_the_leader_says_so() {
        let ok = classify(&get(), &raw(404, r#"{"error":"key not found","outcome_unknown":false}"#));
        assert!(matches!(ok, Verdict::Done(Output::Value(None))));
        // Some other 404 (a proxy, a wrong path) is not "the key does not exist".
        let other = classify(&get(), &raw(404, "<html>not found</html>"));
        assert!(matches!(other, Verdict::Fatal(ClientError::Rejected { status: 404, .. })));
    }

    #[test]
    fn a_raw_read_on_a_follower_follows_the_hint() {
        let verdict = classify(&get(), &raw(421, r#"{"error":"x","is_leader":false,"leader_hint":2}"#));
        assert!(matches!(verdict, Verdict::NotLeader { hint: Some(2) }));
    }

    #[test]
    fn an_overloaded_node_is_busy_and_nothing_was_applied() {
        let mut answer = raw(503, r#"{"error":"overloaded","outcome_unknown":false}"#);
        answer.retry_after = Some(Duration::from_secs(1));
        match classify(&put(), &answer) {
            Verdict::Busy { retry_after, .. } => assert_eq!(retry_after, Some(Duration::from_secs(1))),
            other => panic!("expected Busy, got {other:?}"),
        }
    }

    #[test]
    fn a_write_with_an_unknown_outcome_is_never_treated_as_busy() {
        for status in [503, 504] {
            let verdict = classify(&put(), &raw(status, r#"{"error":"lost leadership","outcome_unknown":true}"#));
            assert!(matches!(verdict, Verdict::Unknown { .. }), "{status}");
        }
        // The same for a delete and for a membership change.
        let body = raw(504, r#"{"error":"timed out","outcome_unknown":true}"#);
        assert!(matches!(classify(&Op::Delete("k".into()), &body), Verdict::Unknown { .. }));
        assert!(matches!(classify(&Op::RemoveMember(2), &body), Verdict::Unknown { .. }));
    }

    #[test]
    fn a_read_that_timed_out_inside_the_cluster_is_just_busy() {
        let verdict = classify(&get(), &raw(504, r#"{"error":"timed out","outcome_unknown":false}"#));
        assert!(matches!(verdict, Verdict::Busy { .. }));
    }

    #[test]
    fn a_503_with_no_readable_body_is_busy_not_unknown() {
        let verdict = classify(&put(), &raw(503, "Service Unavailable"));
        assert!(matches!(verdict, Verdict::Busy { .. }), "a proxy's error page says nothing about the write");
    }

    #[test]
    fn requests_that_can_never_succeed_are_fatal_with_the_servers_reason() {
        for status in [400, 409, 413, 415] {
            match classify(&put(), &raw(status, r#"{"error":"the reason","outcome_unknown":false}"#)) {
                Verdict::Fatal(ClientError::Rejected { status: s, message }) => {
                    assert_eq!((s, message.as_str()), (status, "the reason"));
                }
                other => panic!("{status}: expected Fatal, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_membership_change_answers_with_the_members() {
        let body = raw(200, r#"{"is_leader":true,"applied":true,"leader_hint":null,"members":[1,2,3,4]}"#);
        assert!(matches!(
            classify(&Op::AddMember { id: 4, addr: "x:1".into() }, &body),
            Verdict::Done(Output::Members(m)) if m == vec![1, 2, 3, 4]
        ));
    }

    #[test]
    fn other_server_errors_and_garbage_just_try_the_next_node() {
        assert!(matches!(classify(&put(), &raw(500, "boom")), Verdict::Skip { .. }));
        assert!(matches!(classify(&put(), &raw(502, "")), Verdict::Skip { .. }));
        assert!(matches!(classify(&put(), &raw(200, "not json")), Verdict::Skip { .. }));
    }

    fn config() -> ClientConfig {
        ClientConfig {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            ..ClientConfig::default()
        }
    }

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let delays: Vec<u128> =
            (0..6).map(|a| backoff_delay(&config(), a, None, 0.5).as_millis()).collect();
        assert_eq!(delays, vec![100, 200, 400, 800, 1000, 1000]);
    }

    #[test]
    fn the_clusters_retry_after_is_honoured_but_capped() {
        let wait = |hint| backoff_delay(&config(), 0, Some(hint), 0.5).as_millis();
        assert_eq!(wait(Duration::from_millis(600)), 600, "longer than the doubling would give");
        assert_eq!(wait(Duration::from_millis(10)), 100, "never shorter than the doubling");
        assert_eq!(wait(Duration::from_secs(30)), 1000, "never longer than the cap");
    }

    #[test]
    fn jitter_spreads_the_delay_by_a_fifth_either_way() {
        let at = |j| backoff_delay(&config(), 1, None, j).as_millis();
        assert_eq!((at(0.0), at(0.5), at(1.0)), (160, 200, 240));
    }

    #[test]
    fn a_huge_attempt_number_cannot_overflow() {
        let _ = backoff_delay(&config(), u32::MAX, None, 0.5);
    }

    #[test]
    fn bad_keys_are_rejected_before_anything_is_sent() {
        assert!(matches!(check_key(""), Err(ClientError::InvalidKey(_))));
        assert!(matches!(check_key("a\0b"), Err(ClientError::InvalidKey(_))));
        assert!(matches!(check_key(&"k".repeat(600)), Err(ClientError::InvalidKey(_))));
        assert!(check_key("fine key/with?odd#chars%").is_ok());
    }
}

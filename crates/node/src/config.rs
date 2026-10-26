use raft_core::NodeId;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// How long an HTTP request may wait for the cluster before the API gives up and
/// answers with a 504. Deliberately shorter than the client SDK's own 5 s
/// timeout, so a client hears a definite "timed out, outcome unknown" from the
/// server instead of giving up on a connection it can no longer interpret.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(3000);

/// How this node answers reads.
///
/// `Linearizable` (the default) confirms with a majority that this node is still
/// the leader before answering, so a read can never return data that a newer
/// leader has already overwritten. It costs one extra round trip to the followers.
///
/// `Local` answers straight from this node's own applied state. It is faster, but
/// a leader that has just been cut off from the cluster, and does not yet know it,
/// can return a stale value. Opt in with `QUORUM_LOCAL_READS=true` only if that
/// trade is acceptable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadMode {
    Linearizable,
    Local,
}

/// Default capacity of every queue between the parts of a node (client requests,
/// inbound peer messages, each peer's outbound messages), and the most client
/// requests one node will hold in flight.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1024;

/// Default most requests handled together as one batch (one disk sync, one round
/// of replication).
pub const DEFAULT_MAX_BATCH: usize = 128;

/// Sizes that bound how much work a node will hold at once. Every queue is
/// bounded so that a node under more load than it can carry pushes back on its
/// callers (an HTTP 503 with `Retry-After`, or TCP backpressure from a peer)
/// instead of growing memory until it falls over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Capacity of each internal queue, and the cap on writes (and on reads)
    /// waiting for the cluster at any moment.
    pub channel_capacity: usize,
    /// Most queued requests taken together as one batch: they share a single
    /// `fsync` and a single replication round.
    pub max_batch: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self { channel_capacity: DEFAULT_CHANNEL_CAPACITY, max_batch: DEFAULT_MAX_BATCH }
    }
}

/// Static configuration for one node. Loaded from environment variables
/// rather than a config file so docker-compose (Phase 11) can set each
/// node up purely through its `environment:` block.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub id: NodeId,
    pub listen_addr: String,
    pub metrics_addr: String,
    pub peers: HashMap<NodeId, String>,
    pub storage_dir: PathBuf,
    pub read_mode: ReadMode,
    pub limits: Limits,
}

impl NodeConfig {
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Expected vars:
    ///   QUORUM_NODE_ID=1
    ///   QUORUM_LISTEN_ADDR=0.0.0.0:7000
    ///   QUORUM_METRICS_ADDR=0.0.0.0:8000
    ///   QUORUM_PEERS=2=node2:7000,3=node3:7000
    ///   QUORUM_STORAGE_DIR=/data
    ///   QUORUM_REQUEST_TIMEOUT_MS=3000   (optional, see `request_timeout_from_env`)
    ///   QUORUM_LOCAL_READS=false         (optional; true = fast but possibly stale reads)
    ///   QUORUM_CHANNEL_CAPACITY=1024     (optional; size of every internal queue)
    ///   QUORUM_MAX_BATCH=128             (optional; most requests per disk sync)
    pub fn from_env() -> anyhow::Result<Self> {
        let id: NodeId = std::env::var("QUORUM_NODE_ID")
            .map_err(|_| anyhow::anyhow!("QUORUM_NODE_ID not set"))?
            .parse()?;
        let listen_addr =
            std::env::var("QUORUM_LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:7000".to_string());
        let metrics_addr =
            std::env::var("QUORUM_METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:8000".to_string());
        let storage_dir: PathBuf =
            std::env::var("QUORUM_STORAGE_DIR").unwrap_or_else(|_| "./data".to_string()).into();

        let mut peers = HashMap::new();
        if let Ok(raw) = std::env::var("QUORUM_PEERS") {
            for entry in raw.split(',').filter(|s| !s.is_empty()) {
                let (id_str, addr) = entry
                    .split_once('=')
                    .ok_or_else(|| anyhow::anyhow!("bad QUORUM_PEERS entry: {entry}"))?;
                peers.insert(id_str.parse()?, addr.to_string());
            }
        }

        let read_mode = parse_read_mode(std::env::var("QUORUM_LOCAL_READS").ok().as_deref())?;

        let limits = limits_from_env()?;

        Ok(Self { id, listen_addr, metrics_addr, peers, storage_dir, read_mode, limits })
    }
}

/// Reads `QUORUM_CHANNEL_CAPACITY` and `QUORUM_MAX_BATCH`.
pub fn limits_from_env() -> anyhow::Result<Limits> {
    Ok(Limits {
        channel_capacity: parse_limit(
            "QUORUM_CHANNEL_CAPACITY",
            std::env::var("QUORUM_CHANNEL_CAPACITY").ok().as_deref(),
            DEFAULT_CHANNEL_CAPACITY,
        )?,
        max_batch: parse_limit(
            "QUORUM_MAX_BATCH",
            std::env::var("QUORUM_MAX_BATCH").ok().as_deref(),
            DEFAULT_MAX_BATCH,
        )?,
    })
}

/// Parses a size setting: a whole number of at least 1 (a queue of zero slots
/// cannot hold anything, and a batch of zero would never make progress), or the
/// default when unset.
pub fn parse_limit(name: &str, raw: Option<&str>, default: usize) -> anyhow::Result<usize> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let value: usize = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("{name} must be a whole number, got {raw:?}"))?;
    if value == 0 {
        anyhow::bail!("{name} must be at least 1");
    }
    Ok(value)
}

/// Parses `QUORUM_LOCAL_READS`. Unset means linearizable reads; `true`/`1` opts in
/// to local (possibly stale) reads; `false`/`0` explicitly asks for the default.
/// Anything else is an error rather than a guess, because silently picking the
/// weaker consistency level on a typo would be the wrong way to fail.
pub fn parse_read_mode(raw: Option<&str>) -> anyhow::Result<ReadMode> {
    match raw.map(|s| s.trim().to_ascii_lowercase()) {
        None => Ok(ReadMode::Linearizable),
        Some(value) => match value.as_str() {
            "true" | "1" => Ok(ReadMode::Local),
            "false" | "0" => Ok(ReadMode::Linearizable),
            _ => anyhow::bail!(
                "QUORUM_LOCAL_READS must be true, false, 1 or 0, got {:?}",
                raw.unwrap_or("")
            ),
        },
    }
}

/// Reads `QUORUM_REQUEST_TIMEOUT_MS` (milliseconds), defaulting to
/// [`DEFAULT_REQUEST_TIMEOUT`].
pub fn request_timeout_from_env() -> anyhow::Result<Duration> {
    parse_request_timeout(std::env::var("QUORUM_REQUEST_TIMEOUT_MS").ok().as_deref())
}

/// The parsing half of [`request_timeout_from_env`], separate so it can be
/// tested without touching the process environment.
pub fn parse_request_timeout(raw: Option<&str>) -> anyhow::Result<Duration> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_REQUEST_TIMEOUT);
    };
    let millis: u64 = raw
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("QUORUM_REQUEST_TIMEOUT_MS must be a whole number, got {raw:?}"))?;
    if millis == 0 {
        anyhow::bail!("QUORUM_REQUEST_TIMEOUT_MS must be greater than zero");
    }
    Ok(Duration::from_millis(millis))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_timeout_defaults_when_unset() {
        assert_eq!(parse_request_timeout(None).unwrap(), DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn request_timeout_parses_milliseconds() {
        assert_eq!(parse_request_timeout(Some("1500")).unwrap(), Duration::from_millis(1500));
        assert_eq!(parse_request_timeout(Some(" 250 ")).unwrap(), Duration::from_millis(250));
    }

    #[test]
    fn request_timeout_rejects_zero_and_garbage() {
        assert!(parse_request_timeout(Some("0")).is_err());
        assert!(parse_request_timeout(Some("soon")).is_err());
        assert!(parse_request_timeout(Some("-5")).is_err());
        assert!(parse_request_timeout(Some("")).is_err());
    }

    #[test]
    fn reads_are_linearizable_unless_local_reads_are_requested() {
        assert_eq!(parse_read_mode(None).unwrap(), ReadMode::Linearizable);
        assert_eq!(parse_read_mode(Some("false")).unwrap(), ReadMode::Linearizable);
        assert_eq!(parse_read_mode(Some("0")).unwrap(), ReadMode::Linearizable);
    }

    #[test]
    fn local_reads_must_be_asked_for_explicitly() {
        assert_eq!(parse_read_mode(Some("true")).unwrap(), ReadMode::Local);
        assert_eq!(parse_read_mode(Some(" TRUE ")).unwrap(), ReadMode::Local);
        assert_eq!(parse_read_mode(Some("1")).unwrap(), ReadMode::Local);
    }

    #[test]
    fn a_typo_in_the_read_mode_is_an_error_not_a_silent_downgrade() {
        assert!(parse_read_mode(Some("ture")).is_err());
        assert!(parse_read_mode(Some("")).is_err());
        assert!(parse_read_mode(Some("yes")).is_err());
    }

    #[test]
    fn limits_default_when_unset() {
        assert_eq!(parse_limit("X", None, 1024).unwrap(), 1024);
        assert_eq!(Limits::default().channel_capacity, DEFAULT_CHANNEL_CAPACITY);
        assert_eq!(Limits::default().max_batch, DEFAULT_MAX_BATCH);
    }

    #[test]
    fn limits_parse_whole_numbers() {
        assert_eq!(parse_limit("X", Some("64"), 1024).unwrap(), 64);
        assert_eq!(parse_limit("X", Some(" 7 "), 1024).unwrap(), 7);
    }

    #[test]
    fn a_limit_of_zero_or_garbage_is_rejected() {
        assert!(parse_limit("X", Some("0"), 1024).is_err());
        assert!(parse_limit("X", Some("many"), 1024).is_err());
        assert!(parse_limit("X", Some("-3"), 1024).is_err());
        assert!(parse_limit("X", Some(""), 1024).is_err());
    }

    #[test]
    fn a_rejected_limit_names_the_setting() {
        let message = parse_limit("QUORUM_MAX_BATCH", Some("0"), 128).unwrap_err().to_string();
        assert!(message.contains("QUORUM_MAX_BATCH"), "{message}");
    }
}
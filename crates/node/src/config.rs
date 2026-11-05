use raft_core::{Membership, NodeId};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// How long an HTTP request may wait for the cluster before the API gives up and
/// answers with a 504. Deliberately shorter than the client SDK's own 5 s
/// timeout, so a client hears a definite "timed out, outcome unknown" from the
/// server instead of giving up on a connection it can no longer interpret.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_millis(3000);

/// How long a node being added to the cluster may take to catch up on the log
/// before the leader gives up on it and abandons the request. A node that never
/// catches up (wrong address, crashed, far too slow) must not be able to hold the
/// single membership-change slot forever.
pub const DEFAULT_CATCH_UP_TIMEOUT: Duration = Duration::from_millis(20_000);

/// Snapshot after this many applied entries, by default. A snapshot bounds the disk the
/// log uses and the time a restart spends replaying it; a follower or new node that has
/// fallen behind the compacted part of the log is brought up to date with the snapshot.
pub const DEFAULT_SNAPSHOT_THRESHOLD: u64 = 10_000;

/// How big each piece of a snapshot sent to a follower is, by default.
pub const DEFAULT_SNAPSHOT_CHUNK_BYTES: u64 = 64 * 1024;

/// The largest value, in bytes, the HTTP API accepts by default. A value travels in a log
/// entry, is copied to every node, and is held in memory (and in every snapshot), so very
/// large ones belong in an object store with a pointer here.
pub const DEFAULT_MAX_VALUE_BYTES: usize = 1024 * 1024;

/// How many log entries are kept behind a snapshot, by default, so that a follower
/// that is only slightly behind can still be sent entries rather than a snapshot.
pub const DEFAULT_SNAPSHOT_RETAIN: u64 = 1000;

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
    /// True for a node that is being ADDED to a cluster that already exists. Such
    /// a node starts as a non-voter: it must not hold elections of its own (it
    /// would be a one-node majority of nothing) until the cluster's log tells it
    /// it is a member. `peers` then lists the current members, so it can answer
    /// the leader that sends it the log.
    pub join: bool,
    /// How long a joining node gets to catch up before the leader abandons it.
    pub catch_up_timeout: Duration,
    /// Take a snapshot once this many entries have been applied since the last one.
    /// 0 turns snapshots, and with them log compaction, off.
    pub snapshot_threshold: u64,
    /// Size of each piece a snapshot is sent in when a follower needs it.
    pub snapshot_chunk_bytes: usize,
    /// How many entries to keep in the log behind the newest snapshot.
    pub snapshot_retain: u64,
}

impl NodeConfig {
    /// The membership this node starts from, before any configuration entry in
    /// its log is applied: the configured peers plus this node itself, or, for a
    /// joining node, the peers alone. The log is the source of truth from there on;
    /// this is only the bootstrap configuration the first entries build upon, so
    /// it must stay the same across restarts of the same node.
    pub fn base_membership(&self) -> Membership {
        let mut members: Vec<(NodeId, String)> =
            self.peers.iter().map(|(&id, addr)| (id, addr.clone())).collect();
        if !self.join {
            members.push((self.id, self.listen_addr.clone()));
        }
        Membership::new(members)
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
    ///   QUORUM_JOIN=true                 (optional; this node is being added to an
    ///                                     existing cluster, PEERS lists its members)
    ///   QUORUM_CATCH_UP_TIMEOUT_MS=20000 (optional; how long a joining node gets
    ///                                     to catch up with the leader)
    ///   QUORUM_SNAPSHOT_THRESHOLD=10000  (optional; snapshot after this many applied
    ///                                     entries; 0 = never)
    ///   QUORUM_SNAPSHOT_CHUNK_BYTES=65536 (optional; size of each piece of a snapshot
    ///                                     sent to a follower that needs it)
    ///   QUORUM_SNAPSHOT_RETAIN=1000      (optional; log entries kept behind a snapshot)
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
        let join = parse_join(std::env::var("QUORUM_JOIN").ok().as_deref())?;
        let catch_up_timeout =
            parse_catch_up_timeout(std::env::var("QUORUM_CATCH_UP_TIMEOUT_MS").ok().as_deref())?;

        let snapshot_threshold = parse_count(
            "QUORUM_SNAPSHOT_THRESHOLD",
            std::env::var("QUORUM_SNAPSHOT_THRESHOLD").ok().as_deref(),
            DEFAULT_SNAPSHOT_THRESHOLD,
        )?;
        let snapshot_chunk_bytes = parse_count(
            "QUORUM_SNAPSHOT_CHUNK_BYTES",
            std::env::var("QUORUM_SNAPSHOT_CHUNK_BYTES").ok().as_deref(),
            DEFAULT_SNAPSHOT_CHUNK_BYTES,
        )?;
        if snapshot_chunk_bytes == 0 {
            anyhow::bail!("QUORUM_SNAPSHOT_CHUNK_BYTES must be greater than zero");
        }
        let snapshot_retain = parse_count(
            "QUORUM_SNAPSHOT_RETAIN",
            std::env::var("QUORUM_SNAPSHOT_RETAIN").ok().as_deref(),
            DEFAULT_SNAPSHOT_RETAIN,
        )?;

        Ok(Self {
            id,
            listen_addr,
            metrics_addr,
            peers,
            storage_dir,
            read_mode,
            limits,
            join,
            catch_up_timeout,
            snapshot_threshold,
            snapshot_chunk_bytes: snapshot_chunk_bytes as usize,
            snapshot_retain,
        })
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

/// Parses `QUORUM_JOIN`. Unset or `false`/`0` is an ordinary node; `true`/`1`
/// marks a node that is being added to an existing cluster. Anything else is an
/// error: guessing wrong here would let a node that should wait to be admitted
/// start electing itself instead.
pub fn parse_join(raw: Option<&str>) -> anyhow::Result<bool> {
    match raw.map(|s| s.trim().to_ascii_lowercase()) {
        None => Ok(false),
        Some(value) => match value.as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => anyhow::bail!("QUORUM_JOIN must be true, false, 1 or 0, got {:?}", raw.unwrap_or("")),
        },
    }
}

/// Parses a setting that is a whole number of entries (zero is allowed), falling
/// back to `default` when unset. A typo is an error, not a silent default.
pub fn parse_count(name: &str, raw: Option<&str>, default: u64) -> anyhow::Result<u64> {
    match raw {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse()
            .map_err(|_| anyhow::anyhow!("{name} must be a whole number, got {raw:?}")),
    }
}

/// Parses `QUORUM_MAX_VALUE_BYTES`, defaulting to [`DEFAULT_MAX_VALUE_BYTES`].
pub fn parse_max_value_bytes(raw: Option<&str>) -> anyhow::Result<usize> {
    let bytes = parse_count("QUORUM_MAX_VALUE_BYTES", raw, DEFAULT_MAX_VALUE_BYTES as u64)?;
    if bytes == 0 {
        anyhow::bail!("QUORUM_MAX_VALUE_BYTES must be greater than zero");
    }
    // Snapshots are capped well below the frame limit; one value cannot exceed that.
    if bytes > 16 * 1024 * 1024 {
        anyhow::bail!("QUORUM_MAX_VALUE_BYTES must be at most 16777216 (16 MiB)");
    }
    Ok(bytes as usize)
}

/// Parses `QUORUM_CATCH_UP_TIMEOUT_MS`, defaulting to [`DEFAULT_CATCH_UP_TIMEOUT`].
pub fn parse_catch_up_timeout(raw: Option<&str>) -> anyhow::Result<Duration> {
    let Some(raw) = raw else {
        return Ok(DEFAULT_CATCH_UP_TIMEOUT);
    };
    let millis: u64 = raw.trim().parse().map_err(|_| {
        anyhow::anyhow!("QUORUM_CATCH_UP_TIMEOUT_MS must be a whole number, got {raw:?}")
    })?;
    if millis == 0 {
        anyhow::bail!("QUORUM_CATCH_UP_TIMEOUT_MS must be greater than zero");
    }
    Ok(Duration::from_millis(millis))
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

    #[test]
    fn join_is_off_unless_asked_for() {
        assert!(!parse_join(None).unwrap());
        assert!(!parse_join(Some("false")).unwrap());
        assert!(!parse_join(Some("0")).unwrap());
        assert!(parse_join(Some("true")).unwrap());
        assert!(parse_join(Some(" TRUE ")).unwrap());
        assert!(parse_join(Some("1")).unwrap());
    }

    #[test]
    fn a_typo_in_join_is_an_error_not_a_guess() {
        assert!(parse_join(Some("yes")).is_err());
        assert!(parse_join(Some("")).is_err());
        assert!(parse_join(Some("ture")).is_err());
    }

    #[test]
    fn catch_up_timeout_defaults_parses_and_rejects_nonsense() {
        assert_eq!(parse_catch_up_timeout(None).unwrap(), DEFAULT_CATCH_UP_TIMEOUT);
        assert_eq!(parse_catch_up_timeout(Some("1500")).unwrap(), Duration::from_millis(1500));
        assert!(parse_catch_up_timeout(Some("0")).is_err());
        assert!(parse_catch_up_timeout(Some("soon")).is_err());
    }

    fn config_with(join: bool) -> NodeConfig {
        NodeConfig {
            id: 4,
            listen_addr: "node4:7000".to_string(),
            metrics_addr: String::new(),
            peers: [(1, "node1:7000".to_string()), (2, "node2:7000".to_string())].into(),
            storage_dir: PathBuf::new(),
            read_mode: ReadMode::Linearizable,
            limits: Limits::default(),
            join,
            catch_up_timeout: DEFAULT_CATCH_UP_TIMEOUT,
            snapshot_threshold: 0,
            snapshot_chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES as usize,
            snapshot_retain: DEFAULT_SNAPSHOT_RETAIN,
        }
    }

    #[test]
    fn an_ordinary_node_starts_as_a_member_of_its_own_cluster() {
        let members = config_with(false).base_membership();
        assert_eq!(members.ids().collect::<Vec<_>>(), vec![1, 2, 4]);
    }

    #[test]
    fn a_joining_node_starts_outside_the_cluster_it_is_joining() {
        let members = config_with(true).base_membership();
        assert_eq!(members.ids().collect::<Vec<_>>(), vec![1, 2]);
        assert!(!members.contains(4), "it must not vote until the log says so");
    }

    #[test]
    fn snapshots_are_on_by_default_and_zero_turns_them_off() {
        assert_eq!(
            parse_count("X", None, DEFAULT_SNAPSHOT_THRESHOLD).unwrap(),
            10_000,
            "snapshots are on unless switched off"
        );
        assert_eq!(parse_count("X", Some("500"), 10_000).unwrap(), 500);
        assert_eq!(parse_count("X", Some(" 0 "), 10_000).unwrap(), 0, "zero is a real setting");
        assert_eq!(parse_count("X", None, DEFAULT_SNAPSHOT_RETAIN).unwrap(), 1000);
        assert_eq!(DEFAULT_SNAPSHOT_CHUNK_BYTES, 65_536);
    }

    #[test]
    fn a_typo_in_a_snapshot_setting_names_the_setting() {
        let error = parse_count("QUORUM_SNAPSHOT_THRESHOLD", Some("lots"), 0).unwrap_err();
        assert!(error.to_string().contains("QUORUM_SNAPSHOT_THRESHOLD"), "{error}");
        assert!(parse_count("X", Some("-3"), 0).is_err());
    }

    #[test]
    fn the_value_limit_defaults_to_one_mebibyte_and_rejects_nonsense() {
        assert_eq!(parse_max_value_bytes(None).unwrap(), 1024 * 1024);
        assert_eq!(parse_max_value_bytes(Some("4096")).unwrap(), 4096);
        assert!(parse_max_value_bytes(Some("0")).is_err());
        assert!(parse_max_value_bytes(Some("big")).is_err());
        assert!(parse_max_value_bytes(Some("16777217")).is_err());
        assert_eq!(parse_max_value_bytes(Some("16777216")).unwrap(), 16 * 1024 * 1024);
    }
}

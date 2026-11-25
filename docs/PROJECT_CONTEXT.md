# Quorum: project context (for resuming work)

## What it is
A distributed key-value store on a from-scratch Raft implementation in Rust (no consensus library).
Repo: github.com/MuaazTasawar/quorum. Windows + PowerShell workflow. Rust workspace, edition 2021.

## Crates
- raft-core: pure algorithm (no I/O): roles, election, pre-vote, replication, commit rule, Membership.
- storage: WAL (group commit, torn-tail recovery), hard state (term/vote), Persistence facade, KvStore, Command (Put/Delete/NoOp/AddNode/RemoveNode), snapshot.rs (library only).
- transport: length-prefixed bincode frames over TCP.
- node: server. cluster.rs is the single-owner event loop; network.rs; metrics_api.rs (HTTP + WS); config.rs; errors.rs.
- client: Rust SDK (leader tracking). chaos: Windows/3-node failure-injection CLI.

## Phases done (numbered from 12; commit)
12 housekeeping + CI (fdb1af8) | 13 replication correctness (32d11f3) | 14 durable term/vote, Persistence, test harness (c6b7329)
15 request lifecycle: 503/504 + outcome_unknown (ed57b76) | 16 pre-vote + check-quorum (2615889) + fix 5232505
17 linearizable reads / ReadIndex (138eae5) | 18 backpressure + group commit + torn-WAL recovery (7a0af30)
State at 7a0af30: 189 tests, 0 warnings (259 after Phase 19). HTTP: PUT/GET/DELETE /kv/{key}, GET /health, GET /ws.
Settings: QUORUM_NODE_ID, LISTEN_ADDR, METRICS_ADDR, PEERS, STORAGE_DIR, REQUEST_TIMEOUT_MS, LOCAL_READS, CHANNEL_CAPACITY, MAX_BATCH, JOIN, CATCH_UP_TIMEOUT_MS, SNAPSHOT_THRESHOLD, SNAPSHOT_RETAIN, ELECTION_TIMEOUT_MS (default 1000; see Phase 24.1), TOKENS_FILE, AUTH_PROTECT_OBSERVABILITY (Phase 25a).

## Phase 19 (done): membership changes
Single-server changes through the log. Effective on APPEND, undone if truncated. One change in flight at a time. A new
leader must commit its no-op first (else 503 NotReady). A new node joins as a non-voting learner (receives the log, counts
for nothing), is promoted once match_index >= commit_index, then AddNode{id,addr} is appended. RemoveNode takes effect on
append; a leader removing itself keeps leading until it commits, then steps down. Removed/not-yet-admitted nodes never campaign
and their RequestVote/PreVote is refused without side effects. Membership is rebuilt from the log (ConfigHistory in
raft-core) on top of the bootstrap config (QUORUM_PEERS + self; for QUORUM_JOIN nodes, peers only).
Config: QUORUM_JOIN, QUORUM_CATCH_UP_TIMEOUT_MS (default 20000). Cluster::new takes a PeerConnector (dials members learned from the log).
Endpoints: POST /admin/members {id,addr}; DELETE /admin/members/{id}; /health and /ws show members + learners.
Admin responses: 200 applied / 200 is_leader:false+leader_hint / 409 refused (nothing logged) / 400 bad addr / 503 NotReady or
LeadershipLost (outcome unknown) / 504 timeout (outcome unknown). Admin timeout = 10x request timeout.
Verification: raft-core 68, storage 32, node unit 112 + membership integration 11 (259 total), 0 warnings; 15 mutation checks
all caught; 5 repeat runs of the membership suite; real 4-process TCP run (join, SIGKILL leader, remove); 80-200 ms fsync shim run.

## Phase 20 (done): compaction primitives
Library + startup loading only. The node does NOT yet take snapshots or compact its log (Phase 21), so nothing changes at runtime.
- raft-core Log has a snapshot base (base_index/base_term): get/term_at/matches/append/truncate work after compaction;
  compact_through(i), install_snapshot_point(i, term) (keeps the suffix if the log holds that entry, else replaces the log),
  can_send_after(i). ConfigHistory gains membership_at(i) and compact_through(i); Membership is serde-serialisable.
- handle_append_entries skips entries at/below the follower's snapshot point (they are committed) instead of rejecting.
- storage: snapshot file is versioned and checksummed: "QSNP" magic, format version (1), payload length, CRC-32, then bincode
  {metadata{last_included_index, last_included_term, membership}, state}. Unknown version, bad magic, truncation and checksum
  mismatch are errors, never "no snapshot". Wal::compact_through(i). Persistence::save_snapshot_and_compact saves the snapshot
  durably FIRST, then trims the WAL; Persistence::open finishes an interrupted compaction, rejects gaps (LogGap) and stale
  snapshots (StaleSnapshot), and raises the term to the snapshot's. Recovered carries `snapshot: Option<RecoveredSnapshot>`.
- node: Cluster::new starts from a snapshot (state, log base, commit/applied = base, membership = the snapshot's, replacing the
  environment's). A leader does not build AppendEntries for a follower whose next_index is below its log start (needs
  InstallSnapshot, Phase 22) - it sends that peer nothing.
- This removes the Phase 19 limitation that QUORUM_PEERS must never change: once a node has a snapshot, membership comes from it.
Verification: raft-core 88, storage 58, node unit 120, integration incl. snapshot_test 2 (315 total), 0 warnings; 15 mutation checks
all caught; full suite twice and snapshot_test five times stable.

## Phase 21 (done): snapshot lifecycle
Nodes now take snapshots and trim their logs, OFF BY DEFAULT (QUORUM_SNAPSHOT_THRESHOLD=0) until InstallSnapshot (Phase 22) exists.
- QUORUM_SNAPSHOT_THRESHOLD=N: after N applied entries since the last snapshot, snapshot the state machine at last_applied
  (membership_at(index), term_at(index)). QUORUM_SNAPSHOT_RETAIN=M (default 1000): keep M log entries behind the snapshot.
- Taking a snapshot and trimming the log are separate steps (Persistence::save_snapshot_only / compact_log_through; trimming past
  the snapshot is refused). A LEADER never trims past its slowest peer's match_index, learners included; followers only keep the
  retain margin. After a restart the snapshot makes the margin redundant (log continues at snapshot+1).
- A failed snapshot write is logged and retried after another threshold's worth of entries; it is never fatal (unlike log writes).
- Adding a node when the leader's log is compacted (base_index > 0) is refused up front: 409 LogCompacted (needs Phase 22).
- /health and /ws gain snapshot_index, log_base, snapshots_taken.
Verification: raft-core 88, storage 60, node unit 133 + integration incl. snapshot_test 5 (333 total), 0 warnings; 9 mutation checks all
caught; full suite twice and snapshot_test five times stable; real 3-process run (100 writes, files shrink to a snapshot + 1 WAL entry,
kill -9 all, restart, 0 keys lost); 8 slow-fsync (80-200 ms) runs: 0 acknowledged writes lost, availability the same with
snapshots on or off (leadership flaps either way at that disk speed: pre-existing).

## Phase 22 (done): InstallSnapshot, snapshots on by default
A leader whose peer needs entries it has compacted away sends it the snapshot instead (Raft paper section 7).
- Wire: RpcMessage::InstallSnapshot / InstallSnapshotResponse appended at the END of the enum (variant positions are pinned by a
  codec test). Every node in a cluster must be rebuilt together. The payload is the snapshot FILE bytes (already versioned and
  checksummed), in chunks of QUORUM_SNAPSHOT_CHUNK_BYTES (default 65536).
- raft-core snapshot_transfer.rs (pure): SnapshotSender (position, next_chunk, acknowledge: only a CHANGE of position triggers a send,
  so duplicate answers do not multiply traffic), SnapshotAssembler (in-order reassembly, 1 GiB cap), accept_leader_term,
  apply_snapshot_installed.
- Leader: stop-and-wait, next chunk on each answer, current chunk retransmitted on every heartbeat tick; the snapshot bytes are read once
  and held for the whole transfer (shared by followers); answers count as contact (check-quorum, ReadIndex seq); only voters and
  learners are sent snapshots; transfers are dropped on losing leadership. The lagging peer pins the leader's log (floor) so
  the base never moves past it during a transfer.
- Follower: each chunk is leader contact (term adopted and made durable, known_leader, election timer reset). Offset 0 starts afresh;
  a chunk of an unknown transfer is answered next_offset 0; misaligned chunks are answered with the expected offset; a snapshot
  it already covers (commit_index >= index) is acknowledged on the FIRST chunk. When complete: checksum + the announced index/term must
  match, then disk first (Persistence::install_snapshot_bytes: snapshot file, then WAL trimmed; log suffix kept only if the log held the
  snapshot's last entry with the same term, else the whole log dropped), then memory (state, log base, commit/applied, membership =
  snapshot's + kept config entries).
- QUORUM_SNAPSHOT_THRESHOLD now defaults to 10000 (0 disables). AdminRejection::LogCompacted is gone: adding a node to a compacted
  cluster works (learner -> snapshot -> entries -> promoted).
- /health and /ws gain snapshots_installed.
Verification: raft-core 103, storage 68, transport 6, node unit 151 + integration incl. snapshot_test 6 (377 total), 0 warnings; 18
mutation checks all caught (two needed stronger tests); full suite twice, snapshot_test six times stable; real 4-process TCP run
(join after the leader trimmed to 100, 200-byte chunks, kill -9 leader, 0 keys lost), also under an 80-200 ms fsync shim.

## Phase 23 (done): API + SDK hardening
HTTP API (node/src/metrics_api.rs):
- Keys: shared rules in storage::validate_key (non-empty, <= 512 bytes, no control characters, not "." or ".."; those two cannot
  be addressed over HTTP because URL parsers normalise them). Enforced on every request AND by the SDK. A bad key is 400.
  Previously NOTHING validated keys (InvalidKey existed but was never produced).
- Values are binary-safe. PUT /kv/{key}: Content-Type application/octet-stream = raw bytes; application/json = {"value":"text"} OR
  {"value_b64":"..."} (exactly one; strict RFC 4648 base64, own codec in node/src/base64.rs, no new dependency). Other types: 415.
  GET /kv/{key}: JSON now {is_leader, found, value, value_b64, leader_hint}: value_b64 is always the exact bytes, value is text only
  when valid UTF-8 (null for binary). Accept: application/octet-stream returns the raw bytes (404 {"error":"key not found"} if
  absent, 421 + {is_leader:false, leader_hint} on a follower). Old JSON clients are unchanged.
- QUORUM_MAX_VALUE_BYTES (default 1 MiB, max 16 MiB): larger values are 413, nothing logged. Body limit = base64 size + 4 KiB.
- Status map: 400 bad request/key, 409 membership conflict (nothing applied), 413 too large, 415 bad content type, 421 raw read on
  a follower, 503 busy (outcome_unknown false) or leadership lost (true), 504 timeout (write: true, read: false).
SDK (client crate): QuorumClient::{get,get_bytes,put,put_bytes,delete,add_member,remove_member,members}, ClientConfig
{request_timeout, max_attempts, initial_backoff, max_backoff, retry_unknown_writes}.
- Keys are percent-encoded (before: "a?b" silently addressed key "a", "a/b" a different route). Values are bytes; get() on non-UTF-8 is
  ClientError::NotUtf8, never lossy.
- Every answer is classified (pure fn `classify`): Done / NotLeader(hint) / Busy (retry with exponential backoff + jitter, honours
  Retry-After capped at max_backoff) / Unknown / Fatal / Skip (try next node). Errors: InvalidKey, Rejected{status,message},
  OutcomeUnknown, Unavailable, NoLeaderReachable{detail}, NotUtf8.
- A write or membership change whose outcome is unknown (503/504 with outcome_unknown, or a client-side timeout AFTER connecting) is
  NOT resent by default and NOT tried on another node: the cluster has no request de-duplication, so a replay can overwrite a newer
  write. retry_unknown_writes=true opts in. A connection that failed before sending is safe to move past, even for writes.
  members() reads /health to resolve an unknown membership change.
- client/examples/smoke.rs is a runnable tour (QUORUM_NODES=1=host:port,...).
Verification: storage 73, node unit 163, transport 6, raft-core 103, client unit 22 + scripted-server behaviour tests 23, node integration
incl. api_test 12 and sdk_test 5 (451 total), 0 warnings; 19 mutation checks all caught (one needed a new test: leader memory);
full suite twice, new suites five times stable; real 3-process run (SDK example, 5000 random bytes via curl cmp-identical, odd keys
distinct, 400/413/415/421/404 as designed, old JSON client unchanged, SDK works after kill -9 of the leader).
Cargo.lock changed (client -> storage, node dev-dep -> client): commit it, CI uses --locked.

## Phase 24 (done): observability
Full catalog, scrape config, alert rules and dashboard notes: docs/observability.md.
- GET /metrics: Prometheus text 0.0.4 (node/metrics.rs renders; ClusterSnapshot carries the data). Gauges for role (one-hot),
  is_leader, term, commit/applied/last-log/log-base/snapshot indexes, apply lag, members, learners; counters for wal syncs, dropped
  peer messages, snapshots taken/installed, elections started/won, leadership lost; histograms (14 latency buckets) for WAL append,
  snapshot write and HTTP latency; http_requests_total{route,method,code}; leader-only per-peer gauges.
- The HTTP middleware labels by the route TEMPLATE (MatchedPath), "unmatched" otherwise, unknown methods become OTHER: clients
  cannot create series. CORS (`*`) only on GET /health and GET /metrics.
- GET /dashboard: one self-contained page (CSP: nothing external, no framing, no forms), polls each node's /health from the
  browser; `?nodes=host:port,...` (regex-validated). Pure logic is exposed as window.QuorumDashboard.
- Bugs found by verification and fixed in this phase: (1) the dashboard reported a lone stale leader (older term than the rest of
  the cluster) as a healthy leader of the newest term; it is now shown as stale and the cluster as having no current leader.
  (2) the follower AppendEntries path timed heartbeats (no entries, no write) as WAL appends: followers showed 589 observations
  against 201 fsyncs and the near-zero samples would have hidden slow fsyncs in the quantiles; only batches with entries are timed.
- Tests: node/tests/observability_test.rs (9 tests: strict exposition parser, values vs /health, one leader, peers, election
  counters over a leader change, snapshot metrics, keys never create series, CORS/CSP, heartbeats not timed);
  node/tests/dashboard_logic.js (30 checks on the real dashboard.html in a Node vm; run by CI); 13 new node unit tests.
- Verified: 32 mutation checks (21 Rust, 11 JS) all killed; 25 idle + 15 CPU-contended repeat runs of the new tests, 0 failures;
  real 3-process cluster over TCP (load, leader kill + restart): scrapes accepted by the prometheus_client text parser, counters
  right across the failover; every metric named in the alert rules exists in a real scrape.
- NOT verified: the dashboard was never rendered in a real browser (its DOM/polling code is only syntax-checked and its pure
  functions tested); the alert rules were checked for YAML validity and metric names but never loaded into Prometheus/promtool;
  no slow-fsync shim run (the change does not alter what is written or when).

## Phase 24.1 (done): configurable election timeout, 1 s default
- Found by running three real nodes on the user's laptop: the repo's drive (E:) is a spinning HDD (25 ms for ONE 4 KB flush,
  100-900 ms with three nodes flushing at once; the SSD does it in ~2 ms). The election timeout was hard-coded to 150-300 ms, so a
  leader busy in fsync (it sends no heartbeats while it flushes) was replaced again and again: elections won 6/6/4 per node in 10 s
  with no client traffic, and writes failed.
- Reproduced with the LD_PRELOAD fsync shim on real processes, same binary, 250 ms per fsync: old timing 0/40 writes applied, 145
  elections (term 268); new default 40/40, 1 election (term 1); fast disk 200/200, 1 election.
- QUORUM_ELECTION_TIMEOUT_MS (50..=60000, default 1000): a follower waits that long to twice that for its leader; the leader's
  heartbeat is a quarter of it (ElectionTimeoutConfig::from_min / heartbeat_interval in raft-core). Check-quorum uses twice the value.
  The raft-core default stays 150-300 ms and the test harness uses it explicitly, so tests stay fast.
- Cost: failover takes ~1.6-1.9 s at the default (measured), ~0.4 s with QUORUM_ELECTION_TIMEOUT_MS=300. A fsync LONGER than the
  minimum timeout would still cause elections: keep the setting well above the slowest flush on the machine (see slow_wal_append in
  docs/observability.md: p99 append latency is the number to compare against).
- Tests: node/tests/startup_config_test.rs (real binary: variable reaches the node, default, bad values refuse to start),
  config + cluster + raft-core unit tests; 10 mutation checks, all killed.
- Not done: no jitter beyond the 1:2 spread, no separate heartbeat setting (always min/4), no election-timing gauge in /metrics.

## Phase 25 (in progress): security, in four slices
25a API token authentication - DONE. 25b TLS for the HTTP API | 25c mutual TLS on the peer port | 25d certgen (CA + node certs).
Details, limits and token generation: docs/security.md.
- node/src/auth.rs: roles read < write < admin; `Auth::check(route, method, token)` is pure and table-tested; tokens in a file
  (`<role> <token>` lines, BOM and CRLF tolerated, 16..256 printable ASCII, unique); compared in constant time against every token;
  routes: /kv GET=read, PUT/DELETE=write, /admin/*=admin, anything unclassified=admin (fail closed), /health /metrics /ws open unless
  QUORUM_AUTH_PROTECT_OBSERVABILITY, /dashboard always open. Missing file / no tokens / bad token / empty variable / protection without
  tokens all STOP the node (never silently off). Tokens never logged, never in Debug, never in an error message.
- Middleware `authenticate` sits innermost in metrics_api::router (inside the metrics layer, so 401/403 are counted; before body
  limits and handlers, so a refused request's body is never read). AppError::Unauthorized (401 + WWW-Authenticate) / Forbidden (403).
- SDK: `QuorumClient::with_token(..)?` (token validated, redacted Debug), sent on every request incl. members(); 401/403 are Fatal
  `Rejected`, never retried. ClientConfig is unchanged on purpose (struct literals in tests).
- Tests (42 new; workspace total 525 = 483 + 42): 19 unit (auth.rs), 3 config, 2 errors, 2 SDK unit; node/tests/auth_test.rs (11 integration: open when off, 401 everywhere,
  identical answers for missing/wrong tokens, role matrix, refused writes change nothing, refused before the body is read, protected
  observability, refusals counted, SDK uses the token and never retries, SDK members() with protected /health); startup_config_test.rs
  (+5: refuses bad token files, never logs tokens, the RUNNING node enforces its file). 26 mutation checks all killed by a real test
  failure. Real 3-process run with curl: every status as designed, 0 tokens in logs or /metrics.
- NOT done / known: tokens cross the network in clear until 25b; the peer port is unauthenticated until 25c; no rotation without a
  rolling restart, no rate limit, no per-key ACL, no audit log. The dashboard cannot read a protected /health.

## Remaining plan
25b-25d (see above) | 26 chaos CLI v2 + Docker volumes
27 deterministic simulation + linearizability checker | then regenerate the README.

## How we work
- Each phase: the assistant implements and verifies in a sandbox (Rust 1.91), then gives PowerShell that writes files
  (Write-RepoFile, UTF-8 no BOM, LF) or applies exact-match edits (Edit-RepoFileMany: atomic, all or nothing).
- Verification: unit + integration tests, mutation checks (break each mechanism, confirm a test fails), repeat runs for
  flakiness, a slow-fsync LD_PRELOAD shim (80 to 200 ms), and a real multi-process run over TCP.
- User runs: cargo test --workspace --no-fail-fast, then git add/commit/push, then checks the GitHub Actions run.
- Commit message format: "Phase N: Name, description".

## Gotchas learned
- CODE STYLE IS A CI GATE: run `cargo fmt --all` and `cargo clippy --workspace --all-targets -- -D warnings` before every commit
  (both were made blocking after a one-off cleanup of ~660 rustfmt diffs and 10 clippy findings; behaviour unchanged, 451 tests).
  Clippy suggests newer std APIs (is_none_or needs Rust 1.82, is_multiple_of needs 1.90): the project needs 1.85+, so prefer the
  portable form when a suggestion needs newer than that.
- Files extracted from a zip keep the SANDBOX's modification times; Cargo decides what changed by mtime, so a crate can be silently
  left un-rebuilt (symptom: "cannot find function" for something that is in the source; no `Compiling <crate>` line). After
  extracting, set LastWriteTime to now on the extracted files (or `cargo clean -p <crate>`). Never commit unless the tests pass.
- Cluster tests run all nodes on ONE thread (#[tokio::test] default), so one node's slow fsync freezes the others and a leader
  steps down (check-quorum). Symptoms on a slow/virus-scanned disk: random failures in read_index_test / request_lifecycle_test
  (sanity reads answering is_leader:false). Reproduced with an LD_PRELOAD shim (5% of fsyncs taking 800 ms). A constant delay up
  to ~100 ms is tolerated. On Windows exclude the repo's target dir and %TEMP% from Defender real-time scanning (it also halves
  build time). Do not assert a timing the test cannot guarantee: sdk_test once assumed a cut-off leader had NOT yet stepped down.
- Write-RepoFile leaves NO trailing newline; edits that match the end of a file must not assume one.
- Timing-sensitive tests must stamp "last contact" AFTER durable writes (slow CI disks caused a real failure).
- cargo test stops at the first failing binary, so CI uses --no-fail-fast.
- A node added later must have its QUORUM_PEERS list CURRENT members (it replies to the leader through them). Until a node has
  a snapshot its QUORUM_PEERS must not change between restarts (it is the base the log's config entries build on); a snapshot's
  membership replaces it.
- Persistence::open now errors on a log that does not start at 1 (or at snapshot+1) or has gaps; a damaged snapshot stops startup.
- A removed node that was partitioned at removal time never learns it was removed (the leader stops sending once the removal
  commits); it is harmless (non-member votes are refused) but stays stale.
- Shell checks of acknowledgements must require "applied":true: a not-leader redirect is HTTP 200 with is_leader:false, and a dead connection is an empty body.
- Wire format changes (new RPC fields) need all nodes rebuilt together; WAL/command encodings only append variants.

- A mutation harness that is killed mid-run leaves the mutated file in place: always diff the tree against a backup afterwards.
- `pkill -f`/`pgrep -f` match the calling shell's own command line (it killed the shell once, and once reported a finished job as running): use `pkill -x node` or look in /proc.
- Never point two git worktrees at one CARGO_TARGET_DIR: the two builds of the same crate contaminated each other (spurious E0560).
- A laptop with an HDD and an SSD: keep the repo's data directory and CARGO_TARGET_DIR on the SSD (C:). Run test clusters with
  QUORUM_STORAGE_DIR under %TEMP%. Measure first: a 4 KB write+flush loop shows the disk in seconds (25 ms HDD vs 2 ms SSD here).
- `sh` (dash) executes the commands it has already parsed BEFORE reporting a later syntax error: a failed multi-line script can have
  half-run (it applied a mutation once). Check the state after a syntax error; `time (...)` is not valid in dash.
- Sandbox: test binaries linking reqwest need LD_LIBRARY_PATH=<conda env>/lib (exit status 127 = loader error, not a test failure).
  Without it ~28 SDK-linked tests (client lib, sdk_test, behaviour_test) silently did not run in the sandbox: a binary that fails to
  launch prints no `test result` line, so summing those lines still said "0 failed" (sandbox totals were 28 short of the user's Windows
  total for phases 24-24.1: 455 vs 483, which I wrongly put down to Windows-only tests). Always also grep for "process didn't exit
  successfully" and compare the total with the user's;
  `create_file` refuses to overwrite the empty placeholder files (auth.rs, certgen/*): `rm` them first. A mutation harness must count a
  kill only when a test actually FAILED (not any non-zero exit).
- Process-local counters reset on restart; use rate()/increase(). Leader-only per-peer metrics move with leadership.

## Known limitations (roadmap)
A dead member pins the leader's log until it is removed; snapshot work runs inside the event loop (writing, decoding a received one,
and the leader reading the file all stall it in proportion to state size + fsync); a snapshot is held fully in memory while being sent or
received (1 GiB cap) and a transfer interrupted by a restart starts over; transfer is stop-and-wait (one chunk per round trip);
WAL has no per-record checksums; no TLS/auth; the SDK has no request de-duplication (hence no automatic retry of unknown writes); chaos CLI is Windows and 3-node only.
No directory lock on STORAGE_DIR: a second process started on the same directory recovers (opens) the WAL before it fails to bind its ports. The dashboard's rendering has never been viewed in a browser.

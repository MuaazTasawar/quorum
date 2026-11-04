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
Settings: QUORUM_NODE_ID, LISTEN_ADDR, METRICS_ADDR, PEERS, STORAGE_DIR, REQUEST_TIMEOUT_MS, LOCAL_READS, CHANNEL_CAPACITY, MAX_BATCH, JOIN, CATCH_UP_TIMEOUT_MS, SNAPSHOT_THRESHOLD, SNAPSHOT_RETAIN.

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

## Remaining plan
23 API + SDK hardening (binary-safe values, key validation, SDK handles 503/504/409)
24 observability (Prometheus /metrics, dashboard) | 25 security (TLS, auth, certgen) | 26 chaos CLI v2 + Docker volumes
27 deterministic simulation + linearizability checker | then regenerate the README.

## How we work
- Each phase: the assistant implements and verifies in a sandbox (Rust 1.91), then gives PowerShell that writes files
  (Write-RepoFile, UTF-8 no BOM, LF) or applies exact-match edits (Edit-RepoFileMany: atomic, all or nothing).
- Verification: unit + integration tests, mutation checks (break each mechanism, confirm a test fails), repeat runs for
  flakiness, a slow-fsync LD_PRELOAD shim (80 to 200 ms), and a real multi-process run over TCP.
- User runs: cargo test --workspace --no-fail-fast, then git add/commit/push, then checks the GitHub Actions run.
- Commit message format: "Phase N: Name, description".

## Gotchas learned
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

## Known limitations (roadmap)
A dead member pins the leader's log until it is removed; snapshot work runs inside the event loop (writing, decoding a received one,
and the leader reading the file all stall it in proportion to state size + fsync); a snapshot is held fully in memory while being sent or
received (1 GiB cap) and a transfer interrupted by a restart starts over; transfer is stop-and-wait (one chunk per round trip);
WAL has no per-record checksums; no TLS/auth; values are lossy UTF-8 over HTTP; chaos CLI is Windows and 3-node only.
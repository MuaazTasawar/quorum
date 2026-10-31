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
Settings: QUORUM_NODE_ID, LISTEN_ADDR, METRICS_ADDR, PEERS, STORAGE_DIR, REQUEST_TIMEOUT_MS, LOCAL_READS, CHANNEL_CAPACITY, MAX_BATCH.

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

## Remaining plan
20 compaction primitives (log offset, WAL compaction, versioned snapshots carrying membership) | 21 snapshot lifecycle
22 InstallSnapshot (chunked) | 23 API + SDK hardening (binary-safe values, key validation, SDK handles 503/504/409)
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
- A node added later must have its QUORUM_PEERS list CURRENT members (it replies to the leader through them), and
  a node's QUORUM_PEERS must not change between restarts (it is the base the log's config entries build on) until Phase 20
  snapshots carry membership.
- A removed node that was partitioned at removal time never learns it was removed (the leader stops sending once the removal
  commits); it is harmless (non-member votes are refused) but stays stale.
- Wire format changes (new RPC fields) need all nodes rebuilt together; WAL/command encodings only append variants.

## Known limitations (roadmap)
No log compaction yet, snapshots never loaded, no InstallSnapshot; WAL has no per-record checksums; no TLS/auth;
values are lossy UTF-8 over HTTP; chaos CLI is Windows and 3-node only; a new node is copied the whole log (no InstallSnapshot yet).
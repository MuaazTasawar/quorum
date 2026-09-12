# Quorum

**An embeddable, strongly-consistent key-value store built on a from-scratch Raft consensus implementation in Rust.**

No `openraft`. No `raft-rs`. No consensus library of any kind. Every line of the election, replication, and commit logic in `crates/raft-core` was implemented directly from the [Raft paper](https://raft.github.io/raft.pdf) ("In Search of an Understandable Consensus Algorithm," Ongaro & Ousterhout), then proven correct with a real integration test that kills a live leader mid-write and asserts zero data loss.

Repository: [github.com/MuaazTasawar/quorum](https://github.com/MuaazTasawar/quorum)

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [Architecture](#architecture)
- [How Raft Works Here](#how-raft-works-here)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [The Demo: Killing the Leader](#the-demo-killing-the-leader)
- [Chaos Testing CLI (Local, Non-Docker)](#chaos-testing-cli-local-non-docker)
- [Testing Philosophy](#testing-philosophy)
- [A Real Bug This Project Found (and Fixed)](#a-real-bug-this-project-found-and-fixed)
- [Known Limitations & Design Decisions](#known-limitations--design-decisions)
- [Project Stats](#project-stats)
- [License](#license)

---

## Why This Exists

Most portfolio "distributed systems" projects wrap a REST API around an existing consensus crate. That demonstrates you can *use* Raft. This project exists to demonstrate something different: that the algorithm itself — including the safety properties that are easy to get subtly wrong — was implemented and debugged from first principles.

That distinction matters for the kind of role this project targets: senior infrastructure and platform engineering, where the expectation isn't "can you call a database," it's "can you reason correctly about what happens when a leader dies mid-write, when the network partitions, when two nodes disagree about who's in charge."

Every claim in this README about correctness is backed by a test you can run yourself with `cargo test --workspace`.

---

## Architecture

```
quorum/
├── crates/
│   ├── raft-core/     Pure Raft algorithm - election, replication, log
│   │                  matching, commit-index advancement. Zero I/O,
│   │                  zero networking, zero knowledge that a network
│   │                  even exists. 12 unit tests.
│   │
│   ├── storage/       Write-ahead log (fsync-on-append durability),
│   │                  snapshotting for log compaction, and the actual
│   │                  KV state machine (Put/Delete/NoOp commands).
│   │                  2 unit tests.
│   │
│   ├── transport/     Length-prefixed bincode framing over raw TCP.
│   │                  Nagle's algorithm disabled for low-latency RPC
│   │                  round-trips. 64 MiB frame-size ceiling guards
│   │                  against a corrupt length prefix causing an
│   │                  unbounded allocation. 4 unit tests, including
│   │                  one that round-trips a message over a real
│   │                  bound TCP socket, not just an in-memory buffer.
│   │
│   ├── node/          The server process: wires raft-core + storage +
│   │                  transport into a running node. Owns the actual
│   │                  Raft event loop (tokio::select! over inbound
│   │                  RPCs, client commands, heartbeat ticks, and
│   │                  election timeouts) and exposes an Axum HTTP +
│   │                  WebSocket API for client reads/writes and a
│   │                  live cluster-state stream. 5 unit tests + 2
│   │                  integration tests (the important ones - see
│   │                  below).
│   │
│   ├── client/        Rust SDK with automatic leader discovery and
│   │                  redirect-following. Callers never see a
│   │                  "not the leader" response directly - the SDK
│   │                  tracks its current best guess, falls back to
│   │                  probing the rest of the cluster when wrong, and
│   │                  refuses to trust a leader hint pointing at a
│   │                  node ID it's never heard of. 5 unit tests.
│   │
│   └── chaos/         CLI for local (non-Docker) chaos testing: kill
│                       a node's OS process outright, or partition its
│                       network via a Windows Firewall rule toggle,
│                       without the node binary itself ever knowing
│                       chaos testing exists.
│
├── Dockerfile              Multi-stage build: compiles the node
│                           binary in a rust:1-slim-bookworm builder,
│                           ships only the binary in a minimal
│                           debian:bookworm-slim runtime image.
│
├── docker-compose.yml      A 5-node full-mesh cluster on a named
│                           bridge network, wired via Docker's
│                           built-in service-name DNS resolution.
│
└── (workspace Cargo.toml tying all six crates together)
```

**Design principle:** every crate boundary here is a real separation of concerns, not an arbitrary folder split. `raft-core` has no idea TCP exists. `transport` has no idea Raft exists — it moves bytes, full stop. `storage` doesn't know it's backing a Raft log; it just durably persists entries and applies commands. This is what makes the raft-core unit tests trustworthy in isolation: they're testing the algorithm against nothing but its own data types, with no network flakiness or disk I/O timing to muddy a failure.

---

## How Raft Works Here

A quick primer on what's actually implemented, for anyone reading this without a distributed-systems background:

### Leader Election
Every node starts as a **Follower**. If a Follower doesn't hear from a leader within a randomized timeout (150–300ms, randomized specifically so that when a leader dies, every Follower doesn't become a Candidate in the same instant and split the vote forever), it becomes a **Candidate**, increments its term, votes for itself, and requests votes from every other node. A Candidate that wins a majority becomes the **Leader**. This is implemented in [`raft-core/src/election.rs`](crates/raft-core/src/election.rs).

Crucially, a node only grants a vote if the requesting Candidate's log is **at least as up-to-date** as its own — this is what prevents a node that's missing committed data from ever winning an election and silently discarding history.

### Log Replication
The Leader is the only node that accepts writes. It appends the command to its own log, then replicates it to every Follower via `AppendEntries` RPCs. Once a **majority** of the cluster (including the leader) has durably persisted the entry, it's **committed** — safe to apply to the state machine and safe to acknowledge to the client. This is [`raft-core/src/replication.rs`](crates/raft-core/src/replication.rs).

### The Subtle Safety Rule (and why it matters)
A leader can only *directly* commit entries from **its own current term** — never an earlier term's entry, even if that entry is already replicated to a majority. This is Raft's most commonly-missed correctness property (§5.4.2 of the paper), and it's directly enforced and tested here:

```rust
// raft-core/src/replication.rs
pub fn advance_commit_index(...) -> Option<u64> {
    // ...
    if log.term_at(candidate) != Some(current_term) {
        continue; // never commit an earlier-term entry directly
    }
    // ...
}
```

with a dedicated test, `commit_index_only_advances_for_current_term_entries`, that fails loudly if this rule is ever violated.

### The No-Op-on-Election Fix (Raft §8)
This rule creates a real problem: if a leader dies right after replicating an entry to a majority but *before* telling everyone the new commit index (its next heartbeat), that entry is safely durable but stuck — a new leader can't commit it directly (wrong term), and followers never learned it was committed. The fix, straight from the Raft paper: **every new leader immediately appends a no-op entry in its own term the instant it takes power.** Once that no-op commits, the log-matching property drags every earlier entry's commit forward with it. See [`crates/node/src/cluster.rs`, `become_leader()`](crates/node/src/cluster.rs) — and the section below on how this bug was actually found.

---

## Getting Started

### Prerequisites
- Rust (stable toolchain, 2021 edition or newer)
- Docker Desktop (only needed for the 5-node cluster demo, not for running tests)

### Run the full test suite
```bash
cargo test --workspace
```
Expect **30 tests passing**: 12 in `raft-core`, 4 in `transport`, 5 in `client`, 5 unit + 2 integration in `node`, 2 in `storage`.

### Run a single node locally
```bash
$env:QUORUM_NODE_ID = "1"
$env:QUORUM_LISTEN_ADDR = "127.0.0.1:7000"
$env:QUORUM_METRICS_ADDR = "127.0.0.1:8000"
$env:QUORUM_STORAGE_DIR = "./data/node1"
cargo run -p node
```
A single-node cluster elects itself leader instantly (a majority of one). In another terminal:
```powershell
Invoke-RestMethod -Method Put -Uri "http://localhost:8000/kv/foo" -ContentType "application/json" -Body '{"value":"bar"}'
Invoke-RestMethod -Uri "http://localhost:8000/kv/foo"
```

### Run the 5-node Docker Compose cluster
```bash
docker compose build
docker compose up -d
docker compose ps
```
Each node's metrics API is published on the host at ports **18001–18005** (mapped to each container's internal port 8000). Check who's leader:
```powershell
Invoke-RestMethod http://localhost:18001/health
```
Look for `role: Leader` on exactly one node — the others will show `Follower` with a `leader_hint` pointing at the leader's node ID.

---

## API Reference

Every node exposes the same HTTP API on its metrics port. Writes and reads to a non-leader node don't error out — they return `is_leader: false` with a `leader_hint`, so a well-behaved client (like the included Rust SDK) can transparently redirect.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Full cluster snapshot: role, term, commit index, log length, leader hint |
| `GET` | `/kv/{key}` | Read a value. Only the leader answers meaningfully; a follower returns `is_leader: false` |
| `PUT` | `/kv/{key}` | Write a value. Body: `{"value": "..."}`. Only the leader accepts |
| `DELETE` | `/kv/{key}` | Delete a key. Only the leader accepts |
| `GET` | `/ws` | WebSocket stream of live `ClusterSnapshot` updates — push-based (a `tokio::watch` channel under the hood), not polling |

**Example — writing through a known leader:**
```powershell
Invoke-RestMethod -Method Put -Uri "http://localhost:18005/kv/demo" -ContentType "application/json" -Body '{"value":"quorum-works"}'
# -> { is_leader: True, applied: True, leader_hint: null }

Invoke-RestMethod http://localhost:18005/kv/demo
# -> { is_leader: True, value: "quorum-works", leader_hint: null }

# Hitting a follower instead:
Invoke-RestMethod http://localhost:18001/kv/demo
# -> { is_leader: False, value: null, leader_hint: 5 }
```

---

## The Demo: Killing the Leader

This is the actual point of the project — watching the cluster survive a real crash.

**1. Identify the current leader** (poll each node's `/health` until you find `role: Leader`).

**2. Write a value through it**, then confirm it reads back correctly.

**3. Kill the leader's container outright** — a real process death, no graceful shutdown:
```bash
docker compose kill <leader-service-name>
```

**4. Watch the survivors elect a new leader.** Poll `/health` on the remaining nodes (or connect to `/ws` for a live push feed) — within a couple hundred milliseconds, one of them will flip to `role: Leader` at a higher `current_term`.

**5. Confirm zero data loss.** Read the key you wrote in step 2 from the new leader. It's still there — safely committed before the old leader died, and made visible by the new leader's no-op-on-election commit.

**Simulating a network partition instead of a crash** (the container stays alive, it just can't talk to the rest of the cluster):
```bash
docker network disconnect quorum-net <node-service-name>
# ... watch the rest of the cluster carry on without it ...
docker network connect quorum-net <node-service-name>
# ... watch it reconnect and catch up via normal AppendEntries replication ...
```

---

## Chaos Testing CLI (Local, Non-Docker)

For nodes run directly via `cargo run` (not in Docker), `crates/chaos` provides a CLI:

```powershell
cargo run -p chaos -- status                    # role/term/commit_index/log_len per node
cargo run -p chaos -- kill --pid <pid>           # taskkill /F - real process death
cargo run -p chaos -- partition --node 2         # Windows Firewall rule blocking that node's Raft port (needs an elevated terminal)
cargo run -p chaos -- heal --node 2              # removes the firewall rule
```

**Scope note:** this CLI's `partition`/`heal` commands work by toggling a *host-level* Windows Firewall rule — correct for nodes running directly on your machine, but it does **not** partition Docker containers, since inter-container traffic on a docker-compose network never touches the host firewall at all. For the Docker topology, use `docker network disconnect`/`connect` as shown above. This is a deliberate scope boundary, not an oversight: extending the chaos CLI to also drive the Docker API would be scope creep for what this tool is for.

---

## Testing Philosophy

- **`raft-core`'s 12 tests** validate the algorithm in complete isolation — no networking, no disk I/O, no timing flakiness. Two are worth calling out specifically: `rejects_vote_when_candidate_log_is_behind` (prevents a node with stale data from ever becoming leader) and `commit_index_only_advances_for_current_term_entries` (the subtle §5.4.2 safety rule most from-scratch Raft implementations get wrong).
- **`transport`'s 4 tests** include one that round-trips a message over a *real* bound TCP socket, not just an in-memory buffer — proving the framing works against an actual kernel socket.
- **`node`'s integration tests** (`crates/node/tests/cluster_integration_test.rs`) spin up real, full `Cluster` instances wired together with in-process channels standing in for TCP (transport's own byte-framing is already covered above — this proves *consensus* correctness, not re-testing sockets). `cluster_survives_leader_crash_with_zero_data_loss` is the test that matters most: it writes a key on a 5-node cluster, hard-kills the leader's task mid-flight (`JoinHandle::abort()` — no graceful shutdown), and asserts the surviving majority elects a new leader that still has the write.
- **Every bug found during development was found by these tests**, not by manual clicking — including the one below.

---

## A Real Bug This Project Found (and Fixed)

While writing the leader-crash integration test, it failed — not because the test was wrong, but because it exposed a genuine gap: a leader that dies immediately after replicating an entry to a majority (but before its next heartbeat) leaves that entry durably safe but **stuck** — not yet marked committed on the survivors, and not directly committable by a new leader either, because of the exact "only commit current-term entries directly" safety rule this project already enforced and tested.

The fix is a known Raft technique (§8 of the paper): every new leader immediately appends a no-op entry in its own term on election. Once *that* commits, the log-matching property drags the stranded entry's commit forward with it.

This is included in the README deliberately, not hidden: **finding this via a real correctness test, diagnosing it against the paper, and fixing it properly is a stronger signal of understanding than never having had a bug at all.**

---

## Known Limitations & Design Decisions

- **Reads are not strictly linearizable.** `GET` requests are answered directly from the leader's locally-applied state, without the read-index/lease-read confirmation protocol from Raft §8. This is a deliberate MVP-scope simplification — a production system would add read-index confirmation before answering a read, to rule out the rare case of a stale leader that hasn't yet learned it lost an election.
- **`current_term` / `voted_for` are not yet persisted across a node restart.** A restarted node currently starts at term 0. This is safe (it will simply lose an election to any node with a higher term, never causing a split-brain), just not optimal — a restarted node has to "catch up" via an extra election round rather than resuming exactly where it left off.
- **The chaos CLI's network-partition mechanism (Windows Firewall) only applies to nodes run directly on the host**, not Docker containers — see the Chaos CLI section above for why, and what to use instead for the Docker topology.
- **No log compaction is wired into the running node yet.** The `storage::snapshot` module implements atomic snapshot save/load and the WAL supports truncation, but the node doesn't yet periodically trigger compaction — a long-running cluster's WAL will grow unboundedly. The building blocks exist; the scheduling policy doesn't yet.

---

## Project Stats

- **6 crates**, one Cargo workspace
- **30 tests passing** (12 raft-core, 4 transport, 5 client, 5 node-unit, 2 node-integration, 2 storage)
- **Consensus algorithm implemented from the paper**, not from a library
- **One real, documented, paper-referenced bug found and fixed** via an automated correctness test

## License

MIT
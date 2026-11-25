# Observability

Every node serves three read-only things on its HTTP port (`QUORUM_METRICS_ADDR`):

| Path | What | Format |
|---|---|---|
| `GET /health` | The node's current state as JSON (role, term, indexes, members, peers). | JSON |
| `GET /metrics` | The same state as Prometheus metrics, plus counters and latency histograms. | Prometheus text 0.0.4 |
| `GET /dashboard` | A one-page live view of a whole cluster (see below). | HTML |

With `QUORUM_AUTH_PROTECT_OBSERVABILITY=true` (see `docs/security.md`) `/health`, `/metrics` and `/ws` need a `read` token:
give Prometheus one with `authorization: { type: Bearer, credentials_file: ... }`; the dashboard cannot read a protected `/health`.

`/health` and `/metrics` send `Access-Control-Allow-Origin: *` so a dashboard served by one node can read
the others. Nothing that reads or changes data (`/kv`, `/admin`, `/ws`) is ever opened up this way, and neither is
`/dashboard` itself. There is no authentication yet (Phase 25): put the port behind your network boundary.

Metrics are per node and per process: counters start again at 0 when a node restarts, which Prometheus'
`rate()` and `increase()` handle. Scrape every node and let Prometheus add the `instance` label.

## Scrape config

```yaml
scrape_configs:
  - job_name: quorum
    scrape_interval: 5s
    static_configs:
      - targets: ["node1:8000", "node2:8000", "node3:8000"]
```

## Metric catalog

Gauges describe the node right now; `_total` counters only go up; histograms use these upper bounds, in
seconds: 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, +Inf.

| Metric | Type | Meaning |
|---|---|---|
| `quorum_node_info{node_id,version}` | gauge | Always 1; the labels identify the node. |
| `quorum_role{role}` | gauge | One-hot: `leader`, `follower`, `candidate`. |
| `quorum_is_leader` | gauge | 1 on the leader. **Summed over the cluster it should be exactly 1.** |
| `quorum_known_leader_id` | gauge | Who this node thinks leads (0 = unknown). |
| `quorum_current_term` | gauge | Raft term. |
| `quorum_commit_index` / `quorum_last_applied_index` | gauge | Last committed / applied log index. |
| `quorum_apply_lag_entries` | gauge | Committed but not yet applied. Should be near 0. |
| `quorum_last_log_index` | gauge | Last entry in this node's log. |
| `quorum_log_base_index`, `quorum_snapshot_index` | gauge | Compaction: last discarded index; index the newest snapshot reaches. |
| `quorum_members`, `quorum_learners` | gauge | Voting members as this node sees them; learners being caught up (leader only). |
| `quorum_wal_syncs_total` | counter | Times the log was forced to disk (group commit makes this grow slower than writes). |
| `quorum_peer_messages_dropped_total` | counter | Outbound peer messages dropped because that peer's queue was full. |
| `quorum_snapshots_taken_total`, `quorum_snapshots_installed_total` | counter | Snapshots written here / received from a leader. |
| `quorum_elections_started_total` | counter | Real elections this node started (after a won pre-vote). |
| `quorum_leader_elections_won_total` | counter | Times this node became leader. |
| `quorum_leadership_lost_total` | counter | Times this node stopped being leader. |
| `quorum_wal_append_duration_seconds` | histogram | Time to write log entries including the fsync. Only batches that carry entries are timed; heartbeats write nothing and are not counted. |
| `quorum_snapshot_write_duration_seconds` | histogram | Time to write a snapshot. |
| `quorum_peer_match_index{peer}` | gauge | **Leader only.** Highest index known replicated on that peer. |
| `quorum_peer_replication_lag_entries{peer}` | gauge | **Leader only.** Leader's last index minus the peer's match index. |
| `quorum_peer_seconds_since_ack{peer}` | gauge | **Leader only.** Seconds since the peer last answered; absent if it never has. |
| `quorum_peer_snapshot_transfer_active{peer}` | gauge | **Leader only.** 1 while a snapshot is being sent to that peer. |
| `quorum_http_requests_total{route,method,code}` | counter | Requests answered. |
| `quorum_http_request_duration_seconds{route,method}` | histogram | Request latency. |

`route` is the route **template** (`/kv/{key}`, `/health`, ...) and `unmatched` for any path that matches no
route; `method` is one of GET, PUT, POST, DELETE, HEAD, OPTIONS, or `OTHER`. Keys and arbitrary paths never become
label values, so a client cannot create new series by choosing different keys.

The leader-only per-peer metrics exist only on the current leader; after a leadership change they move to the new
leader's scrape. Group them with `sum`/`max by (peer)` rather than expecting a fixed instance.

## Alert rules

```yaml
groups:
  - name: quorum
    rules:
      - alert: QuorumNodeDown
        expr: up{job="quorum"} == 0
        for: 1m
        labels: {severity: page}
        annotations: {summary: "{{ $labels.instance }} cannot be scraped"}

      - alert: QuorumNoLeader
        expr: sum(quorum_is_leader) == 0
        for: 30s
        labels: {severity: page}
        annotations: {summary: "No node reports being the leader: writes are failing"}

      - alert: QuorumMultipleLeaders
        expr: sum(quorum_is_leader) > 1
        for: 30s
        labels: {severity: page}
        annotations: {summary: "More than one leader for 30s (a stale leader normally steps down within seconds)"}

      - alert: QuorumLeadershipChurn
        expr: sum(increase(quorum_leader_elections_won_total[10m])) > 3
        labels: {severity: warn}
        annotations: {summary: "Leadership changed more than 3 times in 10 minutes"}

      - alert: QuorumApplyLag
        expr: quorum_apply_lag_entries > 1000
        for: 2m
        labels: {severity: warn}
        annotations: {summary: "{{ $labels.instance }} has applied far less than it has committed"}

      - alert: QuorumSlowWalAppend
        expr: |
          histogram_quantile(0.99,
            sum by (le, instance) (rate(quorum_wal_append_duration_seconds_bucket[5m]))) > 0.1
        for: 5m
        labels: {severity: warn}
        annotations: {summary: "p99 log append (fsync) on {{ $labels.instance }} is over 100 ms"}

      - alert: QuorumPeerLagging
        expr: quorum_peer_replication_lag_entries > 1000 and on (peer) quorum_peer_snapshot_transfer_active == 0
        for: 2m
        labels: {severity: warn}
        annotations: {summary: "Peer {{ $labels.peer }} is more than 1000 entries behind the leader and no snapshot is being sent"}

      - alert: QuorumPeerSilent
        expr: quorum_peer_seconds_since_ack > 10
        for: 1m
        labels: {severity: warn}
        annotations: {summary: "Leader has heard nothing from peer {{ $labels.peer }} for over 10 s"}

      - alert: QuorumMessagesDropped
        expr: increase(quorum_peer_messages_dropped_total[5m]) > 0
        labels: {severity: warn}
        annotations: {summary: "{{ $labels.instance }} is dropping outbound peer messages (a peer's queue is full)"}

      - alert: QuorumHighErrorRate
        expr: |
          sum(rate(quorum_http_requests_total{route="/kv/{key}",code=~"5.."}[5m]))
            / sum(rate(quorum_http_requests_total{route="/kv/{key}"}[5m])) > 0.05
        for: 5m
        labels: {severity: warn}
        annotations: {summary: "More than 5% of key requests are failing with 5xx"}
```

If `QuorumLeadershipChurn` fires together with `QuorumSlowWalAppend`, the disk is too slow for the election timeout (a leader
that is flushing sends no heartbeats): move `QUORUM_STORAGE_DIR` to faster storage or raise `QUORUM_ELECTION_TIMEOUT_MS`
(default 1000; it should stay well above the p99 of `quorum_wal_append_duration_seconds`). The node logs the timing it runs on
at startup (`election timing`).

A dead voting member pins the leader's log until it is removed (a known limitation), so `QuorumPeerSilent` is
also the early warning for log growth.

## The dashboard

Open `http://<any-node>:8000/dashboard`. By default it watches the node that served it; add `?nodes=` with a
comma-separated list to watch the others:

```
http://node1:8000/dashboard?nodes=node1:8000,node2:8000,node3:8000
```

For each node it shows the role, term, commit and applied index (and the lag between them), log and snapshot
position, members and learners, and a commits-per-second sparkline; for the leader, a table of each peer's match
index, lag and time since last ack. A summary line reports `healthy`, `degraded` (a node unreachable, quorum intact),
`no quorum`, `no leader`, or `error` (two leaders in the same term), and an event log records role, term and
membership changes and unreachable/reachable transitions as they are seen.

Things to know:

- The page polls `/health` once a second from **your browser**, so every node listed must be reachable from the
  browser (not just from the node serving the page). Other nodes are read cross-origin, which is why `/health`
  allows it.
- A node that is reachable but reports `Leader` in a term older than the highest term seen is shown as stale, not as
  the leader: it will step down on hearing the newer term.
- The page loads nothing from outside itself and is served with a restrictive Content-Security-Policy (no framing,
  no external loads, no forms). Node addresses typed into `?nodes=` are validated as plain `host[:port]` before use.
- Because the browser fetches `http://` addresses, a dashboard opened over `https://` would be blocked as mixed
  content. TLS arrives in Phase 25.

## Testing

- `crates/node/tests/observability_test.rs`: real clusters over real HTTP. It parses every scrape with a strict
  Prometheus text reader (HELP/TYPE first, no duplicate series, cumulative buckets, `+Inf` equals `_count`) and checks
  values against `/health`, exactly one leader across the cluster, the per-peer gauges, election counters across a
  leader change, snapshot metrics, that keys and paths never create series, and the CORS and CSP rules.
- `node crates/node/tests/dashboard_logic.js`: the dashboard's pure functions (`parseNodes`, `nodeUrl`,
  `commitRate`, `summarize`, `diffEvents`), loaded from the real `dashboard.html` into a Node `vm` with no DOM. It is
  run by CI.
- Not covered by automated tests: the dashboard's rendering and polling in a browser (see the Phase 24 notes in
  `PROJECT_CONTEXT.md`).

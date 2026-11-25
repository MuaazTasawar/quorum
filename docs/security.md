# Security

Status: **API authentication is done (Phase 25a).** TLS for the HTTP API, mutual TLS between nodes and a
certificate generator are still to come (25b-25d). Read "What this does not protect yet" before relying on
any of it.

## API authentication

Every request to `/kv/*` and `/admin/*` can be required to carry `Authorization: Bearer <token>`. A token has one
of three roles, each allowed everything the one before it is:

| Role | May do |
|---|---|
| `read` | read keys (`GET /kv/{key}`) |
| `write` | read, write and delete keys (`PUT`, `DELETE /kv/{key}`) |
| `admin` | all of the above, and change the membership (`/admin/members...`) |

A route this code does not know about needs `admin`, so a route added in the future is closed until someone decides
otherwise.

### Turning it on

Put the tokens in a file and point `QUORUM_TOKENS_FILE` at it, on every node:

```
# role token        (blank lines and # comments are ignored)
read  3f9c...64 hex characters...
write 8a41...
admin c07d...
```

A token is 16 to 256 printable ASCII characters with no spaces; **generate 32 random bytes as hex (64 characters)**.
Tokens must be unique. The role word comes first. Generating a file on Windows PowerShell (5.1 or 7):

```powershell
function New-Token { $b = New-Object byte[] 32; [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($b); ($b | ForEach-Object { $_.ToString("x2") }) -join "" }
@("read $(New-Token)", "write $(New-Token)", "admin $(New-Token)") | Set-Content -Encoding ascii tokens.txt
```

or on Linux: `printf 'read %s\nwrite %s\nadmin %s\n' $(openssl rand -hex 32) $(openssl rand -hex 32) $(openssl rand -hex 32) > tokens.txt && chmod 600 tokens.txt`.

Keep the file out of the repository (`tokens.txt`, `*.tokens` and `/secrets/` are git-ignored) and readable only by the
user running the node (the node warns on Linux if other users can read it). In Docker, mount it as a secret or a
read-only volume; do not bake it into an image.

| Setting | Meaning |
|---|---|
| `QUORUM_TOKENS_FILE` | Path to the token file. Unset: authentication is **off** and the node logs a warning. |
| `QUORUM_AUTH_PROTECT_OBSERVABILITY` | `true` also requires a `read` token for `/health`, `/metrics` and `/ws`. Default `false`. |

The node refuses to start, rather than quietly running without protection, if the file is missing, unreadable,
contains no tokens, has a malformed or too-short or repeated token, if `QUORUM_TOKENS_FILE` is set but empty, or if
`QUORUM_AUTH_PROTECT_OBSERVABILITY` is set without a token file.

### What a client sees

| Status | Meaning |
|---|---|
| `401` | No usable token, or one that matches nothing. A missing token and a wrong one get the identical answer. Carries `WWW-Authenticate: Bearer realm="quorum"`. |
| `403` | A valid token whose role is too low. The body names the role needed. |

Both have the usual `{"error": ..., "outcome_unknown": false}` body: a refused request is never applied, so retrying it
unchanged cannot help. The Rust SDK sends a token with `QuorumClient::new(nodes).with_token(token)?` (on every request,
including the `/health` call `members()` makes) and reports a refusal as `ClientError::Rejected { status: 401 | 403, .. }`
without retrying it.

A refused request is turned away before its body is read and before any handler runs. Refusals show up in
`quorum_http_requests_total{route,method,code}` with `code` 401 or 403, like any other response.

### Open by default: `/health`, `/metrics`, `/ws`, `/dashboard`

A Prometheus scrape and the dashboard have no token to send, so these stay open unless `QUORUM_AUTH_PROTECT_OBSERVABILITY`
is `true`. `/dashboard` is a static page with no data in it and is always open. What `/health` and `/metrics` reveal:
roles, terms, log indexes, member ids, peer lag and request counters; never keys or values.

If you protect them:
- Give Prometheus a token: `authorization: { type: Bearer, credentials_file: /etc/prometheus/quorum.token }` in the scrape config (a `read` token is enough).
- The dashboard page still loads but cannot read `/health` from the browser (it has nowhere to put a token), so it will show every node as unreachable. Use Prometheus/Grafana, or leave observability open on a private network.

### Rotating tokens

Tokens are read once, at startup. To rotate: add the new token to the file next to the old one, restart the nodes one
at a time, move clients to the new token, then remove the old one and restart again. A node restart is cheap (it recovers
from its log).

## What this does not protect yet

- **The token travels in the clear.** Until the API speaks TLS (25b), anyone who can watch the network between a client and
  a node can read the token and use it. Treat it as protection against accidental and casual access on a trusted network,
  not as protection against an attacker on the path.
- **The peer port is unauthenticated.** The Raft port (`QUORUM_LISTEN_ADDR`) accepts anyone who can reach it and speaks the
  protocol, and does not encrypt. Keep it on a private network or behind a firewall until mutual TLS (25c).
- **No lockout, rate limit or token expiry.** A caller can try tokens as fast as the node answers; a 64-hex-character token
  makes guessing hopeless, which is why short ones are refused.
- **No per-key permissions and no audit log.** Roles apply to the whole keyspace. Counts of refusals are in the metrics; who
  made them is not recorded.
- **Tokens are not logged anywhere**, but anyone who can read the token file or the node's environment can read them.

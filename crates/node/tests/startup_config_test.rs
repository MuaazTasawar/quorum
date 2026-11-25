//! The settings that tune consensus timing and protect the API, checked through the real `node`
//! binary: unit tests cover parsing and the cluster, but only a started process shows that an
//! environment variable is actually read and reaches the node.

use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::Duration;

const READ_TOKEN: &str = "read-token-0123456789abcdef";
const ADMIN_TOKEN: &str = "admin-token-0123456789abcdef";

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("quorum-startup-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Starts a single-node server on ephemeral ports with the given extra environment. The
/// settings under test are removed first, so the test machine's own cannot leak in.
fn spawn_node_with(dir: &Path, envs: &[(&str, &str)]) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_node"));
    cmd.env("QUORUM_NODE_ID", "1")
        .env("QUORUM_LISTEN_ADDR", "127.0.0.1:0")
        .env("QUORUM_METRICS_ADDR", "127.0.0.1:0")
        .env("QUORUM_STORAGE_DIR", dir)
        .env_remove("QUORUM_PEERS")
        .env_remove("QUORUM_ELECTION_TIMEOUT_MS")
        .env_remove("QUORUM_TOKENS_FILE")
        .env_remove("QUORUM_AUTH_PROTECT_OBSERVABILITY")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (name, value) in envs {
        cmd.env(name, value);
    }
    cmd.spawn().expect("the node binary starts")
}

fn spawn_node(dir: &Path, election_timeout: Option<&str>) -> Child {
    match election_timeout {
        Some(value) => spawn_node_with(dir, &[("QUORUM_ELECTION_TIMEOUT_MS", value)]),
        None => spawn_node_with(dir, &[]),
    }
}

/// Reads the node's log until a line contains `needle`. Returns that line and everything
/// logged up to and including it, or None if the process ends or 20 s pass first.
fn log_until(child: &mut Child, needle: &str) -> Option<(String, String)> {
    let stdout = child.stdout.take().unwrap();
    let needle = needle.to_string();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut seen = String::new();
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            seen.push_str(&line);
            seen.push('\n');
            if line.contains(&needle) {
                let _ = tx.send((line, seen));
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20)).ok()
}

fn election_timing_line(child: &mut Child) -> Option<String> {
    log_until(child, "election timing").map(|(line, _)| line)
}

fn stop(mut child: Child, dir: &Path) {
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(dir);
}

/// Waits for a node that should refuse to start to exit, and returns how and what it said on
/// stderr. A node that kept running is a failure, not a hang: it is killed and reported.
fn exit_with_message(mut child: Child, what: &str) -> (ExitStatus, String) {
    for _ in 0..200 {
        if let Some(status) = child.try_wait().expect("the node can be polled") {
            let mut message = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut message)
                .unwrap();
            return (status, message);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("{what} was accepted: the node kept running");
}

// ---- election timeout ------------------------------------------------------------

#[test]
fn the_election_timeout_variable_reaches_the_running_node() {
    let dir = scratch_dir("set");
    let mut child = spawn_node(&dir, Some("400"));
    let line = election_timing_line(&mut child);
    stop(child, &dir);

    let line = line.expect("the node logs its election timing");
    assert!(line.contains("\"election_timeout_min_ms\":400"), "{line}");
    assert!(line.contains("\"election_timeout_max_ms\":800"), "{line}");
    assert!(line.contains("\"heartbeat_ms\":100"), "{line}");
}

#[test]
fn without_the_variable_the_node_uses_the_one_second_default() {
    let dir = scratch_dir("default");
    let mut child = spawn_node(&dir, None);
    let line = election_timing_line(&mut child);
    stop(child, &dir);

    let line = line.expect("the node logs its election timing");
    assert!(line.contains("\"election_timeout_min_ms\":1000"), "{line}");
    assert!(line.contains("\"election_timeout_max_ms\":2000"), "{line}");
    assert!(line.contains("\"heartbeat_ms\":250"), "{line}");
}

#[test]
fn a_nonsense_election_timeout_stops_the_node_from_starting() {
    for bad in ["10", "abc", "70000"] {
        let dir = scratch_dir(&format!("bad{bad}"));
        let child = spawn_node(&dir, Some(bad));
        let (status, message) = exit_with_message(child, &format!("{bad:?}"));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(!status.success(), "{bad:?} must not start a node");
        assert!(
            message.contains("QUORUM_ELECTION_TIMEOUT_MS"),
            "{bad:?}: the error should name the variable, got {message:?}"
        );
    }
}

// ---- API authentication ----------------------------------------------------------

fn write_tokens(tag: &str, text: &str) -> PathBuf {
    let dir = scratch_dir(&format!("tokens-{tag}"));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("tokens.txt");
    std::fs::write(&path, text).unwrap();
    path
}

#[test]
fn a_token_file_that_cannot_be_used_stops_the_node_instead_of_turning_authentication_off() {
    let missing = std::env::temp_dir().join("quorum-no-such-token-file.txt");
    let empty = write_tokens("empty", "# nothing here\n");
    let weak = write_tokens("weak", "admin short\n");
    let cases: Vec<(PathBuf, &str)> = vec![
        (missing, "token file"),
        (empty.clone(), "no tokens"),
        (weak.clone(), "at least"),
    ];
    for (path, expected) in cases {
        let dir = scratch_dir("badtokens");
        let child = spawn_node_with(&dir, &[("QUORUM_TOKENS_FILE", path.to_str().unwrap())]);
        let (status, message) = exit_with_message(child, &format!("{path:?}"));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!status.success(), "{path:?}");
        assert!(message.contains(expected), "{path:?}: {message:?}");
    }
    let _ = std::fs::remove_dir_all(empty.parent().unwrap());
    let _ = std::fs::remove_dir_all(weak.parent().unwrap());
}

#[test]
fn protecting_observability_without_tokens_stops_the_node() {
    let dir = scratch_dir("protect-only");
    let child = spawn_node_with(&dir, &[("QUORUM_AUTH_PROTECT_OBSERVABILITY", "true")]);
    let (status, message) = exit_with_message(child, "protection without tokens");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(!status.success());
    assert!(
        message.contains("QUORUM_AUTH_PROTECT_OBSERVABILITY"),
        "{message:?}"
    );
}

#[test]
fn a_token_file_turns_authentication_on_and_no_token_is_ever_logged() {
    let path = write_tokens("on", &format!("read {READ_TOKEN}\nadmin {ADMIN_TOKEN}\n"));
    let dir = scratch_dir("auth-on");
    let mut child = spawn_node_with(&dir, &[("QUORUM_TOKENS_FILE", path.to_str().unwrap())]);
    let logged = log_until(&mut child, "API authentication is on");
    stop(child, &dir);
    let _ = std::fs::remove_dir_all(path.parent().unwrap());

    let (line, everything) = logged.expect("the node says authentication is on");
    assert!(line.contains("\"read_tokens\":1"), "{line}");
    assert!(line.contains("\"admin_tokens\":1"), "{line}");
    assert!(line.contains("\"write_tokens\":0"), "{line}");
    assert!(line.contains("\"protect_observability\":false"), "{line}");
    for secret in [READ_TOKEN, ADMIN_TOKEN] {
        assert!(
            !everything.contains(secret),
            "a token was logged:\n{everything}"
        );
    }
}

#[test]
fn without_a_token_file_the_node_warns_that_authentication_is_off() {
    let dir = scratch_dir("auth-off");
    let mut child = spawn_node_with(&dir, &[]);
    let logged = log_until(&mut child, "API authentication is OFF");
    stop(child, &dir);
    let (line, _) = logged.expect("the node warns that authentication is off");
    assert!(line.contains("\"level\":\"WARN\""), "{line}");
}

// ---- the running node enforces its token file -----------------------------------

/// A port nothing is using right now: bind to port 0, note it, release it.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// One plain HTTP/1.1 request to `127.0.0.1:port`; returns the status code.
fn http_status(port: u16, method: &str, path: &str, token: Option<&str>, body: &[u8]) -> u16 {
    use std::io::Write;
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).expect("the node listens");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let auth = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{auth}\
         Content-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(body).unwrap();
    let mut raw = String::new();
    let _ = stream.read_to_string(&mut raw);
    raw.split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no HTTP status in {raw:?}"))
}

#[test]
fn the_running_node_enforces_the_token_file_it_was_given() {
    let tokens = write_tokens(
        "enforce",
        &format!("read {READ_TOKEN}\nadmin {ADMIN_TOKEN}\n"),
    );
    let dir = scratch_dir("enforce");
    let port = free_port();
    let metrics_addr = format!("127.0.0.1:{port}");
    let mut child = spawn_node_with(
        &dir,
        &[
            ("QUORUM_TOKENS_FILE", tokens.to_str().unwrap()),
            ("QUORUM_METRICS_ADDR", &metrics_addr),
        ],
    );
    let listening = log_until(&mut child, "metrics API listening");

    let results = listening.map(|_| {
        [
            http_status(port, "GET", "/kv/a", None, b""),
            http_status(
                port,
                "GET",
                "/kv/a",
                Some("wrong-token-0123456789abcdef"),
                b"",
            ),
            http_status(port, "GET", "/kv/a", Some(READ_TOKEN), b""),
            http_status(port, "PUT", "/kv/a", Some(READ_TOKEN), b"x"),
            http_status(port, "POST", "/admin/members", Some(READ_TOKEN), b"{}"),
            http_status(port, "GET", "/health", None, b""),
        ]
    });
    stop(child, &dir);
    let _ = std::fs::remove_dir_all(tokens.parent().unwrap());

    let [no_token, wrong, read_ok, read_put, read_admin, health] =
        results.expect("the node says it is listening");
    assert_eq!(no_token, 401, "a request without a token");
    assert_eq!(wrong, 401, "a request with a wrong token");
    assert_eq!(read_ok, 200, "a read with a read token");
    assert_eq!(read_put, 403, "a write with a read token");
    assert_eq!(read_admin, 403, "a membership change with a read token");
    assert_eq!(health, 200, "/health is open by default");
}

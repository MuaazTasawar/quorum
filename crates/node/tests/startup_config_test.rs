//! The settings that tune consensus timing, checked through the real `node` binary: unit tests
//! cover parsing and the cluster, but only a started process shows that the environment
//! variable is actually read and reaches the node.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("quorum-startup-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Starts a single-node server on ephemeral ports with `QUORUM_ELECTION_TIMEOUT_MS` set to
/// `value` (or unset for `None`).
fn spawn_node(dir: &PathBuf, value: Option<&str>) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_node"));
    cmd.env("QUORUM_NODE_ID", "1")
        .env("QUORUM_LISTEN_ADDR", "127.0.0.1:0")
        .env("QUORUM_METRICS_ADDR", "127.0.0.1:0")
        .env("QUORUM_STORAGE_DIR", dir)
        .env_remove("QUORUM_PEERS")
        .env_remove("QUORUM_ELECTION_TIMEOUT_MS")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(value) = value {
        cmd.env("QUORUM_ELECTION_TIMEOUT_MS", value);
    }
    cmd.spawn().expect("the node binary starts")
}

/// Waits for the line that reports the election timing, or None if the process ends or
/// 20 s pass without one.
fn election_timing_line(child: &mut Child) -> Option<String> {
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.contains("election timing") {
                let _ = tx.send(line);
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20)).ok()
}

#[test]
fn the_election_timeout_variable_reaches_the_running_node() {
    let dir = scratch_dir("set");
    let mut child = spawn_node(&dir, Some("400"));
    let line = election_timing_line(&mut child);
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);

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
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);

    let line = line.expect("the node logs its election timing");
    assert!(line.contains("\"election_timeout_min_ms\":1000"), "{line}");
    assert!(line.contains("\"election_timeout_max_ms\":2000"), "{line}");
    assert!(line.contains("\"heartbeat_ms\":250"), "{line}");
}

#[test]
fn a_nonsense_election_timeout_stops_the_node_from_starting() {
    for bad in ["10", "abc", "70000"] {
        let dir = scratch_dir(&format!("bad{bad}"));
        let mut child = spawn_node(&dir, Some(bad));
        // A node that accepted the value would run forever, so wait a bounded time and
        // fail the test instead of hanging the suite.
        let mut status = None;
        for _ in 0..200 {
            if let Some(s) = child.try_wait().expect("the node can be polled") {
                status = Some(s);
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let Some(status) = status else {
            let _ = child.kill();
            let _ = child.wait();
            let _ = std::fs::remove_dir_all(&dir);
            panic!("{bad:?} was accepted: the node kept running");
        };
        let mut message = String::new();
        std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut message).unwrap();
        let _ = std::fs::remove_dir_all(&dir);

        assert!(!status.success(), "{bad:?} must not start a node");
        assert!(
            message.contains("QUORUM_ELECTION_TIMEOUT_MS"),
            "{bad:?}: the error should name the variable, got {message:?}"
        );
    }
}

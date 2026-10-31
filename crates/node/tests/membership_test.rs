//! Membership changes against real nodes over a simulated network.
//!
//! The cluster changes shape through its own log, one server at a time: a new node
//! joins as a learner, is promoted once it has caught up, and only then counts
//! toward any majority. These tests drive that through the same code paths the
//! node binary uses (including the HTTP admin API), and check the properties that
//! matter: no committed data is lost, there is never more than one leader, a
//! removed node cannot disturb the cluster, and the majority really does change.

mod common;

use common::TestCluster;
use node::cluster::{
    AdminRejection, AdminResult, ClientCommandResult, MembershipChange, QueryResult,
};
use raft_core::NodeId;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const SETTLE: Duration = Duration::from_secs(10);

/// Minimal HTTP/1.1 client over a raw socket: (status, whole raw response).
async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.expect("server should accept connections");
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response: {text:?}"));
    (status, text)
}

fn applied(result: &AdminResult) -> Option<Vec<NodeId>> {
    match result {
        AdminResult::Applied { members } => Some(members.clone()),
        _ => None,
    }
}

/// Makes a membership change through whichever node is leader right now, retrying
/// while the cluster is between leaders or a new leader is still settling in. Every
/// result that is not one of those transient ones is returned as it is.
async fn change(cluster: &TestCluster, change: MembershipChange, among: &[NodeId]) -> AdminResult {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let leader = cluster.wait_for_leader_among(among, SETTLE).await;
        let result = cluster.change_membership(leader, change.clone()).await;
        let transient = matches!(
            result,
            AdminResult::NotLeader { .. }
                | AdminResult::LeadershipLost
                | AdminResult::Rejected(AdminRejection::NotReady)
        );
        if !transient || tokio::time::Instant::now() >= deadline {
            return result;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn add(cluster: &TestCluster, id: NodeId, among: &[NodeId]) -> AdminResult {
    change(cluster, MembershipChange::Add { id, addr: format!("node{id}:7000") }, among).await
}

async fn remove(cluster: &TestCluster, id: NodeId, among: &[NodeId]) -> AdminResult {
    change(cluster, MembershipChange::Remove { id }, among).await
}

/// Writes through the leader among `among`, retrying across elections, and returns
/// once the write is acknowledged as applied.
async fn write(cluster: &TestCluster, among: &[NodeId], key: &str, value: &str) {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let leader = cluster.wait_for_leader_among(among, SETTLE).await;
        if let ClientCommandResult::Applied(_) = cluster.put(leader, key, value).await {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "could not write {key} in time");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Reads through the leader among `among`.
async fn read(cluster: &TestCluster, among: &[NodeId], key: &str) -> Option<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let leader = cluster.wait_for_leader_among(among, SETTLE).await;
        if let QueryResult::Value(value) = cluster.get(leader, key).await {
            return value;
        }
        assert!(tokio::time::Instant::now() < deadline, "could not read {key} in time");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn wait_for_members(cluster: &TestCluster, nodes: &[NodeId], expected: &[NodeId]) {
    let nodes = nodes.to_vec();
    let expected = expected.to_vec();
    cluster
        .wait_until(SETTLE, &format!("nodes {nodes:?} to agree the members are {expected:?}"), |c| {
            nodes.iter().all(|&id| c.members_of(id) == expected)
        })
        .await;
}

#[tokio::test]
async fn a_new_node_catches_up_is_admitted_and_then_carries_real_weight() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let all = [1, 2, 3, 4];
    cluster.wait_for_leader(SETTLE).await;
    for i in 0..25 {
        write(&cluster, &[1, 2, 3], &format!("key-{i}"), &format!("value-{i}")).await;
    }

    cluster.start_joiner(4);
    let result = add(&cluster, 4, &[1, 2, 3]).await;
    assert_eq!(applied(&result), Some(vec![1, 2, 3, 4]), "{result:?}");
    wait_for_members(&cluster, &all, &all).await;

    // It has the whole history, and it takes part in new writes.
    write(&cluster, &all, "after-join", "yes").await;
    let leader = cluster.wait_for_leader_among(&[1, 2, 3], SETTLE).await;
    cluster
        .wait_until(SETTLE, "node 4 to apply everything the leader has", |c| {
            c.snapshot(4).last_applied == c.snapshot(leader).last_applied
                && c.snapshot(4).last_applied >= 27
        })
        .await;
    assert_eq!(cluster.snapshot(4).role, "Follower");

    // Prove it is a real voter holding real data: take down two of the ORIGINAL
    // nodes (the leader and one other). A majority of four needs three, so the
    // survivors (one old node and node 4) cannot make progress alone... so take
    // down just the leader instead, and require the other three to carry on.
    cluster.crash(leader).await;
    let remaining: Vec<NodeId> = all.iter().copied().filter(|&id| id != leader).collect();
    for i in 0..25 {
        assert_eq!(
            read(&cluster, &remaining, &format!("key-{i}")).await,
            Some(format!("value-{i}").into_bytes()),
            "key-{i} must survive losing the leader and the membership change"
        );
    }
    assert_eq!(cluster.leaders().len(), 1);
}

#[tokio::test]
async fn a_node_waiting_to_join_never_elects_itself() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let term_before = cluster.snapshot(leader).current_term;

    cluster.start_joiner(4);
    // Far longer than several election timeouts.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let snapshot = cluster.snapshot(4);
    assert_eq!(snapshot.role, "Follower");
    assert_eq!(snapshot.current_term, 0, "it has not even started an election");
    assert_eq!(cluster.leaders(), vec![leader]);
    assert_eq!(cluster.snapshot(leader).current_term, term_before);
    assert_eq!(cluster.members_of(4), vec![1, 2, 3], "and it knows it is not a member");
}

#[tokio::test]
async fn removing_nodes_really_shrinks_the_majority() {
    let cluster = TestCluster::start(&[1, 2, 3, 4, 5]);
    cluster.wait_for_leader(SETTLE).await;
    write(&cluster, &[1, 2, 3, 4, 5], "before", "1").await;

    for id in [4, 5] {
        let result = remove(&cluster, id, &[1, 2, 3, 4, 5]).await;
        assert!(applied(&result).is_some(), "removing {id}: {result:?}");
    }
    wait_for_members(&cluster, &[1, 2, 3], &[1, 2, 3]).await;

    // Three of the five original nodes are now gone or removed. Had nothing
    // changed, the two survivors could not form a majority of five. With the
    // membership shrunk to {1,2,3}, two of three is enough.
    let mut cluster = cluster;
    cluster.crash(4).await;
    cluster.crash(5).await;
    cluster.crash(3).await;
    write(&cluster, &[1, 2], "after", "2").await;
    assert_eq!(read(&cluster, &[1, 2], "before").await, Some(b"1".to_vec()));
    assert_eq!(read(&cluster, &[1, 2], "after").await, Some(b"2".to_vec()));
}

#[tokio::test]
async fn a_leader_can_remove_itself_and_the_others_carry_on_undisturbed() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader(SETTLE).await;
    write(&cluster, &[1, 2, 3], "k", "v").await;
    let others: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != old_leader).collect();

    let result = cluster.remove_member(old_leader, old_leader).await;
    assert_eq!(applied(&result), Some(others.clone()), "{result:?}");

    let new_leader = cluster.wait_for_leader_among(&others, SETTLE).await;
    assert_ne!(new_leader, old_leader);
    assert_eq!(cluster.snapshot(old_leader).role, "Follower", "it handed over");

    write(&cluster, &others, "k2", "v2").await;
    assert_eq!(read(&cluster, &others, "k").await, Some(b"v".to_vec()));

    // The departed node must not keep campaigning against the cluster it left.
    let term = cluster.snapshot(new_leader).current_term;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(cluster.snapshot(new_leader).current_term, term, "no election churn");
    assert_eq!(cluster.leaders(), vec![new_leader]);
    assert_eq!(cluster.members_of(old_leader), others);
}

#[tokio::test]
async fn a_removed_node_that_never_heard_of_it_cannot_disrupt_the_cluster() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let removed = [1, 2, 3].into_iter().find(|&id| id != leader).unwrap();
    let kept: Vec<NodeId> = [1, 2, 3].into_iter().filter(|&id| id != removed).collect();

    // The node is cut off when it is removed, so it never learns that it was. It
    // still believes it is a voter.
    cluster.isolate(removed);
    let result = remove(&cluster, removed, &kept).await;
    assert!(applied(&result).is_some(), "{result:?}");
    assert_eq!(cluster.members_of(removed), vec![1, 2, 3], "still thinks it is a member");
    let term = cluster.snapshot(leader).current_term;

    // It comes back and, finding no leader, keeps trying to start elections.
    cluster.heal();
    tokio::time::sleep(Duration::from_millis(2000)).await;

    assert_eq!(cluster.leaders(), vec![leader], "the leader is undisturbed");
    assert_eq!(cluster.snapshot(leader).current_term, term, "no term inflation");
    write(&cluster, &kept, "still", "working").await;
}

#[tokio::test]
async fn an_add_survives_the_new_node_restarting() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    write(&cluster, &[1, 2, 3], "k", "v").await;
    cluster.start_joiner(4);
    let result = add(&cluster, 4, &[1, 2, 3]).await;
    assert!(applied(&result).is_some(), "{result:?}");

    cluster.crash(4).await;
    cluster.restart(4).await;

    // The new node's own log tells it that it is a member.
    cluster
        .wait_until(SETTLE, "node 4 to recover its membership", |c| {
            c.members_of(4) == vec![1, 2, 3, 4]
        })
        .await;
    write(&cluster, &[1, 2, 3, 4], "after", "restart").await;
    let leader = cluster.wait_for_leader_among(&[1, 2, 3, 4], SETTLE).await;
    cluster
        .wait_until(SETTLE, "node 4 to catch up after its restart", |c| {
            c.snapshot(4).last_applied == c.snapshot(leader).last_applied
        })
        .await;
}

#[tokio::test]
async fn a_node_that_never_answers_is_abandoned_and_the_cluster_is_unharmed() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    cluster.set_catch_up_timeout(Duration::from_millis(600));
    write(&cluster, &[1, 2, 3], "k", "v").await;

    // Node 4 does not exist: nobody is there to answer.
    let result = add(&cluster, 4, &[1, 2, 3]).await;
    assert!(
        matches!(result, AdminResult::Rejected(AdminRejection::CatchUpFailed)),
        "{result:?}"
    );
    assert_eq!(cluster.members_of(1), vec![1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    assert!(cluster.snapshot(leader).learners.is_empty(), "the learner was dropped");
    write(&cluster, &[1, 2, 3], "still", "fine").await;

    // And the slot is free: a real node can be added afterwards.
    cluster.start_joiner(4);
    let result = add(&cluster, 4, &[1, 2, 3]).await;
    assert!(applied(&result).is_some(), "{result:?}");
}

#[tokio::test]
async fn a_slow_new_node_cannot_stall_writes_while_it_catches_up() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    cluster.start_joiner(4);
    // Node 4 is unreachable for now: the learner never answers.
    cluster.isolate(4);
    let leader_for_add = leader;
    let adding = cluster.change_membership(
        leader_for_add,
        MembershipChange::Add { id: 4, addr: "node4:7000".to_string() },
    );
    let writing = async {
        for i in 0..20 {
            write(&cluster, &[1, 2, 3], &format!("key-{i}"), "v").await;
        }
    };
    // While the learner is stuck, the voters (which alone form the majority) keep
    // committing. Then let the learner through so the add can finish.
    let heal = async {
        tokio::time::sleep(Duration::from_millis(800)).await;
        cluster.heal();
    };
    let (result, _, _) = tokio::join!(adding, writing, heal);
    assert!(applied(&result).is_some(), "{result:?}");
    wait_for_members(&cluster, &[1, 2, 3, 4], &[1, 2, 3, 4]).await;
}

#[tokio::test]
async fn writes_keep_flowing_through_a_grow_and_a_shrink() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    cluster.start_joiner(4);

    let acknowledged = std::sync::Mutex::new(Vec::<String>::new());
    let writer = async {
        for i in 0..60 {
            let key = format!("w-{i}");
            // Any live node may be asked; only acknowledged writes are recorded.
            let among = cluster.live_ids();
            let leader = cluster.wait_for_leader_among(&among, SETTLE).await;
            if let ClientCommandResult::Applied(_) = cluster.put(leader, &key, "v").await {
                acknowledged.lock().unwrap().push(key);
            }
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    };
    let reshaper = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let grown = add(&cluster, 4, &[1, 2, 3]).await;
        assert!(applied(&grown).is_some(), "grow: {grown:?}");
        let shrunk = remove(&cluster, 2, &[1, 2, 3, 4]).await;
        assert!(applied(&shrunk).is_some(), "shrink: {shrunk:?}");
    };
    tokio::join!(writer, reshaper);

    let members = [1, 3, 4];
    wait_for_members(&cluster, &members, &members).await;
    assert_eq!(cluster.leaders().iter().filter(|id| members.contains(id)).count(), 1);
    let keys = acknowledged.lock().unwrap().clone();
    assert!(keys.len() > 30, "most writes should have gone through, got {}", keys.len());
    for key in keys {
        assert_eq!(
            read(&cluster, &members, &key).await,
            Some(b"v".to_vec()),
            "{key} was acknowledged and must not be lost"
        );
    }
}

#[tokio::test]
async fn a_cluster_can_grow_one_node_at_a_time_and_shrink_down_to_one() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    cluster.wait_for_leader(SETTLE).await;
    write(&cluster, &[1, 2, 3], "k", "v").await;

    cluster.start_joiner(4);
    assert!(applied(&add(&cluster, 4, &[1, 2, 3]).await).is_some());
    cluster.start_joiner(5);
    assert!(applied(&add(&cluster, 5, &[1, 2, 3, 4]).await).is_some());
    wait_for_members(&cluster, &[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5]).await;

    for id in [1, 2, 3, 4] {
        let among: Vec<NodeId> = (id..=5).collect();
        let result = remove(&cluster, id, &among).await;
        assert!(applied(&result).is_some(), "removing {id}: {result:?}");
    }
    wait_for_members(&cluster, &[5], &[5]).await;
    // One node left: it leads itself and still has everything.
    cluster.wait_for_leader_among(&[5], SETTLE).await;
    write(&cluster, &[5], "last", "one").await;
    assert_eq!(read(&cluster, &[5], "k").await, Some(b"v".to_vec()));
    assert_eq!(read(&cluster, &[5], "last").await, Some(b"one".to_vec()));
}

#[tokio::test]
async fn the_admin_api_adds_and_removes_members_over_http() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    let follower = [1, 2, 3].into_iter().find(|&id| id != leader).unwrap();
    let leader_http = cluster.serve_http(leader, Duration::from_secs(3)).await;
    let follower_http = cluster.serve_http(follower, Duration::from_secs(3)).await;

    // /health lists the members (and no learners at rest).
    let (status, body) = http(leader_http, "GET", "/health", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("\"members\":[1,2,3]"), "{body}");
    assert!(body.contains("\"learners\":[]"), "{body}");

    // A follower says who the leader is rather than acting.
    let (status, body) = http(
        follower_http,
        "POST",
        "/admin/members",
        Some(r#"{"id":4,"addr":"node4:7000"}"#),
    )
    .await;
    assert_eq!(status, 200);
    assert!(body.contains("\"is_leader\":false"), "{body}");
    assert!(body.contains(&format!("\"leader_hint\":{leader}")), "{body}");

    cluster.start_joiner(4);
    let (status, body) = http(
        leader_http,
        "POST",
        "/admin/members",
        Some(r#"{"id":4,"addr":"node4:7000"}"#),
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"applied\":true"), "{body}");
    assert!(body.contains("\"members\":[1,2,3,4]"), "{body}");
    wait_for_members(&cluster, &[1, 2, 3, 4], &[1, 2, 3, 4]).await;

    // The same request again is a conflict, not a second add.
    let (status, body) = http(
        leader_http,
        "POST",
        "/admin/members",
        Some(r#"{"id":4,"addr":"node4:7000"}"#),
    )
    .await;
    assert_eq!(status, 409, "{body}");
    assert!(body.contains("already a member"), "{body}");
    assert!(body.contains("\"outcome_unknown\":false"), "{body}");

    // A missing address is a bad request.
    let (status, _) =
        http(leader_http, "POST", "/admin/members", Some(r#"{"id":9,"addr":""}"#)).await;
    assert_eq!(status, 400);

    // Removal.
    let (status, body) = http(leader_http, "DELETE", "/admin/members/4", None).await;
    assert_eq!(status, 200, "{body}");
    assert!(body.contains("\"members\":[1,2,3]"), "{body}");
    let (status, body) = http(leader_http, "DELETE", "/admin/members/4", None).await;
    assert_eq!(status, 409, "{body}");
    assert!(body.contains("not a member"), "{body}");
}

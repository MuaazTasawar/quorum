//! Nodes whose storage has been compacted into a snapshot.
//!
//! Compaction is done to a stopped node's directory with the same storage calls the
//! node will use once it takes snapshots itself (a later phase), then the node is
//! restarted. What is checked is that a node comes back from a snapshot exactly as
//! it would from the full log: same data, same membership, and still able to
//! replicate, vote and lead.

mod common;

use common::TestCluster;
use node::cluster::{AdminResult, ClientCommandResult, QueryResult};
use raft_core::{Membership, NodeId};
use std::time::Duration;
use storage::{Command, KvStore, Persistence, SnapshotMetadata, Wal, WAL_FILE};

const SETTLE: Duration = Duration::from_secs(10);

/// Snapshots node `id`'s stopped storage through `through`, discarding the log up to
/// there. `membership` is the cluster membership as of that index.
fn compact(cluster: &TestCluster, id: NodeId, through: u64, membership: Membership) {
    let recovered = Persistence::open(cluster.node_dir(id)).expect("storage should open");
    let mut persistence = recovered.persistence;
    let mut state = KvStore::new();
    let mut term = 0;
    for entry in recovered.entries.iter().filter(|e| e.index <= through) {
        state.apply(&Command::decode(&entry.command).expect("entries decode"));
        term = entry.term;
    }
    assert!(term > 0, "node {id} has nothing to snapshot through {through}");
    persistence
        .save_snapshot_and_compact(
            SnapshotMetadata { last_included_index: through, last_included_term: term, membership },
            &state,
        )
        .expect("compaction should succeed");
}

fn first_wal_index(cluster: &TestCluster, id: NodeId) -> Option<u64> {
    Wal::replay(cluster.node_dir(id).join(WAL_FILE)).unwrap().first().map(|e| e.index)
}

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

#[tokio::test]
async fn nodes_restarted_from_snapshots_hold_the_same_data_and_can_lead() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let original_leader = cluster.wait_for_leader(SETTLE).await;
    for i in 0..15 {
        write(&cluster, &[1, 2, 3], &format!("before-{i}"), &format!("v{i}")).await;
    }
    let followers: Vec<NodeId> =
        [1, 2, 3].into_iter().filter(|&id| id != original_leader).collect();
    cluster
        .wait_until(SETTLE, "followers to apply everything", |c| {
            followers
                .iter()
                .all(|&f| c.snapshot(f).last_applied == c.snapshot(original_leader).last_applied)
        })
        .await;
    let through = cluster.snapshot(followers[0]).last_applied;

    // Snapshot both followers and discard the log they have already applied.
    for &f in &followers {
        cluster.crash(f).await;
        compact(&cluster, f, through, Membership::of_ids([1, 2, 3]));
        assert_eq!(first_wal_index(&cluster, f), None, "everything was covered by the snapshot");
    }
    for &f in &followers {
        cluster.restart(f).await;
    }
    for &f in &followers {
        let snapshot = cluster.snapshot(f);
        assert!(snapshot.last_applied >= through, "starts from the snapshot, not from zero");
    }

    // They keep replicating with the leader that was never compacted.
    for i in 0..5 {
        write(&cluster, &[1, 2, 3], &format!("after-{i}"), &format!("w{i}")).await;
    }
    cluster
        .wait_until(SETTLE, "all nodes to apply the same entries", |c| {
            [1, 2, 3].iter().all(|&id| c.snapshot(id).last_applied == c.snapshot(1).last_applied)
        })
        .await;

    // Now the only nodes left are two that started from snapshots. One of them
    // leads, and everything, from before and after the snapshot point, is there.
    cluster.crash(original_leader).await;
    for i in 0..15 {
        assert_eq!(
            read(&cluster, &followers, &format!("before-{i}")).await,
            Some(format!("v{i}").into_bytes()),
            "before-{i} lives in the snapshot now"
        );
    }
    for i in 0..5 {
        assert_eq!(
            read(&cluster, &followers, &format!("after-{i}")).await,
            Some(format!("w{i}").into_bytes())
        );
    }
    write(&cluster, &followers, "later", "still-works").await;
    assert_eq!(read(&cluster, &followers, "later").await, Some(b"still-works".to_vec()));
}

#[tokio::test]
async fn the_snapshot_not_a_stale_environment_decides_who_the_members_are() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(SETTLE).await;
    write(&cluster, &[1, 2, 3], "k", "v").await;

    cluster.start_joiner(4);
    let added = cluster.add_member(leader, 4).await;
    assert!(matches!(added, AdminResult::Applied { .. }), "{added:?}");
    write(&cluster, &[1, 2, 3, 4], "k2", "v2").await;

    let follower = [1, 2, 3].into_iter().find(|&id| id != leader).unwrap();
    let original_others: Vec<NodeId> =
        [1, 2, 3].into_iter().filter(|&id| id != follower).collect();
    cluster
        .wait_until(SETTLE, "the follower to apply everything", |c| {
            c.snapshot(follower).last_applied == c.snapshot(leader).last_applied
        })
        .await;
    let through = cluster.snapshot(follower).last_applied;

    // Compact away the entry that added node 4. From now on the only record of node
    // 4's membership on this node is the snapshot.
    cluster.crash(follower).await;
    compact(
        &cluster,
        follower,
        through,
        Membership::new([
            (1, String::new()),
            (2, String::new()),
            (3, String::new()),
            (4, "node4:7000".to_string()),
        ]),
    );
    assert_eq!(first_wal_index(&cluster, follower), None);

    // Restart it with an OUT-OF-DATE peer list that has never heard of node 4.
    cluster.restart_with_peers(follower, &original_others).await;
    assert_eq!(
        cluster.members_of(follower),
        vec![1, 2, 3, 4],
        "the snapshot's membership wins over the environment"
    );

    // And it really acts on that: with the old leader gone, a majority of the four
    // (this node, another original and node 4) must be able to elect and commit,
    // which needs this node to talk to node 4 and to count its votes.
    write(&cluster, &[1, 2, 3, 4], "k3", "v3").await;
    cluster.crash(leader).await;
    let alive: Vec<NodeId> = [1, 2, 3, 4].into_iter().filter(|&id| id != leader).collect();
    write(&cluster, &alive, "k4", "v4").await;
    for (key, value) in [("k", "v"), ("k2", "v2"), ("k3", "v3"), ("k4", "v4")] {
        assert_eq!(read(&cluster, &alive, key).await, Some(value.as_bytes().to_vec()));
    }
    assert_eq!(cluster.members_of(follower), vec![1, 2, 3, 4]);
}

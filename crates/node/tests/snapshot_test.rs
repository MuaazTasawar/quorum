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

// ---- snapshots taken by the nodes themselves ------------------------------------

#[tokio::test]
async fn a_running_cluster_snapshots_trims_its_logs_and_restarts_from_them() {
    let mut cluster = TestCluster::start_with_snapshots(&[1, 2, 3], 10, 0);
    let all = [1, 2, 3];
    cluster.wait_for_leader(SETTLE).await;
    for i in 0..60 {
        write(&cluster, &all, &format!("key-{i}"), &format!("value-{i}")).await;
    }

    // Every node takes snapshots as it applies, and trims its log once it is safe.
    cluster
        .wait_until(SETTLE, "every node to snapshot and trim its log", |c| {
            all.iter().all(|&id| {
                let s = c.snapshot(id);
                s.snapshot_index >= 50 && s.log_base >= 50 && s.snapshots_taken >= 1
            })
        })
        .await;
    for id in all {
        let held = cluster.snapshot(id).log_len - cluster.snapshot(id).log_base;
        assert!(held < 15, "node {id} still holds {held} log entries");
    }

    // Stop everything and start it again: each node now comes back from a snapshot
    // plus the short tail of its log.
    for id in all {
        cluster.crash(id).await;
    }
    for id in all {
        cluster.restart(id).await;
    }
    for id in all {
        assert!(cluster.snapshot(id).snapshot_index >= 50, "node {id} restarts from its snapshot");
    }
    for i in 0..60 {
        assert_eq!(
            read(&cluster, &all, &format!("key-{i}")).await,
            Some(format!("value-{i}").into_bytes()),
            "key-{i} must survive compaction and a full restart"
        );
    }
    write(&cluster, &all, "after-restart", "ok").await;
    assert_eq!(read(&cluster, &all, "after-restart").await, Some(b"ok".to_vec()));
}

#[tokio::test]
async fn a_leader_never_discards_log_a_lagging_follower_still_needs() {
    let cluster = TestCluster::start_with_snapshots(&[1, 2, 3], 10, 0);
    let all = [1, 2, 3];
    let leader = cluster.wait_for_leader(SETTLE).await;
    let lagging = all.into_iter().find(|&id| id != leader).unwrap();
    let others: Vec<NodeId> = all.into_iter().filter(|&id| id != lagging).collect();
    write(&cluster, &all, "warmup", "1").await;
    cluster
        .wait_until(SETTLE, "the follower to be level", |c| {
            c.snapshot(lagging).last_applied == c.snapshot(leader).last_applied
        })
        .await;

    // The follower drops out while the others carry on and snapshot repeatedly.
    cluster.isolate(lagging);
    for i in 0..50 {
        write(&cluster, &others, &format!("key-{i}"), &format!("value-{i}")).await;
    }
    cluster
        .wait_until(SETTLE, "the leader to snapshot", |c| c.snapshot(leader).snapshot_index >= 40)
        .await;
    let behind = cluster.snapshot(lagging).last_applied;
    let leader_view = cluster.snapshot(leader);
    assert!(
        leader_view.log_base <= behind,
        "the leader trimmed to {} but the follower only has {behind}: it could never catch up",
        leader_view.log_base
    );
    assert!(leader_view.snapshot_index > behind, "the snapshot itself is well ahead of it");

    // It comes back and catches up through ordinary replication, then the leader
    // is free to trim.
    cluster.heal();
    cluster
        .wait_until(SETTLE, "the follower to catch up", |c| {
            c.snapshot(lagging).last_applied == c.snapshot(leader).last_applied
                && c.snapshot(lagging).last_applied >= 50
        })
        .await;
    cluster
        .wait_until(SETTLE, "the leader to trim now that nobody needs the log", |c| {
            c.snapshot(leader).log_base >= 40
        })
        .await;
    for i in 0..50 {
        assert_eq!(
            read(&cluster, &all, &format!("key-{i}")).await,
            Some(format!("value-{i}").into_bytes())
        );
    }
}

// ---- InstallSnapshot: catching up from a snapshot ---------------------------------

#[tokio::test]
async fn a_node_added_after_the_log_was_compacted_catches_up_from_a_snapshot() {
    // Small pieces, so the snapshot really does travel in many chunks.
    let mut cluster = TestCluster::start_with_snapshot_chunks(&[1, 2, 3], 5, 0, 96);
    let all = [1, 2, 3];
    cluster.wait_for_leader(SETTLE).await;
    for i in 0..40 {
        write(&cluster, &all, &format!("key-{i}"), &format!("value-{i}")).await;
    }
    cluster
        .wait_until(SETTLE, "the leader's log to be compacted", |c| {
            c.leaders().iter().any(|&l| c.snapshot(l).log_base > 0)
        })
        .await;

    cluster.start_joiner(4);
    let leader = cluster.wait_for_leader_among(&all, SETTLE).await;
    assert!(cluster.snapshot(leader).log_base > 0, "the entries node 4 needs are gone");
    let result = cluster.add_member(leader, 4).await;
    assert!(matches!(result, AdminResult::Applied { .. }), "{result:?}");

    let joined = cluster.snapshot(4);
    assert!(joined.snapshots_installed >= 1, "it was brought up to date by a snapshot");
    assert_eq!(cluster.members_of(4), vec![1, 2, 3, 4]);

    // It holds all the data and takes part: lose the old leader and node 4 is needed
    // for a majority of the four.
    write(&cluster, &[1, 2, 3, 4], "after-join", "ok").await;
    cluster
        .wait_until(SETTLE, "node 4 to apply everything", |c| {
            let leader = c.leaders()[0];
            c.snapshot(4).last_applied == c.snapshot(leader).last_applied
        })
        .await;
    cluster.crash(leader).await;
    let alive: Vec<NodeId> = [1, 2, 3, 4].into_iter().filter(|&id| id != leader).collect();
    for i in 0..40 {
        assert_eq!(
            read(&cluster, &alive, &format!("key-{i}")).await,
            Some(format!("value-{i}").into_bytes()),
            "key-{i} must have reached node 4 through the snapshot"
        );
    }
    assert_eq!(read(&cluster, &alive, "after-join").await, Some(b"ok".to_vec()));
}

#[tokio::test]
async fn a_follower_behind_a_new_leaders_compacted_log_is_caught_up_from_a_snapshot() {
    // Followers trim their own logs without waiting for anyone, so when one becomes
    // leader its log may start after what a lagging peer needs. Only a snapshot helps.
    let mut cluster = TestCluster::start_with_snapshot_chunks(&[1, 2, 3], 5, 0, 128);
    let all = [1, 2, 3];
    let old_leader = cluster.wait_for_leader(SETTLE).await;
    let lagging = all.into_iter().find(|&id| id != old_leader).unwrap();
    let healthy = all.into_iter().find(|&id| id != old_leader && id != lagging).unwrap();
    write(&cluster, &all, "warmup", "1").await;
    cluster
        .wait_until(SETTLE, "everyone to be level", |c| {
            all.iter().all(|&id| c.snapshot(id).last_applied == c.snapshot(old_leader).last_applied)
        })
        .await;

    cluster.isolate(lagging);
    let others = [old_leader, healthy];
    for i in 0..45 {
        write(&cluster, &others, &format!("key-{i}"), &format!("value-{i}")).await;
    }
    // The healthy follower snapshots and trims past what the lagging one has.
    cluster
        .wait_until(SETTLE, "the healthy follower to trim past the lagging one", |c| {
            c.snapshot(healthy).log_base > c.snapshot(lagging).last_applied + 5
        })
        .await;

    // The old leader goes; the lagging node comes back. The only node that can lead
    // is the healthy one (the other has an out of date log), and it cannot send the
    // entries the lagging node is missing.
    cluster.crash(old_leader).await;
    cluster.heal();
    let survivors = [healthy, lagging];
    cluster.wait_for_leader_among(&survivors, SETTLE).await;
    cluster
        .wait_until(SETTLE, "the lagging node to be brought up to date", |c| {
            c.snapshot(lagging).last_applied == c.snapshot(healthy).last_applied
                && c.snapshot(lagging).last_applied >= 45
        })
        .await;
    assert!(
        cluster.snapshot(lagging).snapshots_installed >= 1,
        "it got there by installing a snapshot"
    );
    for i in 0..45 {
        assert_eq!(
            read(&cluster, &survivors, &format!("key-{i}")).await,
            Some(format!("value-{i}").into_bytes())
        );
    }
    write(&cluster, &survivors, "later", "fine").await;
    assert_eq!(read(&cluster, &survivors, "later").await, Some(b"fine".to_vec()));
}

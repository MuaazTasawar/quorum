//! Multi-node consensus tests. Each node is a full, real `Cluster` task running
//! the actual election / replication / commit logic against a real on-disk
//! `Persistence` directory. Only the network is simulated (see `common`), which
//! is what lets these tests crash, restart and partition nodes.

mod common;

use common::TestCluster;
use node::cluster::{ClientCommandResult, QueryResult};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(5);

#[tokio::test]
async fn three_node_cluster_elects_exactly_one_leader() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader_id = cluster.wait_for_leader(Duration::from_secs(2)).await;

    // Give the rest of the cluster a moment to settle on the same leader
    // (heartbeats propagate) before asserting uniqueness.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let leaders = cluster.leaders();
    assert_eq!(leaders.len(), 1, "exactly one node must be leader, found {leaders:?}");
    assert_eq!(leaders[0], leader_id);
}

/// THE test: proves the entire premise of this project. A write is
/// committed by the leader, the leader's process is then killed outright
/// (no clean shutdown, no final heartbeat, matching a real crash), and the
/// surviving majority elects a new leader that still has the committed write.
/// Zero data loss despite losing the leader.
#[tokio::test]
async fn cluster_survives_leader_crash_with_zero_data_loss() {
    let mut cluster = TestCluster::start(&[1, 2, 3, 4, 5]);

    let leader_id = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let write_result = cluster.put(leader_id, "critical-key", "must-survive").await;
    assert!(
        matches!(write_result, ClientCommandResult::Applied(_)),
        "write to the leader should be applied, got {write_result:?}"
    );

    cluster.crash(leader_id).await;

    let new_leader_id = cluster.wait_for_leader(Duration::from_secs(3)).await;
    assert_ne!(
        new_leader_id, leader_id,
        "a NEW leader must be elected - the old one is dead, not just unresponsive"
    );

    // Give the new leader a moment to finish applying anything it inherited.
    tokio::time::sleep(Duration::from_millis(100)).await;

    match cluster.get(new_leader_id, "critical-key").await {
        QueryResult::Value(Some(bytes)) => {
            assert_eq!(bytes, b"must-survive".to_vec(), "value must match what was written");
        }
        other => panic!("expected the committed write to survive the leader crash, got {other:?}"),
    }
}

/// A restarted node must come back with the term and log it had when it died,
/// read from disk, not reset to term 0 with an empty log.
#[tokio::test]
async fn restarted_node_recovers_its_term_and_log_from_disk() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader_id = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let follower_id = cluster.live_ids().into_iter().find(|&id| id != leader_id).unwrap();

    let result = cluster.put(leader_id, "k", "v").await;
    assert!(matches!(result, ClientCommandResult::Applied(_)));

    // Wait until the follower has replicated everything the leader has.
    cluster
        .wait_until(SETTLE, "follower to replicate the leader's whole log", |c| {
            let f = c.snapshot(follower_id);
            f.log_len >= 2 && f.log_len == c.snapshot(leader_id).log_len
        })
        .await;

    let before = cluster.snapshot(follower_id);
    assert!(before.current_term >= 1);

    cluster.crash(follower_id).await;
    cluster.restart(follower_id).await;

    // Read immediately: this is the state recovered from disk, before the node
    // has exchanged a single message.
    let after = cluster.snapshot(follower_id);
    assert_eq!(after.current_term, before.current_term, "term must survive the restart");
    assert_eq!(after.log_len, before.log_len, "log must survive the restart");
}

#[tokio::test]
async fn restarted_follower_catches_up_on_writes_it_missed() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader_id = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let follower_id = cluster.live_ids().into_iter().find(|&id| id != leader_id).unwrap();

    assert!(matches!(
        cluster.put(leader_id, "k1", "v1").await,
        ClientCommandResult::Applied(_)
    ));

    cluster.crash(follower_id).await;

    // The leader and the remaining follower are still a majority of three.
    assert!(matches!(
        cluster.put(leader_id, "k2", "v2").await,
        ClientCommandResult::Applied(_)
    ));
    assert!(matches!(
        cluster.put(leader_id, "k3", "v3").await,
        ClientCommandResult::Applied(_)
    ));
    let target = cluster.snapshot(leader_id).last_applied;
    assert!(target >= 4, "no-op + three writes should be applied, got {target}");

    cluster.restart(follower_id).await;

    cluster
        .wait_until(SETTLE, "restarted follower to apply the writes it missed", |c| {
            c.snapshot(follower_id).last_applied >= target
        })
        .await;
}

/// The harness can cut links, so this also proves the cluster recovers from a
/// partition: an isolated leader is replaced, and once the network heals it
/// steps down and converges on the new leader's log.
#[tokio::test]
async fn partitioned_leader_is_replaced_then_rejoins_as_a_follower() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let others: Vec<u32> = cluster.live_ids().into_iter().filter(|&id| id != old_leader).collect();

    assert!(matches!(
        cluster.put(old_leader, "before", "partition").await,
        ClientCommandResult::Applied(_)
    ));

    cluster.isolate(old_leader);

    // The two connected nodes elect a replacement among themselves.
    let new_leader = cluster.wait_for_leader_among(&others, Duration::from_secs(3)).await;
    assert_ne!(new_leader, old_leader);
    assert!(
        cluster.snapshot(new_leader).current_term > cluster.snapshot(old_leader).current_term,
        "the replacement leader must be in a newer term"
    );

    assert!(matches!(
        cluster.put(new_leader, "during", "partition").await,
        ClientCommandResult::Applied(_)
    ));

    cluster.heal();

    // The old leader hears a newer term, steps down, and catches up.
    cluster
        .wait_until(SETTLE, "old leader to step down and catch up", |c| {
            let old = c.snapshot(old_leader);
            old.role != "Leader" && old.last_applied >= c.snapshot(new_leader).last_applied
        })
        .await;

    cluster
        .wait_until(SETTLE, "exactly one leader after healing", |c| c.leaders().len() == 1)
        .await;

    let leader = cluster.leaders()[0];
    match cluster.get(leader, "during").await {
        QueryResult::Value(Some(v)) => assert_eq!(v, b"partition".to_vec()),
        other => panic!("write made during the partition must be visible, got {other:?}"),
    }
}
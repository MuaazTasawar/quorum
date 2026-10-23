//! A node that is cut off from the cluster must not be able to disturb it.
//!
//! Without pre-vote, an isolated follower keeps timing out, bumping its term each
//! time, and when the network heals its inflated term deposes a perfectly healthy
//! leader. Without check-quorum, an isolated leader keeps believing it is the
//! leader. These tests partition real multi-node clusters and check that neither
//! happens.

mod common;

use common::TestCluster;
use node::cluster::ClientCommandResult;
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(5);

/// Several election timeouts' worth of waiting: long enough that an isolated
/// node would have timed out (and, without pre-vote, bumped its term) many times.
const LONG_ENOUGH_TO_TIME_OUT_MANY_TIMES: Duration = Duration::from_millis(2000);

#[tokio::test]
async fn an_isolated_follower_does_not_inflate_its_term() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let follower = cluster.live_ids().into_iter().find(|&id| id != leader).unwrap();

    // Let the follower settle into the leader's term before cutting it off.
    cluster
        .wait_until(SETTLE, "follower to adopt the leader's term", |c| {
            c.snapshot(follower).current_term == c.snapshot(leader).current_term
        })
        .await;
    let term_before = cluster.snapshot(follower).current_term;

    cluster.isolate(follower);
    tokio::time::sleep(LONG_ENOUGH_TO_TIME_OUT_MANY_TIMES).await;

    let after = cluster.snapshot(follower);
    assert_eq!(
        after.current_term, term_before,
        "an isolated node must not burn terms: it was {term_before}, now {}",
        after.current_term
    );
    assert_ne!(after.role, "Leader");
}

/// The headline guarantee: reconnecting an isolated node does not disturb the
/// leader that kept the cluster running without it.
#[tokio::test]
async fn a_follower_rejoining_after_a_partition_does_not_depose_the_leader() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let follower = cluster.live_ids().into_iter().find(|&id| id != leader).unwrap();
    cluster
        .wait_until(SETTLE, "follower to adopt the leader's term", |c| {
            c.snapshot(follower).current_term == c.snapshot(leader).current_term
        })
        .await;
    let leader_term = cluster.snapshot(leader).current_term;

    cluster.isolate(follower);
    tokio::time::sleep(LONG_ENOUGH_TO_TIME_OUT_MANY_TIMES).await;
    cluster.heal();

    // Give any disruption time to show itself.
    tokio::time::sleep(Duration::from_millis(800)).await;

    assert_eq!(cluster.leaders(), vec![leader], "the same node must still be the leader");
    assert_eq!(
        cluster.snapshot(leader).current_term,
        leader_term,
        "no election may have been forced on the healthy leader"
    );
    assert!(matches!(
        cluster.put(leader, "after-rejoin", "ok").await,
        ClientCommandResult::Applied(_)
    ));
    // The rejoined node catches up and accepts the leader's term.
    cluster
        .wait_until(SETTLE, "rejoined follower to catch up", |c| {
            c.snapshot(follower).last_applied >= c.snapshot(leader).last_applied
        })
        .await;
}

/// An asymmetric failure: one follower stops hearing the leader, but can still
/// talk to everyone, including the other follower, which still hears the leader
/// fine. The cut-off follower's election timer keeps firing and it keeps asking
/// for pre-votes. The other follower must refuse (it has a live leader), or the
/// two of them would form a majority and force an election against a leader that
/// is working perfectly. This is what the "heard from a leader recently" rule is for.
#[tokio::test]
async fn a_follower_that_stops_hearing_the_leader_cannot_force_an_election() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let deaf_follower = cluster.live_ids().into_iter().find(|&id| id != leader).unwrap();
    cluster
        .wait_until(SETTLE, "follower to adopt the leader's term", |c| {
            c.snapshot(deaf_follower).current_term == c.snapshot(leader).current_term
        })
        .await;
    let leader_term = cluster.snapshot(leader).current_term;

    // Only the leader's messages to this follower are lost.
    cluster.cut_link(leader, deaf_follower);
    tokio::time::sleep(LONG_ENOUGH_TO_TIME_OUT_MANY_TIMES).await;

    assert_eq!(cluster.leaders(), vec![leader], "the leader must be undisturbed");
    assert_eq!(cluster.snapshot(leader).current_term, leader_term);
    assert_eq!(
        cluster.snapshot(deaf_follower).current_term,
        leader_term,
        "the follower that cannot hear the leader must not have started elections"
    );
    assert!(matches!(
        cluster.put(leader, "still-working", "yes").await,
        ClientCommandResult::Applied(_)
    ));
}

#[tokio::test]
async fn a_leader_cut_off_from_the_majority_steps_down_without_changing_its_term() {
    let cluster = TestCluster::start(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let others: Vec<u32> = cluster.live_ids().into_iter().filter(|&id| id != old_leader).collect();
    let old_term = cluster.snapshot(old_leader).current_term;

    cluster.isolate(old_leader);

    cluster
        .wait_until(Duration::from_secs(3), "the cut-off leader to step down", |c| {
            c.snapshot(old_leader).role != "Leader"
        })
        .await;
    assert_eq!(
        cluster.snapshot(old_leader).current_term,
        old_term,
        "stepping down on its own must not change the term"
    );

    // The connected majority carries on with a new leader in a newer term.
    let new_leader = cluster.wait_for_leader_among(&others, Duration::from_secs(3)).await;
    assert!(cluster.snapshot(new_leader).current_term > old_term);

    cluster.heal();
    cluster
        .wait_until(SETTLE, "a single leader after healing", |c| c.leaders() == vec![new_leader])
        .await;
}

/// A minority partition can neither elect a leader nor burn terms, and when it
/// heals the majority's leader is undisturbed.
#[tokio::test]
async fn a_minority_partition_elects_no_leader_and_does_not_disturb_the_majority() {
    let cluster = TestCluster::start(&[1, 2, 3, 4, 5]);
    let old_leader = cluster.wait_for_leader(Duration::from_secs(2)).await;

    // Cut the leader plus one other node off from the remaining three.
    let minority_partner = cluster.live_ids().into_iter().find(|&id| id != old_leader).unwrap();
    let minority = vec![old_leader, minority_partner];
    let majority: Vec<u32> =
        cluster.live_ids().into_iter().filter(|id| !minority.contains(id)).collect();
    let term_before = cluster.snapshot(old_leader).current_term;
    cluster.partition(&minority, &majority);

    // The majority elects its own leader...
    let majority_leader = cluster.wait_for_leader_among(&majority, Duration::from_secs(3)).await;
    let majority_term = cluster.snapshot(majority_leader).current_term;
    assert!(majority_term > term_before);

    // ...while the minority never does, and never inflates its terms.
    tokio::time::sleep(LONG_ENOUGH_TO_TIME_OUT_MANY_TIMES).await;
    for &id in &minority {
        let snap = cluster.snapshot(id);
        assert_ne!(snap.role, "Leader", "node {id} in the minority must not be leader");
        assert_eq!(
            snap.current_term, term_before,
            "node {id} in the minority must not burn terms"
        );
    }

    // Healing disturbs nobody: the majority's leader keeps its term.
    cluster.heal();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(cluster.leaders(), vec![majority_leader]);
    assert_eq!(cluster.snapshot(majority_leader).current_term, majority_term);
}

/// Pre-vote must not make elections impossible: after a real leader failure the
/// survivors still elect a replacement.
#[tokio::test]
async fn pre_vote_still_lets_the_survivors_replace_a_crashed_leader() {
    let mut cluster = TestCluster::start(&[1, 2, 3]);
    let leader = cluster.wait_for_leader(Duration::from_secs(2)).await;
    let term_before = cluster.snapshot(leader).current_term;

    cluster.crash(leader).await;

    let replacement = cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert_ne!(replacement, leader);
    assert!(cluster.snapshot(replacement).current_term > term_before);
}
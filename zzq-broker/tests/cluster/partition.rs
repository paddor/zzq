//! Link cuts between brokers: which one leads after each, and that the
//! leader then serves every acked message.

use std::time::Duration;

use zzq_proto::QoS;

use super::{check, fill, next};
use crate::sim::{Cluster, simulated};

/// Waits until all three run one view and both followers applied what
/// node `l` committed.
async fn settled(c: &Cluster, l: usize) {
    let others = [(l + 1) % 3, (l + 2) % 3];
    assert!(
        c.until(10, |c| others.iter().all(|&f| c.caught_up(f, l)))
            .await,
        "the cluster settles under node {l}"
    );
}

/// A cut that leaves the leader one follower both ways: it keeps leading
/// and its clients, through several seconds of acked publishes.
async fn leader_keeps(cut: impl Fn(&Cluster, bool)) {
    let c = Cluster::new().await;
    assert!(c.until(2, |c| c.leader() == Some(0)).await);
    let mut sub = c.client(0, "sub", true).await;
    sub.subscribe("orders/#", QoS::AtLeastOnce).await.unwrap();
    let publisher = c.client(0, "pub", true).await;
    cut(&c, true);
    for i in 0..3 {
        publisher
            .publish("orders/1", format!("m{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
        assert_eq!(next(&mut sub).await.payload, format!("m{i}"));
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    assert_eq!(c.leader(), Some(0), "one suspicion moved the view");
    cut(&c, false);
    settled(&c, 0).await;
}

#[test]
fn a_leader_cut_from_one_follower_keeps_leading_and_its_clients() {
    simulated(|| leader_keeps(|c, cut| c.cut_pair(0, 1, cut)));
}

#[test]
fn followers_cut_from_each_other_leave_the_leader_and_its_clients_alone() {
    simulated(|| leader_keeps(|c, cut| c.cut_pair(1, 2, cut)));
}

#[test]
fn a_follower_deaf_to_the_leader_leaves_it_alone() {
    simulated(|| leader_keeps(|c, cut| c.cut(0, 1, cut)));
}

/// Leader 0 loses one direction to both followers: it hears no ack when
/// `deaf`, or no follower hears it otherwise. A publish it takes right
/// after the cut can never commit. Another node comes to lead and serves
/// every message acked before.
async fn one_sided_leader(deaf: bool) {
    let c = Cluster::new().await;
    assert!(c.until(2, |c| c.leader() == Some(0)).await);
    fill(&c, 0, 3).await;
    let late = c.client(0, "late", true).await;
    for f in [1, 2] {
        if deaf {
            c.cut(f, 0, true);
        } else {
            c.cut(0, f, true);
        }
    }
    tokio::task::spawn_local(async move {
        let _ = late.publish("orders/1", "unacked", QoS::AtLeastOnce).await;
    });
    assert!(
        c.until(10, |c| c.leader().is_some_and(|l| l != 0)).await,
        "a one-sided leader still leads"
    );
    let l = c.leader().unwrap();
    check(&c, l, 3).await;
    c.isolate(0, false);
    settled(&c, l).await;
}

#[test]
fn a_leader_that_hears_no_follower_is_replaced() {
    simulated(|| one_sided_leader(true));
}

#[test]
fn a_leader_no_follower_hears_is_replaced() {
    simulated(|| one_sided_leader(false));
}

/// Node 2 misses three views nodes 0 and 1 form, then hears only the
/// follower of the last when its leader is cut off. The two elect within
/// one election timeout and serve what was acked.
#[test]
fn a_node_that_missed_views_joins_them_through_a_follower() {
    simulated(|| async {
        let c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        fill(&c, 0, 3).await;
        c.isolate(2, true);
        for _ in 0..3 {
            let l = c.leader().unwrap();
            let before = c.status(l).unwrap().view;
            c.cut_pair(0, 1, true);
            tokio::time::sleep(Duration::from_millis(400)).await;
            c.cut_pair(0, 1, false);
            assert!(
                c.until(10, |c| c.leader().is_some_and(|n| n != 2)
                    && (0..2).all(|i| c.status(i).unwrap().view > before))
                    .await,
                "nodes 0 and 1 move on"
            );
        }
        let l = c.leader().unwrap();
        let follower = 1 - l;
        assert!(c.status(2).unwrap().view + 3 <= c.status(l).unwrap().view);
        c.cut_pair(2, follower, false);
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.isolate(l, true);
        let start = tokio::time::Instant::now();
        assert!(
            c.until(10, |c| c.leader().is_some_and(|n| n != l)).await,
            "the two form a view"
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "{:?}",
            start.elapsed()
        );
        let n = c.leader().unwrap();
        check(&c, n, 3).await;
        c.isolate(l, false);
        settled(&c, n).await;
    });
}

//! Brokers as nodes of a three-node cluster, on one thread under a paused
//! clock.

#![expect(clippy::unwrap_used, clippy::expect_used, reason = "tests fail loudly")]

#[path = "cluster/sim.rs"]
mod sim;
#[path = "cluster/workload.rs"]
mod workload;

use std::time::Duration;

use sim::{Cluster, NODES, simulated};
use zzq_client::{Client, Message};
use zzq_cluster::node::{LogLimits, Role};
use zzq_proto::QoS;

#[test]
fn a_new_cluster_elects_the_first_node() {
    simulated(|| async {
        let c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
    });
}

#[test]
fn a_queued_message_survives_the_leader_and_reaches_its_subscriber() {
    simulated(|| async {
        let mut c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        let sub = c.client(0, "sub", true).await;
        sub.subscribe("orders/#", QoS::AtLeastOnce).await.unwrap();
        sub.disconnect().await;
        let publisher = c.client(0, "pub", true).await;
        publisher
            .publish("orders/1", "first", QoS::AtLeastOnce)
            .await
            .unwrap();
        // The PUBACK came: the message is in RAM on a follower too.
        tokio::time::sleep(Duration::from_millis(50)).await;
        c.crash(0);
        assert!(
            c.until(5, |c| c.leader().is_some_and(|l| l != 0)).await,
            "another node leads"
        );
        let l = c.leader().unwrap();
        let mut sub = c.client(l, "sub", false).await;
        let m = tokio::time::timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("delivered")
            .unwrap();
        assert_eq!(m.payload, "first");
    });
}

#[test]
fn an_isolated_leader_steps_down_and_drops_its_clients() {
    simulated(|| async {
        let c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        let mut client = c.client(0, "a", true).await;
        c.isolate(0, true);
        assert!(
            c.until(5, |c| c.leader().is_some_and(|l| l != 0)).await,
            "another node leads"
        );
        assert_ne!(c.status(0).unwrap().role, Role::Leader);
        let end = tokio::time::timeout(Duration::from_secs(5), client.recv()).await;
        assert!(end.expect("dropped").is_none(), "the client is gone");
        assert!(c.connect(0, "b", true).await.is_err(), "it refuses clients");
    });
}

#[test]
fn a_deposed_leader_rebuilds_its_state_and_serves_it_when_it_leads_again() {
    simulated(|| async {
        let c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        let sub = c.client(0, "sub", true).await;
        sub.subscribe("orders/#", QoS::AtLeastOnce).await.unwrap();
        sub.disconnect().await;
        c.isolate(0, true);
        assert!(c.until(5, |c| c.leader() == Some(1)).await, "node 1 leads");
        let publisher = c.client(1, "pub", true).await;
        publisher
            .publish("orders/1", "while away", QoS::AtLeastOnce)
            .await
            .unwrap();
        c.isolate(0, false);
        assert!(c.until(5, |c| c.caught_up(0, 1)).await, "node 0 follows");
        c.lead(0).await;
        let mut sub = c.client(0, "sub", false).await;
        assert_eq!(next(&mut sub).await.payload, "while away");
    });
}

const fn small_log() -> LogLimits {
    LogLimits {
        max_operations: 16,
        max_body_bytes: 1 << 20,
    }
}

async fn next(client: &mut Client) -> Message {
    tokio::time::timeout(Duration::from_secs(5), client.recv())
        .await
        .expect("delivered")
        .expect("connected")
}

/// A persistent subscriber to `orders/#` and a retained message, then
/// `n` messages for the subscriber, through the leader `l`.
async fn fill(c: &Cluster, l: usize, n: usize) {
    let sub = c.client(l, "sub", true).await;
    sub.subscribe("orders/#", QoS::AtLeastOnce).await.unwrap();
    sub.disconnect().await;
    let publisher = c.client(l, "pub", true).await;
    publisher
        .publish_retained("config/a", "kept", QoS::AtLeastOnce)
        .await
        .unwrap();
    for i in 0..n {
        publisher
            .publish("orders/1", format!("m{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
}

/// Node `l` leads and serves what [`fill`] left.
async fn check(c: &Cluster, l: usize, n: usize) {
    let mut sub = c.client(l, "sub", false).await;
    for i in 0..n {
        assert_eq!(next(&mut sub).await.payload, format!("m{i}"));
    }
    let mut reader = c.client(l, "reader", true).await;
    reader
        .subscribe("config/#", QoS::AtLeastOnce)
        .await
        .unwrap();
    let m = next(&mut reader).await;
    assert!(m.retain && m.payload == "kept", "{m:?}");
}

#[test]
fn a_node_cut_off_past_the_log_catches_up_from_an_image_and_serves_it() {
    simulated(|| async {
        let c = Cluster::with_log(small_log()).await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        c.isolate(2, true);
        fill(&c, 0, 100).await;
        // Long enough that the leader keeps no ops for the lost node.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let noise = c.client(0, "noise", true).await;
        for i in 0..20 {
            let topic = format!("noise/{i}");
            noise
                .publish_retained(&topic, "x", QoS::AtLeastOnce)
                .await
                .unwrap();
        }
        let behind = c.status(2).unwrap().applied;
        assert!(c.status(0).unwrap().log_base > behind, "the log moved on");
        c.isolate(2, false);
        assert!(c.until(10, |c| c.caught_up(2, 0)).await, "node 2 follows");
        c.lead(2).await;
        check(&c, 2, 100).await;
    });
}

#[test]
fn a_node_that_lost_its_state_recovers_it_from_the_others_and_serves_it() {
    simulated(|| async {
        let mut c = Cluster::with_log(small_log()).await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        fill(&c, 0, 100).await;
        c.crash(2);
        c.restart(2).await;
        assert!(c.until(10, |c| c.caught_up(2, 0)).await, "node 2 follows");
        c.lead(2).await;
        check(&c, 2, 100).await;
    });
}

#[test]
fn a_cluster_stopped_node_by_node_comes_back_from_its_dumps() {
    for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0]] {
        simulated(|| async move {
            let mut c = Cluster::with_log(small_log()).await;
            assert!(c.until(2, |c| c.leader() == Some(0)).await);
            fill(&c, 0, 20).await;
            for (k, i) in order.into_iter().enumerate() {
                assert!(c.stop(i).await, "{order:?}: node {i} dumps");
                if k == 0 {
                    assert!(c.until(5, |c| c.leader().is_some()).await, "{order:?}");
                }
            }
            for i in 0..NODES {
                c.restart(i).await;
            }
            assert!(c.until(10, |c| c.leader().is_some()).await, "{order:?}");
            check(&c, c.leader().unwrap(), 20).await;
        });
    }
}

#[test]
fn a_node_restarts_from_its_dump_and_catches_up_with_what_it_missed() {
    simulated(|| async {
        let mut c = Cluster::with_log(small_log()).await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        fill(&c, 0, 10).await;
        assert!(c.stop(2).await, "node 2 dumps");
        let publisher = c.client(0, "pub", false).await;
        for i in 10..20 {
            let payload = format!("m{i}");
            publisher
                .publish("orders/1", payload, QoS::AtLeastOnce)
                .await
                .unwrap();
        }
        c.restart(2).await;
        assert!(c.until(10, |c| c.caught_up(2, 0)).await, "node 2 follows");
        c.lead(2).await;
        check(&c, 2, 20).await;
    });
}

/// Seeds a sweep runs: `ZZQ_SIM_SEEDS`, or a few.
fn seeds() -> u64 {
    std::env::var("ZZQ_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
}

#[test]
fn seeded_faults_never_lose_an_acked_message() {
    for seed in 0..seeds() {
        let mut result = Ok(());
        simulated(|| async {
            result = workload::run(seed).await;
        });
        if let Err(e) = result {
            panic!("seed {seed}: {e}");
        }
    }
}

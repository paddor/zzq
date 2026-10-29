//! Brokers as nodes of a three-node cluster, on one thread under a paused
//! clock.

#![expect(clippy::unwrap_used, clippy::expect_used, reason = "tests fail loudly")]

#[path = "cluster/sim.rs"]
mod sim;
#[path = "cluster/workload.rs"]
mod workload;

use std::time::Duration;

use sim::{Cluster, NODES, simulated};
use zzq_client::{Client, ClientOptions, LastWill, Message};
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

/// Seeds a sweep runs: `ZZQ_SIM_SEED` alone, or the first
/// `ZZQ_SIM_SEEDS`, or a few.
fn seeds() -> std::ops::Range<u64> {
    let var = |name| std::env::var(name).ok().and_then(|s| s.parse().ok());
    match (var("ZZQ_SIM_SEED"), var("ZZQ_SIM_SEEDS")) {
        (Some(seed), _) => seed..seed + 1,
        (None, n) => 0..n.unwrap_or(3),
    }
}

#[test]
fn seeded_faults_never_lose_an_acked_message() {
    for seed in seeds() {
        let mut result = Ok(());
        simulated(|| async {
            result = workload::run(seed).await;
        });
        if let Err(e) = result {
            panic!("seed {seed}: {e}");
        }
    }
}

#[test]
fn a_forced_node_brings_back_its_state_when_the_others_lost_theirs() {
    simulated(|| async {
        let mut c = Cluster::with_log(small_log()).await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        fill(&c, 0, 20).await;
        assert!(c.stop(0).await, "node 0 dumps");
        c.crash(1);
        c.crash(2);
        c.restart(1).await;
        c.restart(2).await;
        assert!(
            !c.until(3, |c| c.leader().is_some()).await,
            "no two nodes have the state"
        );
        c.force(0).await;
        assert!(c.until(10, |c| c.leader() == Some(0)).await, "node 0 leads");
        assert!(
            c.until(10, |c| c.caught_up(1, 0) && c.caught_up(2, 0))
                .await,
            "both follow"
        );
        assert_eq!(c.status(1).map(|s| s.epoch), Some(2));
        c.lead(1).await;
        check(&c, 1, 20).await;
    });
}

#[test]
fn after_all_three_crash_a_forced_node_starts_from_the_dump_it_last_loaded() {
    simulated(|| async {
        let mut c = Cluster::with_log(small_log()).await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        fill(&c, 0, 10).await;
        for i in [1, 2, 0] {
            assert!(c.stop(i).await, "node {i} dumps");
        }
        for i in 0..NODES {
            c.restart(i).await;
        }
        assert!(
            c.until(10, |c| c.leader().is_some()).await,
            "the dumps elect a leader"
        );
        // Power goes: no node dumps, but each set aside the dump it loaded.
        for i in 0..NODES {
            c.crash(i);
        }
        for i in 1..NODES {
            c.restart(i).await;
        }
        c.force(0).await;
        assert!(c.until(10, |c| c.leader() == Some(0)).await, "node 0 leads");
        check(&c, 0, 10).await;
    });
}

#[test]
fn a_leader_that_misses_acks_for_a_moment_keeps_its_sessions_and_clients() {
    simulated(|| async {
        let c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        let sub = c.client(0, "sub", true).await;
        sub.subscribe("orders/#", QoS::AtLeastOnce).await.unwrap();
        sub.disconnect().await;
        let mut sub = c.client(0, "sub", false).await;
        let publisher = c.client(0, "pub", true).await;
        publisher
            .publish("orders/1", "m0", QoS::AtLeastOnce)
            .await
            .unwrap();
        assert_eq!(next(&mut sub).await.payload, "m0");
        // The followers still hear it, but their acks are lost: its lease
        // ends, its view does not, as long as its last op commits before
        // the followers give up on it.
        c.cut(1, 0, true);
        c.cut(2, 0, true);
        assert!(c.until(1, |c| c.leader().is_none()).await, "the lease ends");
        let pending = publisher.publish("orders/1", "m1", QoS::AtLeastOnce);
        let heal = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            c.cut(1, 0, false);
            c.cut(2, 0, false);
        };
        let (acked, ()) = tokio::join!(pending, heal);
        acked.unwrap();
        assert_eq!(c.leader(), Some(0), "it leads again");
        assert_eq!(next(&mut sub).await.payload, "m1", "its client stayed");
        let again = c.client(0, "sub2", true).await;
        again.disconnect().await;
    });
}

/// A client of node 0 with a will to `wills/<id>` after `delay` seconds,
/// and a session that outlives its connection by `expiry` seconds.
async fn with_will(c: &Cluster, id: &str, delay: u32, expiry: u32) -> Client {
    let opts = ClientOptions {
        clean_start: true,
        session_expiry: expiry,
        will: Some(LastWill {
            topic: format!("wills/{id}"),
            payload: "gone".into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay,
        }),
        ..sim::options(id)
    };
    c.connect_with(0, opts).await.unwrap()
}

/// A persistent subscriber to `wills/#`, gone for now.
async fn will_watcher(c: &Cluster) {
    let watch = c.client(0, "watch", true).await;
    watch.subscribe("wills/#", QoS::AtLeastOnce).await.unwrap();
    watch.disconnect().await;
}

/// The node that leads once leader 0 crashed.
async fn fail_over(c: &mut Cluster) -> usize {
    // Its records are on a follower.
    tokio::time::sleep(Duration::from_millis(50)).await;
    c.crash(0);
    assert!(c.until(5, |c| c.leader().is_some_and(|l| l != 0)).await);
    c.leader().unwrap()
}

#[test]
fn the_will_of_a_client_connected_at_a_dead_leader_goes_out_from_the_next() {
    simulated(|| async {
        let mut c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        will_watcher(&c).await;
        let _dev = with_will(&c, "dev", 0, 60).await;
        let l = fail_over(&mut c).await;
        let mut watch = c.client(l, "watch", false).await;
        let will = next(&mut watch).await;
        assert_eq!(
            (&*will.topic, &will.payload[..]),
            ("wills/dev", &b"gone"[..])
        );
    });
}

#[test]
fn a_client_back_within_its_will_delay_keeps_its_session_and_sends_no_will() {
    simulated(|| async {
        let mut c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        will_watcher(&c).await;
        let _dev = with_will(&c, "dev", 30, 60).await;
        let l = fail_over(&mut c).await;
        let opts = ClientOptions {
            clean_start: false,
            session_expiry: 60,
            ..sim::options("dev")
        };
        let dev = c.connect_with(l, opts).await.unwrap();
        assert!(dev.connected().session_present, "its session moved");
        let mut watch = c.client(l, "watch", false).await;
        let got = tokio::time::timeout(Duration::from_secs(60), watch.recv()).await;
        assert!(got.is_err(), "no will: {got:?}");
    });
}

#[test]
fn a_session_connected_at_a_dead_leader_expires_on_the_next() {
    simulated(|| async {
        let mut c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        let short = ClientOptions {
            clean_start: true,
            session_expiry: 3,
            ..sim::options("short")
        };
        let _short = c.connect_with(0, short).await.unwrap();
        let _long = c.client(0, "long", true).await;
        let l = fail_over(&mut c).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        let back = |id: &str| ClientOptions {
            clean_start: false,
            ..sim::options(id)
        };
        let short = c.connect_with(l, back("short")).await.unwrap();
        assert!(
            !short.connected().session_present,
            "expired 3 s after the failover"
        );
        let long = c.connect_with(l, back("long")).await.unwrap();
        assert!(long.connected().session_present, "kept for its hour");
    });
}

//! Brokers whose clocks disagree: deadlines follow the time most clocks
//! agree on, and wait while none do.

use std::time::Duration;

use zzq_client::ClientOptions;

use super::fail_over;
use crate::sim::{self, Cluster, NODES, simulated};

const HOUR_MS: i64 = 3_600_000;

/// A client that comes back to session `id` at node `l`.
fn back(id: &str) -> ClientOptions {
    ClientOptions {
        clean_start: false,
        ..sim::options(id)
    }
}

/// An offline session `id` at leader 0 that expires `secs` after now.
async fn offline(c: &Cluster, id: &str, secs: u32) {
    let opts = ClientOptions {
        clean_start: true,
        session_expiry: secs,
        ..sim::options(id)
    };
    c.connect_with(0, opts).await.unwrap().disconnect().await;
}

/// Waits until node `l` takes clients, at most `secs`.
async fn serving(c: &Cluster, l: usize, secs: u64) -> bool {
    for _ in 0..secs * 10 {
        if let Ok(probe) = c.connect(l, "probe", true).await {
            probe.disconnect().await;
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[test]
fn a_node_whose_clock_is_hours_ahead_keeps_sessions_on_the_others_time() {
    simulated(|| async {
        let mut c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        // One session reaches node 1 in the image it recovers from, the
        // other in an op.
        offline(&c, "image", 3600).await;
        c.set_clock(1, 2 * HOUR_MS);
        c.crash(1);
        c.restart(1).await;
        assert!(c.until(10, |c| c.caught_up(1, 0)).await, "node 1 follows");
        let ahead = |c: &Cluster| {
            let m = c.status(1).and_then(|s| s.clock.majority);
            m.is_some_and(|m| (m + 2 * HOUR_MS).abs() < 100)
        };
        assert!(c.until(5, ahead).await, "node 1 sees its clock ahead");
        offline(&c, "op", 3600).await;
        let l = fail_over(&mut c).await;
        assert_eq!(l, 1, "the next view's leader");
        tokio::time::sleep(Duration::from_secs(5)).await;
        for id in ["image", "op"] {
            let s = c.connect_with(l, back(id)).await.unwrap();
            assert!(s.connected().session_present, "{id}: kept for its hour");
        }
    });
}

#[test]
fn a_restart_where_no_two_clocks_agree_holds_expiry_until_most_do() {
    simulated(|| async {
        let mut c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        offline(&c, "a", 20).await;
        offline(&c, "b", 20).await;
        for i in [2, 1, 0] {
            assert!(c.stop(i).await, "node {i} dumps");
        }
        c.set_clock(1, 10_000);
        c.restart(0).await;
        c.restart(1).await;
        assert!(c.until(10, |c| c.leader().is_some()).await);
        let l = c.leader().unwrap();
        let start = tokio::time::Instant::now();
        assert!(serving(&c, l, 10).await, "it leads without a majority");
        assert!(
            start.elapsed() >= Duration::from_secs(4),
            "it waited for one first: {:?}",
            start.elapsed()
        );
        tokio::time::sleep(Duration::from_secs(30)).await;
        let a = c.connect_with(l, back("a")).await.unwrap();
        assert!(a.connected().session_present, "expiry waits");
        c.restart(2).await;
        let agreed = |c: &Cluster| {
            (0..NODES).all(|i| c.status(i).is_some_and(|s| s.clock.majority.is_some()))
        };
        assert!(c.until(5, agreed).await, "most clocks agree again");
        tokio::time::sleep(Duration::from_secs(5)).await;
        let b = c.connect_with(l, back("b")).await.unwrap();
        assert!(!b.connected().session_present, "expired once they agree");
    });
}

#[test]
fn a_restart_with_agreeing_clocks_serves_without_waiting() {
    simulated(|| async {
        let mut c = Cluster::new().await;
        assert!(c.until(2, |c| c.leader() == Some(0)).await);
        for i in [2, 1, 0] {
            assert!(c.stop(i).await, "node {i} dumps");
        }
        for i in 0..NODES {
            c.restart(i).await;
        }
        assert!(c.until(5, |c| c.leader().is_some()).await);
        let start = tokio::time::Instant::now();
        assert!(serving(&c, c.leader().unwrap(), 2).await);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "{:?}",
            start.elapsed()
        );
    });
}

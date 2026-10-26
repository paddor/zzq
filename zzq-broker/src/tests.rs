//! Regression tests that look inside the lanes: their tasks and state.

#[path = "../tests/support/mod.rs"]
mod support;

use std::rc::Rc;
use std::time::Duration;

use zzq_client::{Client, ClientOptions, LastWill};
use zzq_proto::QoS;

use crate::lane::Lane;
use crate::{Broker, BrokerOptions, Context};
use support::{WAIT, setup, stats_until};

/// Holders of lane `i`'s state: its loops, and every task of a connection,
/// timer or wait that runs on it.
async fn lane_refs(ctx: &Context, broker: &Broker, i: usize) -> usize {
    let id = broker.id();
    ctx.lane(i)
        .call(move || crate::broker::lane_ref(id).map_or(0, |l| Rc::strong_count(&l) - 1))
        .await
        .unwrap()
}

/// Runs `f` with the state of lane `i` of `broker`.
async fn on_lane<T: Send + 'static>(
    ctx: &Context,
    broker: &Broker,
    i: usize,
    f: impl FnOnce(&mut Lane) -> T + Send + 'static,
) -> T {
    let id = broker.id();
    ctx.lane(i)
        .call(move || {
            let lane = crate::broker::lane_ref(id).expect("the broker runs on this lane");
            f(&mut lane.borrow_mut())
        })
        .await
        .unwrap()
}

/// Waits until no more than `n` hold lane `i`'s state, and returns how
/// many do.
async fn refs_settle(ctx: &Context, broker: &Broker, i: usize, n: usize) -> usize {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let now = lane_refs(ctx, broker, i).await;
        if now <= n || tokio::time::Instant::now() > deadline {
            return now;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn reconnects_leave_no_will_timers_behind() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let clients = Context::new().unwrap();
    let opts = ClientOptions {
        clean_start: false,
        session_expiry: 3600,
        will: Some(LastWill {
            topic: "wills/w".into(),
            payload: "x".into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 3600,
        }),
        ..ClientOptions::new("w")
    };
    let mut first = None;
    for _ in 0..6 {
        // Dropped without DISCONNECT, so the will waits for its delay until
        // the next connection takes the session.
        drop(Client::connect(&clients, &uri, opts.clone()).await.unwrap());
        stats_until(&broker, |s| s.connections == 0).await;
        let idle = *first.get_or_insert(lane_refs(&ctx, &broker, 0).await);
        assert_eq!(refs_settle(&ctx, &broker, 0, idle).await, idle);
    }
    let pending = on_lane(&ctx, &broker, 0, |l| l.sessions.len()).await;
    assert_eq!(pending, 1);
}

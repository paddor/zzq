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

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_that_shut_down_refuses_connections() {
    use zzq_proto::encode::ConnectFields;
    use zzq_proto::{Version, reason};

    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    // A connection that reached the lane before it shut down, such as one
    // whose CONNECT comes through a WebSocket, still holds its state.
    on_lane(&ctx, &broker, 0, Lane::shutdown).await;
    let c = ConnectFields {
        version: Version::V5,
        clean_start: true,
        keep_alive: 0,
        properties: &[],
        client_id: "late",
        will: None,
        username: None,
        password: None,
    };
    let (_raw, code) = support::Raw::connect_with(&uri, &c).await;
    assert_eq!(code, reason::SERVER_UNAVAILABLE);
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_do_not_expire_while_the_lane_closes_for_a_dump() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let clients = Context::new().unwrap();
    let opts = ClientOptions {
        clean_start: false,
        session_expiry: 1,
        ..ClientOptions::new("brief")
    };
    drop(Client::connect(&clients, &uri, opts).await.unwrap());
    stats_until(&broker, |s| s.connections == 0).await;
    on_lane(&ctx, &broker, 0, Lane::quiesce).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let left = on_lane(&ctx, &broker, 0, |l| {
        for _ in 0..crate::lane::SWEEP_PARTS {
            l.sweep(std::time::Instant::now());
        }
        l.sessions.len()
    })
    .await;
    assert_eq!(
        left, 1,
        "the dump would miss the session, or send its will twice"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kicked_connection_that_stopped_reading_lets_go() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use zzq_proto::encode::{self, ConnectFields};
    use zzq_proto::{SubscriptionOptions, Version};

    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let idle = lane_refs(&ctx, &broker, 0).await;
    // A subscriber that stops reading, with little room to receive.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut stuck = socket
        .connect(uri.trim_start_matches("tcp://").parse().unwrap())
        .await
        .unwrap();
    let mut out = Vec::new();
    let c = ConnectFields {
        version: Version::V5,
        clean_start: true,
        keep_alive: 0,
        properties: &[],
        client_id: "stuck",
        will: None,
        username: None,
        password: None,
    };
    encode::connect(&c, &mut out);
    let opts = SubscriptionOptions::from(QoS::AtMostOnce);
    encode::subscribe(Version::V5, 1, &[], &[("flood", opts)], &mut out);
    stuck.write_all(&out).await.unwrap();
    let mut acks = [0u8; 64];
    let _ = stuck.read(&mut acks).await.unwrap();
    stats_until(&broker, |s| s.subscriptions == 1).await;

    let clients = Context::new().unwrap();
    let publisher = Client::connect(&clients, &uri, ClientOptions::new("pub"))
        .await
        .unwrap();
    for _ in 0..20_000 {
        publisher
            .publish("flood", vec![7u8; 1024], QoS::AtMostOnce)
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    // A takeover kicks the stuck connection while its writer waits for room.
    let fresh = support::Raw::connect(&uri, "stuck", Version::V5, 0).await;
    drop(fresh);
    publisher.disconnect().await;
    stats_until(&broker, |s| s.connections == 0).await;
    assert_eq!(refs_settle(&ctx, &broker, 0, idle).await, idle);
    drop(stuck);
}

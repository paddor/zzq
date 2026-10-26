//! Regression tests that look inside the lanes: their tasks and state.

#[path = "../tests/support/mod.rs"]
pub(crate) mod support;

use std::rc::Rc;
use std::time::Duration;

use zzq_client::{Client, ClientOptions, LastWill};
use zzq_proto::QoS;

use crate::lane::Lane;
use crate::{Broker, BrokerOptions, Context};
use support::{WAIT, setup, stats_until};

/// Polls `f` on lane `i` until it returns `Some`. Fails after [`WAIT`].
async fn lane_until<T: Send + 'static>(
    ctx: &Context,
    broker: &Broker,
    i: usize,
    f: impl Fn(&mut Lane) -> Option<T> + Send + Sync + Clone + 'static,
) -> T {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        if let Some(t) = on_lane(ctx, broker, i, f.clone()).await {
            return t;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lane never got there"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

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
pub(crate) async fn on_lane<T: Send + 'static>(
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

#[tokio::test(flavor = "multi_thread")]
async fn a_held_puback_goes_to_the_connection_that_sent_the_publish() {
    use zzq_proto::encode::{self, ConnectFields, PublishHeader};
    use zzq_proto::property::SESSION_EXPIRY_INTERVAL;
    use zzq_proto::{PacketType, Property, SubscriptionOptions, Version};

    use support::{Got, Raw};

    let opts = BrokerOptions {
        max_inflight: 1,
        qos_queue_high: 2,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let mut sub = Raw::connect(&uri, "sub", Version::V5, 0).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    let props = [Property::U32(SESSION_EXPIRY_INTERVAL, 60)];
    let connect = |clean_start| ConnectFields {
        version: Version::V5,
        clean_start,
        keep_alive: 0,
        properties: &props,
        client_id: "pub",
        will: None,
        username: None,
        password: None,
    };
    let (mut first, _) = Raw::connect_with(&uri, &connect(true)).await;
    for pkid in 1..=3 {
        first.pub_("t", b"x", QoS::AtLeastOnce, pkid).await;
    }
    // The third makes the subscriber congested, so its ack waits.
    for pkid in 1..=2 {
        let got = first.next().await;
        assert!(
            matches!(got, Got::Ack { pkid: p, .. } if p == pkid),
            "{got:?}"
        );
    }
    first.quiet(100).await;
    first.reset();
    let (mut again, _) = Raw::connect_with(&uri, &connect(false)).await;
    let h = PublishHeader {
        version: Version::V5,
        dup: true,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "t",
        pkid: 3,
        properties: &[],
    };
    let mut resend = Vec::new();
    encode::publish(&h, b"x", &mut resend);
    again.send(&resend).await;
    again.quiet(100).await;
    // The subscriber drains: the held acks go out.
    for _ in 0..4 {
        let p = sub.publish().await;
        sub.ack(PacketType::PubAck, p.pkid).await;
    }
    let got = again.next().await;
    assert!(matches!(got, Got::Ack { pkid: 3, .. }), "{got:?}");
    again.quiet(300).await;
}

/// A subscriber that never acks and is congested from its third message,
/// and the options that make it so, with a Receive Maximum of 10.
async fn congested_subscriber() -> (Context, Broker, String, support::Raw) {
    use zzq_proto::{SubscriptionOptions, Version};

    let opts = BrokerOptions {
        max_inflight: 1,
        qos_queue_high: 2,
        receive_maximum: 10,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let mut sub = support::Raw::connect(&uri, "sub", Version::V5, 0).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    (ctx, broker, uri, sub)
}

#[tokio::test(flavor = "multi_thread")]
async fn held_acks_of_a_v311_publisher_stop_at_the_receive_maximum() {
    use zzq_proto::Version;

    let (_ctx, _broker, uri, _sub) = congested_subscriber().await;
    let mut publisher = support::Raw::connect(&uri, "pub", Version::V311, 0).await;
    for pkid in 1..=30 {
        publisher.pub_("t", b"x", QoS::AtLeastOnce, pkid).await;
    }
    // 1 and 2 before the congestion, then 10 held, then the rest at once.
    let mut acked = Vec::new();
    while let Some(Some(support::Got::Ack { pkid, .. })) =
        publisher.try_next_within(Duration::from_millis(300)).await
    {
        acked.push(pkid);
    }
    let want: Vec<u16> = [1, 2].into_iter().chain(13..=30).collect();
    assert_eq!(acked, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_v5_publisher_past_the_receive_maximum_is_disconnected() {
    use zzq_proto::{Version, reason};

    let (_ctx, _broker, uri, _sub) = congested_subscriber().await;
    let mut publisher = support::Raw::connect(&uri, "pub", Version::V5, 0).await;
    for pkid in 1..=13 {
        publisher.pub_("t", b"x", QoS::AtLeastOnce, pkid).await;
    }
    for pkid in 1..=2 {
        let got = publisher.next().await;
        assert!(
            matches!(got, support::Got::Ack { pkid: p, .. } if p == pkid),
            "{got:?}"
        );
    }
    publisher
        .disconnected(&[reason::RECEIVE_MAXIMUM_EXCEEDED])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_messages_on_subscribe_queue_past_the_limit_only_by_their_batch() {
    use zzq_proto::{SubscriptionOptions, Version};

    let opts = BrokerOptions {
        max_inflight: 1,
        max_queued: 10,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, uri) = setup(1, opts).await;
    for i in 0..50 {
        broker
            .publish_retained(&format!("r/{i}"), "x", QoS::AtLeastOnce)
            .unwrap();
    }
    stats_until(&broker, |s| s.retained == 50).await;
    let mut sub = support::Raw::connect(&uri, "sub", Version::V5, 0).await;
    // Never acked, and each SUBSCRIBE sends all 50 again.
    for _ in 0..4 {
        sub.subscribe("r/#", SubscriptionOptions::from(QoS::AtLeastOnce))
            .await;
    }
    // One in flight, then at most 10 + 50 queued: 39 dropped by the
    // second batch and 50 by each after it.
    let stats = stats_until(&broker, |s| s.dropped.queue_full >= 139).await;
    assert_eq!(stats.dropped.queue_full, 139);
}

/// A client ID that `lane_for` puts on `lane` of `lanes`.
fn id_on(prefix: &str, lane: usize, lanes: usize) -> String {
    (0..u32::MAX)
        .map(|i| format!("{prefix}{i}"))
        .find(|id| crate::lane_for(id, lanes) == lane)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shared_message_for_an_offline_member_on_another_lane_is_copied() {
    use zzq_proto::encode::ConnectFields;
    use zzq_proto::property::SESSION_EXPIRY_INTERVAL;
    use zzq_proto::{Property, SubscriptionOptions, Version};

    let (ctx, broker, uri) = setup(2, BrokerOptions::default()).await;
    let member = id_on("member", 1, 2);
    let props = [Property::U32(SESSION_EXPIRY_INTERVAL, 60)];
    let c = ConnectFields {
        version: Version::V5,
        clean_start: true,
        keep_alive: 0,
        properties: &props,
        client_id: &member,
        will: None,
        username: None,
        password: None,
    };
    let (mut m, _) = support::Raw::connect_with(&uri, &c).await;
    m.subscribe("$share/g/t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    m.reset();
    stats_until(&broker, |s| s.connections == 0).await;
    let mut publisher = support::Raw::connect(&uri, &id_on("pub", 0, 2), Version::V5, 0).await;
    publisher.pub_("t", &[1; 100], QoS::AtLeastOnce, 1).await;
    let _ = publisher.next().await;
    // Queued for an offline session, it must not pin the read buffer of
    // the publisher on lane 0.
    let room = lane_until(&ctx, &broker, 1, |l| {
        let s = l.sessions.keys().find_map(|k| l.sessions.get(k))?;
        (s.queued() == 1).then(|| s.arena_room())
    })
    .await;
    assert!(room > 0, "the message was not copied");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_long_control_queue_lets_other_tasks_run() {
    let (ctx, broker, _uri) = setup(1, BrokerOptions::default()).await;
    let id = broker.id();
    let seen = on_lane(&ctx, &broker, 0, move |l| {
        for _ in 0..1000 {
            let msg = crate::msg::Msg::new("t", b"x", &[], QoS::AtMostOnce, false);
            let _ = l.shared.lanes[0]
                .control
                .push(crate::lane::Control::Publish(msg));
        }
        // Queued behind the control loop, which the pushes woke.
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::task::spawn_local(async move {
            let lane = crate::broker::lane_ref(id).unwrap();
            let _ = tx.send(lane.borrow().stats.publishes);
        });
        rx
    })
    .await;
    let seen = seen.await.unwrap();
    assert!(
        seen < 1000,
        "the control loop ran {seen} controls without a break"
    );
}

/// A v5 CONNECT for `id` that keeps its session, with a Receive Maximum
/// and, if not zero, a Maximum Packet Size.
fn keeping_connect(id: &str, clean_start: bool, receive: u16, max_packet: u32) -> Vec<u8> {
    use zzq_proto::encode::{self, ConnectFields};
    use zzq_proto::property::{MAXIMUM_PACKET_SIZE, RECEIVE_MAXIMUM, SESSION_EXPIRY_INTERVAL};
    use zzq_proto::{Property, Version};

    let mut props = vec![
        Property::U32(SESSION_EXPIRY_INTERVAL, 60),
        Property::U16(RECEIVE_MAXIMUM, receive),
    ];
    if max_packet > 0 {
        props.push(Property::U32(MAXIMUM_PACKET_SIZE, max_packet));
    }
    let c = ConnectFields {
        version: Version::V5,
        clean_start,
        keep_alive: 0,
        properties: &props,
        client_id: id,
        will: None,
        username: None,
        password: None,
    };
    let mut out = Vec::new();
    encode::connect(&c, &mut out);
    out
}

async fn raw_with(uri: &str, connect: &[u8]) -> support::Raw {
    let mut raw = support::Raw::open(uri).await;
    raw.send(connect).await;
    let got = raw.next().await;
    assert!(
        matches!(got, support::Got::ConnAck { code: 0, .. }),
        "{got:?}"
    );
    raw
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unacked_message_too_large_after_a_reconnect_frees_its_slot() {
    use zzq_proto::SubscriptionOptions;

    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, uri) = setup(1, opts).await;
    let mut sub = raw_with(&uri, &keeping_connect("sub", true, 1, 0)).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    broker
        .publish("t", vec![0u8; 500], QoS::AtLeastOnce)
        .unwrap();
    assert_eq!(sub.publish().await.payload.len(), 500);
    sub.reset();
    stats_until(&broker, |s| s.connections == 0).await;
    // Back with room for small messages only: the unacked one cannot be
    // resent and must not keep the only window slot.
    let mut sub = raw_with(&uri, &keeping_connect("sub", false, 1, 100)).await;
    broker.publish("t", "ok", QoS::AtLeastOnce).unwrap();
    assert_eq!(sub.publish().await.payload, b"ok");
    let stats = stats_until(&broker, |s| s.dropped.too_large == 1).await;
    assert_eq!(stats.dropped.too_large, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_messages_sent_or_dropped_on_reconnect_are_counted() {
    use zzq_proto::SubscriptionOptions;

    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, uri) = setup(1, opts).await;
    let mut sub = raw_with(&uri, &keeping_connect("sub", true, 10, 0)).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    sub.reset();
    stats_until(&broker, |s| s.connections == 0).await;
    broker
        .publish("t", vec![0u8; 500], QoS::AtLeastOnce)
        .unwrap();
    for _ in 0..3 {
        broker.publish("t", "ok", QoS::AtLeastOnce).unwrap();
    }
    stats_until(&broker, |s| s.publishes == 4).await;
    let mut sub = raw_with(&uri, &keeping_connect("sub", false, 10, 100)).await;
    for _ in 0..3 {
        assert_eq!(sub.publish().await.payload, b"ok");
    }
    let stats = stats_until(&broker, |s| s.deliveries == 3 && s.dropped.too_large == 1).await;
    assert_eq!((stats.deliveries, stats.dropped.too_large), (3, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn broker_publishes_to_one_topic_keep_their_order() {
    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, uri) = setup(4, opts).await;
    let clients = Context::new().unwrap();
    let mut sub = Client::connect(&clients, &uri, ClientOptions::new("sub"))
        .await
        .unwrap();
    sub.subscribe("seq", QoS::AtLeastOnce).await.unwrap();
    for i in 0..1000u32 {
        broker
            .publish("seq", i.to_be_bytes(), QoS::AtLeastOnce)
            .unwrap();
    }
    for want in 0..1000u32 {
        let m = support::next(&mut sub).await;
        let got = u32::from_be_bytes(m.payload[..].try_into().unwrap());
        assert_eq!(got, want);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_returns_once_connections_said_goodbye() {
    use zzq_proto::{SubscriptionOptions, Version, reason};

    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(2, opts).await;
    let mut readers = Vec::new();
    for i in 0..20 {
        let mut raw = support::Raw::connect(&uri, &format!("c{i}"), Version::V5, 0).await;
        raw.subscribe("t", SubscriptionOptions::from(QoS::AtMostOnce))
            .await;
        readers.push(raw);
    }
    for _ in 0..200 {
        broker
            .publish("t", vec![0u8; 4096], QoS::AtMostOnce)
            .unwrap();
    }
    stats_until(&broker, |s| s.deliveries == 20 * 200).await;
    let readers: Vec<_> = readers
        .into_iter()
        .map(|mut raw| {
            tokio::spawn(async move {
                loop {
                    match raw.try_next().await {
                        Some(support::Got::Publish(_)) => {}
                        Some(support::Got::Disconnect { reason }) => return Some(reason),
                        _ => return None,
                    }
                }
            })
        })
        .collect();
    broker.shutdown().await.unwrap();
    // The lanes stop with the context: whatever they have not written yet
    // is lost.
    drop(ctx);
    for r in readers {
        assert_eq!(r.await.unwrap(), Some(reason::SERVER_SHUTTING_DOWN));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn options_that_cannot_work_are_refused() {
    let ctx = Context::new().unwrap();
    let cases = [
        BrokerOptions {
            sys_interval: Some(Duration::ZERO),
            ..BrokerOptions::default()
        },
        BrokerOptions {
            sn: crate::SnOptions {
                retry: Duration::ZERO,
                ..crate::SnOptions::default()
            },
            ..BrokerOptions::default()
        },
        BrokerOptions {
            receive_maximum: 0,
            ..BrokerOptions::default()
        },
    ];
    for opts in cases {
        let res = Broker::new(&ctx, opts).await;
        assert!(
            matches!(res, Err(crate::Error::InvalidOption(_))),
            "{res:?}"
        );
    }
}

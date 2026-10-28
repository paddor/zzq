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

    // Without $SYS, whose retained topics would make 61.
    let opts = BrokerOptions {
        max_inflight: 1,
        max_queued: 10,
        sys_interval: None,
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
        BrokerOptions {
            data_dir: Some("d".into()),
            state_file: Some("s".into()),
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

#[tokio::test(flavor = "multi_thread")]
async fn a_denial_with_a_success_code_still_refuses() {
    use std::sync::Arc;

    use zzq_proto::encode::ConnectFields;
    use zzq_proto::{ReasonCode, Version, reason};

    use crate::{AuthDecision, Authenticator, ConnectInfo};

    struct DenyWithSuccess;
    impl Authenticator for DenyWithSuccess {
        fn authenticate(&self, _: &ConnectInfo<'_>) -> AuthDecision {
            AuthDecision::Deny(ReasonCode::SUCCESS)
        }
    }
    let opts = BrokerOptions {
        authenticator: Some(Arc::new(DenyWithSuccess)),
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let c = ConnectFields {
        version: Version::V5,
        clean_start: true,
        keep_alive: 0,
        properties: &[],
        client_id: "x",
        will: None,
        username: None,
        password: None,
    };
    let (_raw, code) = support::Raw::connect_with(&uri, &c).await;
    assert_eq!(code, reason::NOT_AUTHORIZED);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_retained_write_nacks_v5_publishers_and_drops_v3_ones() {
    use zzq_proto::encode::{self, PublishHeader};
    use zzq_proto::{PacketType, Version, reason};

    use support::{Got, Raw};

    let dir = std::env::temp_dir().join(format!("zzq-failed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let opts = BrokerOptions {
        data_dir: Some(dir.clone()),
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, uri) = setup(1, opts).await;
    // The first three records of the topic's journal fail.
    broker.shared().retained.cursor_of("f/1").fail(1, 3);
    let publish = |version, qos, pkid, retain| {
        let h = PublishHeader {
            version,
            dup: false,
            qos,
            retain,
            topic: "f/1",
            pkid,
            properties: &[],
        };
        let mut out = Vec::new();
        encode::publish(&h, b"v", &mut out);
        out
    };
    let ack = |kind, pkid, reason| Got::Ack { kind, pkid, reason };
    let mut v5 = Raw::connect(&uri, "v5", Version::V5, 0).await;
    // The ack of the publish after it waits too, to keep their order.
    let both = [
        publish(Version::V5, QoS::AtLeastOnce, 1, true),
        publish(Version::V5, QoS::AtLeastOnce, 2, false),
    ];
    v5.send(&both.concat()).await;
    let failed = reason::UNSPECIFIED_ERROR;
    assert_eq!(v5.next().await, ack(PacketType::PubAck, 1, failed));
    assert_eq!(v5.next().await, ack(PacketType::PubAck, 2, 0));
    v5.send(&publish(Version::V5, QoS::ExactlyOnce, 3, true))
        .await;
    assert_eq!(v5.next().await, ack(PacketType::PubRec, 3, failed));
    // The failed PUBREC ended the flow.
    v5.ack(PacketType::PubRel, 3).await;
    let unknown = reason::PACKET_IDENTIFIER_NOT_FOUND;
    assert_eq!(v5.next().await, ack(PacketType::PubComp, 3, unknown));
    let mut v3 = Raw::connect(&uri, "v3", Version::V311, 0).await;
    v3.send(&publish(Version::V311, QoS::AtLeastOnce, 1, true))
        .await;
    v3.closed().await;
    // Later records are written.
    let mut ok = Raw::connect(&uri, "ok", Version::V5, 0).await;
    ok.send(&publish(Version::V5, QoS::AtLeastOnce, 1, true))
        .await;
    assert_eq!(ok.next().await, ack(PacketType::PubAck, 1, 0));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_subscription_write_disconnects() {
    use zzq_proto::encode::ConnectFields;
    use zzq_proto::{SubscriptionOptions, Version, reason};

    use support::{Got, Raw};

    let dir = std::env::temp_dir().join(format!("zzq-failed-sub-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let opts = BrokerOptions {
        data_dir: Some(dir.clone()),
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, uri) = setup(1, opts).await;
    // The lane journal fails its first records: the session, then the
    // subscription.
    broker.shared().lane_logs[0].cursor.fail(1, 2);
    let expiry = [zzq_proto::Property::U32(
        zzq_proto::property::SESSION_EXPIRY_INTERVAL,
        60,
    )];
    let connect = ConnectFields {
        version: Version::V5,
        clean_start: false,
        keep_alive: 0,
        properties: &expiry,
        client_id: "failing",
        will: None,
        username: None,
        password: None,
    };
    let (mut c, code) = Raw::connect_with(&uri, &connect).await;
    assert_eq!(code, 0);
    c.send(&{
        let mut out = Vec::new();
        let opts = SubscriptionOptions::from(QoS::AtLeastOnce);
        zzq_proto::encode::subscribe(Version::V5, 1, &[], &[("t", opts)], &mut out);
        out
    })
    .await;
    assert_eq!(
        c.next().await,
        Got::Disconnect {
            reason: reason::UNSPECIFIED_ERROR
        }
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Options of a broker on data directory `name`, and CONNECT fields for
/// a session that outlives its connection.
fn on_disk(name: &str) -> (std::path::PathBuf, impl Fn() -> BrokerOptions) {
    let dir = std::env::temp_dir().join(format!("zzq-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let opts = {
        let dir = dir.clone();
        move || BrokerOptions {
            data_dir: Some(dir.clone()),
            sys_interval: None,
            ..BrokerOptions::default()
        }
    };
    (dir, opts)
}

const EXPIRY: [zzq_proto::Property<'static>; 1] = [zzq_proto::Property::U32(
    zzq_proto::property::SESSION_EXPIRY_INTERVAL,
    600,
)];

fn durable(client_id: &str) -> zzq_proto::encode::ConnectFields<'_> {
    zzq_proto::encode::ConnectFields {
        version: zzq_proto::Version::V5,
        clean_start: false,
        keep_alive: 0,
        properties: &EXPIRY,
        client_id,
        will: None,
        username: None,
        password: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_qos2_publish_repeated_after_a_restart_is_delivered_once() {
    use zzq_proto::encode::{self, PublishHeader};
    use zzq_proto::{PacketType, SubscriptionOptions, Version};

    use support::{Got, Raw};

    let (dir, opts) = on_disk("qos2-in");
    let publish = |dup| {
        let h = PublishHeader {
            version: Version::V5,
            dup,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "q2/t",
            pkid: 5,
            properties: &[],
        };
        let mut out = Vec::new();
        encode::publish(&h, b"once", &mut out);
        out
    };
    let ack = |kind, pkid| Got::Ack {
        kind,
        pkid,
        reason: 0,
    };
    {
        let (_ctx, _broker, uri) = setup(1, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable("q2-sub")).await;
        sub.subscribe("q2/t", SubscriptionOptions::from(QoS::ExactlyOnce))
            .await;
        drop(sub);
        let (mut p, _) = Raw::connect_with(&uri, &durable("q2-pub")).await;
        p.send(&publish(false)).await;
        // The PUBREC means the ID is on disk: a repeat is no new message.
        assert_eq!(p.next().await, ack(PacketType::PubRec, 5));
    }
    let (_ctx, _broker, uri) = setup(1, opts()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("q2-pub")).await;
    p.send(&publish(true)).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 5));
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp, 5));
    let (mut sub, _) = Raw::connect_with(&uri, &durable("q2-sub")).await;
    let m = sub.publish().await;
    assert_eq!(m.payload, b"once");
    sub.complete(&m).await;
    sub.quiet(300).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_released_delivery_gets_its_pubrel_after_a_restart() {
    use zzq_proto::{PacketType, SubscriptionOptions};

    use support::{Got, Raw};

    let (dir, opts) = on_disk("qos2-out");
    let pkid = {
        let (_ctx, broker, uri) = setup(1, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable("rel-sub")).await;
        sub.subscribe("r/t", SubscriptionOptions::from(QoS::ExactlyOnce))
            .await;
        broker.publish("r/t", "x", QoS::ExactlyOnce).unwrap();
        let m = sub.publish().await;
        sub.ack(PacketType::PubRec, m.pkid).await;
        // PUBREL only once the journal has the PUBREC.
        let rel = Got::Ack {
            kind: PacketType::PubRel,
            pkid: m.pkid,
            reason: 0,
        };
        assert_eq!(sub.next().await, rel);
        m.pkid
    };
    let (_ctx, _broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("rel-sub")).await;
    // The client has the message: PUBREL again, not the PUBLISH.
    let rel = Got::Ack {
        kind: PacketType::PubRel,
        pkid,
        reason: 0,
    };
    assert_eq!(sub.next().await, rel);
    sub.ack(PacketType::PubComp, pkid).await;
    sub.quiet(300).await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_snapshot_replaces_the_segments_before_it() {
    use zzq_proto::SubscriptionOptions;

    use support::Raw;

    let (dir, opts) = on_disk("lane-snapshot");
    {
        let (ctx, broker, uri) = setup(1, opts()).await;
        let (mut c, _) = Raw::connect_with(&uri, &durable("snap")).await;
        c.subscribe("a", SubscriptionOptions::from(QoS::AtLeastOnce))
            .await;
        // The first try asks for a new segment; a later one rolls into it.
        lane_until(&ctx, &broker, 0, |l| l.snapshot_journal(true).then_some(())).await;
        c.subscribe("b", SubscriptionOptions::from(QoS::AtLeastOnce))
            .await;
        let first = dir.join("lanes-1/lane-0/0000000000000001.seg");
        let deadline = tokio::time::Instant::now() + WAIT;
        while first.exists() {
            assert!(tokio::time::Instant::now() < deadline, "segment 1 stays");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(c);
        broker.shutdown().await.unwrap();
    }
    let (ctx, broker, _uri) = setup(1, opts()).await;
    let subs = on_lane(&ctx, &broker, 0, |l| {
        let key = l.sessions.keys().next()?;
        let mut f: Vec<String> = l
            .sessions
            .get(key)?
            .subs
            .keys()
            .map(ToString::to_string)
            .collect();
        f.sort();
        Some(f)
    })
    .await;
    assert_eq!(subs, Some(vec!["a".to_owned(), "b".to_owned()]));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The inode of `path`.
fn ino(path: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata(path).unwrap().ino()
}

/// Waits until `path` is gone.
async fn gone(path: &std::path::Path) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while path.exists() {
        assert!(tokio::time::Instant::now() < deadline, "{path:?} stays");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_journal_without_room_for_a_segment_snapshots_into_its_spare() {
    use zzq_proto::SubscriptionOptions;

    use crate::durable::JournalId;
    use support::Raw;

    let (dir, opts) = on_disk("lane-full");
    let file = |name: &str| dir.join("lanes-1/lane-0").join(name);
    {
        let (ctx, broker, uri) = setup(1, opts()).await;
        let (mut c, _) = Raw::connect_with(&uri, &durable("full")).await;
        c.subscribe("a", SubscriptionOptions::from(QoS::AtLeastOnce))
            .await;
        let reserve = ino(&file("0000000000000000.spare"));
        // The segment the snapshot asks for fails as on a full device, so
        // it starts in the spare kept back.
        broker.shared().fail_next_segment(JournalId::Lane(0));
        lane_until(&ctx, &broker, 0, |l| l.snapshot_journal(true).then_some(())).await;
        gone(&file("0000000000000001.seg")).await;
        assert_eq!(ino(&file("0000000000000002.seg")), reserve);
        let stats = broker.stats().await.unwrap();
        assert!(stats.write_errors >= 1);
        // The spare kept back shows no room on the device.
        assert_eq!(stats.full_journals, 1);
        c.subscribe("b", SubscriptionOptions::from(QoS::AtLeastOnce))
            .await;
        drop(c);
        broker.shutdown().await.unwrap();
    }
    let (ctx, broker, _uri) = setup(1, opts()).await;
    let subs = on_lane(&ctx, &broker, 0, |l| {
        let key = l.sessions.keys().next()?;
        let mut f: Vec<String> = l
            .sessions
            .get(key)?
            .subs
            .keys()
            .map(ToString::to_string)
            .collect();
        f.sort();
        Some(f)
    })
    .await;
    assert_eq!(subs, Some(vec!["a".to_owned(), "b".to_owned()]));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retained_journal_without_room_for_a_segment_snapshots_into_its_spare() {
    use zzq_proto::{PacketType, Version};

    use crate::durable::JournalId;
    use support::{Got, Raw};

    let (dir, opts) = on_disk("retained-full");
    let stripe = crate::retain::Retained::stripe_of("r/full");
    let file = |name: &str| dir.join(format!("retained-{stripe}")).join(name);
    let ack = |pkid| Got::Ack {
        kind: PacketType::PubAck,
        pkid,
        reason: 0,
    };
    {
        let (_ctx, broker, uri) = setup(1, opts()).await;
        let mut p = Raw::connect(&uri, "full-pub", Version::V5, 0).await;
        let publish = |pkid, payload: &[u8]| {
            let h = zzq_proto::encode::PublishHeader {
                version: Version::V5,
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: true,
                topic: "r/full",
                pkid,
                properties: &[],
            };
            let mut out = Vec::new();
            zzq_proto::encode::publish(&h, payload, &mut out);
            out
        };
        p.send(&publish(1, b"first")).await;
        assert_eq!(p.next().await, ack(1));
        let reserve = ino(&file("0000000000000000.spare"));
        broker
            .shared()
            .fail_next_segment(JournalId::Retained(stripe));
        let deadline = tokio::time::Instant::now() + WAIT;
        while !broker.shared().retained.snapshot(true) {
            assert!(tokio::time::Instant::now() < deadline, "no snapshot");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        gone(&file("0000000000000001.seg")).await;
        assert_eq!(ino(&file("0000000000000002.seg")), reserve);
        assert_eq!(broker.stats().await.unwrap().full_journals, 1);
        p.send(&publish(2, b"second")).await;
        assert_eq!(p.next().await, ack(2));
    }
    let (_ctx, broker, _uri) = setup(1, opts()).await;
    let got = broker.shared().retained.matching("r/full");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload(), b"second");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_dir_short_of_room_stops_growth_not_traffic() {
    use zzq_proto::ReasonCode;

    let (dir, opts) = on_disk("room");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let durable = |id: &str| ClientOptions {
        clean_start: false,
        session_expiry: 60,
        ..ClientOptions::new(id)
    };
    let d = Client::connect(&ctx, &uri, durable("d")).await.unwrap();
    d.subscribe("q", QoS::AtLeastOnce).await.unwrap();
    d.disconnect().await;
    // No room for another copy of the state: it stops growing.
    broker.shared().set_free_bytes(Some(0));
    lane_until(&ctx, &broker, 0, |l| l.budget.full.then_some(())).await;
    let refused = Client::connect(&ctx, &uri, durable("e")).await;
    assert!(
        matches!(
            refused,
            Err(zzq_client::Error::Refused(ReasonCode::QUOTA_EXCEEDED))
        ),
        "{refused:?}"
    );
    let mut live = Client::connect(&ctx, &uri, ClientOptions::new("live"))
        .await
        .unwrap();
    live.subscribe("live", QoS::AtLeastOnce).await.unwrap();
    live.publish("live", "x", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(support::next(&mut live).await.payload, "x");
    // A message dropped for the full state costs the journal nothing.
    let journaled = broker.shared().lane_logs[0].counts().0;
    live.publish("q", "dropped", QoS::AtLeastOnce)
        .await
        .unwrap();
    assert_eq!(broker.shared().lane_logs[0].counts().0, journaled);
    live.publish_retained("live", "kept?", QoS::AtLeastOnce)
        .await
        .unwrap();
    assert_eq!(support::next(&mut live).await.payload, "kept?");
    stats_until(&broker, |s| {
        s.dropped.state_full == 1 && (s.retained, s.retained_rejected) == (0, 1)
    })
    .await;
    // Room again: the state grows again.
    broker.shared().set_free_bytes(None);
    lane_until(&ctx, &broker, 0, |l| (!l.budget.full).then_some(())).await;
    live.publish("q", "kept", QoS::AtLeastOnce).await.unwrap();
    let mut d = Client::connect(&ctx, &uri, durable("d")).await.unwrap();
    assert!(d.connected().session_present);
    assert_eq!(support::next(&mut d).await.payload, "kept");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_lane_write_nacks_publishers_on_other_lanes() {
    use zzq_proto::{PacketType, SubscriptionOptions, Version, reason};

    use crate::durable::JournalId;
    use support::{Got, Raw};

    let (dir, opts) = on_disk("lane-fail");
    let sub_id = id_on("fail-sub", 1, 2);
    let ack = |pkid, reason| Got::Ack {
        kind: PacketType::PubAck,
        pkid,
        reason,
    };
    {
        let (_ctx, broker, uri) = setup(2, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable(&sub_id)).await;
        // Once acked, the lane has nothing more to write.
        sub.subscribe("f/t", SubscriptionOptions::from(QoS::AtLeastOnce))
            .await;
        let mut p = Raw::connect(&uri, &id_on("fail-pub", 0, 2), Version::V5, 0).await;
        broker.shared().fail_next_write(JournalId::Lane(1));
        p.pub_("f/t", b"lost", QoS::AtLeastOnce, 1).await;
        assert_eq!(p.next().await, ack(1, reason::UNSPECIFIED_ERROR));
        p.pub_("f/t", b"kept", QoS::AtLeastOnce, 2).await;
        assert_eq!(p.next().await, ack(2, 0));
        assert!(broker.stats().await.unwrap().write_errors >= 1);
    }
    // The lane wrote its sessions again after the failure.
    let (_ctx, _broker, uri) = setup(2, opts()).await;
    let (mut sub, got) = Raw::connack_with(&uri, &durable(&sub_id)).await;
    assert!(
        matches!(
            got,
            Got::ConnAck {
                session_present: true,
                ..
            }
        ),
        "{got:?}"
    );
    while sub.publish().await.payload != b"kept" {}
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_retained_write_rewrites_the_stripe() {
    use zzq_proto::{PacketType, Version, reason};

    use crate::durable::JournalId;
    use support::{Got, Raw};

    let (dir, opts) = on_disk("stripe-fail");
    let publish = |pkid, payload: &[u8]| {
        let h = zzq_proto::encode::PublishHeader {
            version: Version::V5,
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: true,
            topic: "r/fail",
            pkid,
            properties: &[],
        };
        let mut out = Vec::new();
        zzq_proto::encode::publish(&h, payload, &mut out);
        out
    };
    let ack = |pkid, reason| Got::Ack {
        kind: PacketType::PubAck,
        pkid,
        reason,
    };
    {
        let (_ctx, broker, uri) = setup(1, opts()).await;
        let stripe = crate::retain::Retained::stripe_of("r/fail");
        broker.shared().fail_next_write(JournalId::Retained(stripe));
        let mut p = Raw::connect(&uri, "stripe-pub", Version::V5, 0).await;
        p.send(&publish(1, b"first")).await;
        assert_eq!(p.next().await, ack(1, reason::UNSPECIFIED_ERROR));
        p.send(&publish(2, b"second")).await;
        assert_eq!(p.next().await, ack(2, 0));
    }
    let (_ctx, broker, _uri) = setup(1, opts()).await;
    let got = broker.shared().retained.matching("r/fail");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].payload(), b"second");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_burst_to_another_lane_is_acked_once_journaled() {
    use zzq_proto::{PacketType, SubscriptionOptions, Version};

    use support::{Got, Raw};

    let (dir, opts) = on_disk("burst");
    let (_ctx, _broker, uri) = setup(2, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable(&id_on("burst-sub", 1, 2))).await;
    sub.subscribe("b/t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    drop(sub);
    let mut p = Raw::connect(&uri, &id_on("burst-pub", 0, 2), Version::V5, 0).await;
    let burst: Vec<u8> = (1..=200)
        .flat_map(|i| support::publish_bytes(Version::V5, "b/t", b"x", QoS::AtLeastOnce, i, &[]))
        .collect();
    p.send(&burst).await;
    for i in 1..=200 {
        let got = p.next().await;
        assert_eq!(
            got,
            Got::Ack {
                kind: PacketType::PubAck,
                pkid: i,
                reason: 0
            }
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn only_journaled_subscribers_on_other_lanes_hold_the_ack() {
    use zzq_proto::{PacketType, SubscriptionOptions, Version};

    use support::{Got, Raw};

    let (dir, opts) = on_disk("cross-hold");
    let (ctx, _broker, uri) = setup(2, opts()).await;
    let qos1 = SubscriptionOptions::from(QoS::AtLeastOnce);
    let mut plain = Raw::connect(&uri, &id_on("plain-sub", 1, 2), Version::V5, 0).await;
    plain.subscribe("plain/t", qos1).await;
    let (mut kept, _) = Raw::connect_with(&uri, &durable(&id_on("kept-sub", 1, 2))).await;
    kept.subscribe("kept/t", qos1).await;
    let mut p = Raw::connect(&uri, &id_on("cross-pub", 0, 2), Version::V5, 0).await;
    // Lane 1 stops until `go`.
    let (stopped, on_stop) = tokio::sync::oneshot::channel();
    let (go, on_go) = std::sync::mpsc::channel::<()>();
    let lanes = ctx.clone();
    let stall = tokio::spawn(async move {
        let lane = lanes.lane(1);
        lane.call(move || {
            let _ = stopped.send(());
            let _ = on_go.recv_timeout(WAIT);
        })
        .await
    });
    on_stop.await.unwrap();
    let ack = |pkid| Got::Ack {
        kind: PacketType::PubAck,
        pkid,
        reason: 0,
    };
    p.pub_("plain/t", b"x", QoS::AtLeastOnce, 1).await;
    assert_eq!(p.next().await, ack(1));
    p.pub_("kept/t", b"x", QoS::AtLeastOnce, 2).await;
    p.quiet(300).await;
    go.send(()).unwrap();
    stall.await.unwrap().unwrap();
    assert_eq!(p.next().await, ack(2));
    assert_eq!(plain.publish().await.payload, b"x");
    assert_eq!(kept.publish().await.payload, b"x");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn acked_deliveries_go_out_with_later_writes() {
    use zzq_proto::{PacketType, SubscriptionOptions, Version};

    use support::{Got, Raw};

    let (dir, opts) = on_disk("done-along");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("along-sub")).await;
    sub.subscribe("a/t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    let mut p = Raw::connect(&uri, "along-pub", Version::V5, 0).await;
    let log = &broker.shared().lane_logs[0];
    let (_, before) = log.counts();
    for pkid in 1..=20 {
        p.pub_("a/t", b"x", QoS::AtLeastOnce, pkid).await;
        let ack = Got::Ack {
            kind: PacketType::PubAck,
            pkid,
            reason: 0,
        };
        assert_eq!(p.next().await, ack);
        let m = sub.publish().await;
        sub.ack(PacketType::PubAck, m.pkid).await;
    }
    // Each DONE went with the next publish's records, not alone.
    let writes = log.counts().1 - before;
    assert!(writes < 30, "{writes} writes for 20 publishes");
    // The last one goes out with a sweep, once the lane has it.
    lane_until(&ctx, &broker, 0, |l| {
        let keys: Vec<_> = l.sessions.keys().collect();
        keys.into_iter()
            .all(|k| l.sessions.get(k).is_none_or(|s| s.inflight().len() == 0))
            .then_some(())
    })
    .await;
    let deadline = tokio::time::Instant::now() + WAIT;
    while log.cursor.check(log.counts().0) != Some(true) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the last DONE stays"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn stored_topics_deeper_than_allowed_are_not_loaded() {
    let dir = std::env::temp_dir().join(format!("zzq-deep-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let opts = |max_topic_levels| BrokerOptions {
        data_dir: Some(dir.clone()),
        max_topic_levels,
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, _uri) = setup(1, opts(2000)).await;
    for topic in ["ok/1".to_owned(), "a/".repeat(1000) + "a"] {
        broker
            .publish_retained(&topic, "x", QoS::AtMostOnce)
            .unwrap();
    }
    broker.shutdown().await.unwrap();
    let (_ctx, broker, _uri) = setup(1, opts(128)).await;
    assert_eq!(broker.stats().await.unwrap().retained, 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idle_publisher_lets_go_of_stale_matches() {
    use zzq_proto::{SubscriptionOptions, Version};

    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut publisher = support::Raw::connect(&uri, "pub", Version::V5, 0).await;
    for _ in 0..2 {
        publisher.pub_("t", b"x", QoS::AtMostOnce, 0).await;
    }
    let held = |l: &mut Lane| {
        let key = l.sessions.keys().next()?;
        Some(l.sessions.get(key)?.inbound.last_topic.is_some())
    };
    assert!(lane_until(&ctx, &broker, 0, move |l| held(l).filter(|&h| h)).await);
    // A new subscription makes the cached matches stale.
    let mut sub = support::Raw::connect(&uri, "sub", Version::V5, 0).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtMostOnce))
        .await;
    let still = on_lane(&ctx, &broker, 0, move |l| {
        for _ in 0..crate::lane::SWEEP_PARTS {
            l.sweep(std::time::Instant::now());
        }
        let key = l
            .sessions
            .keys()
            .find(|&k| l.sessions.get(k).is_some_and(|s| &*s.client_id == "pub"))?;
        Some(l.sessions.get(key)?.inbound.last_topic.is_some())
    })
    .await;
    assert_eq!(still, Some(false));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publisher_held_back_by_the_broker_is_not_kicked_for_keep_alive() {
    use zzq_proto::{SubscriptionOptions, Version};

    let (ctx, _broker, uri) = setup(2, BrokerOptions::default()).await;
    let mut sub = support::Raw::connect(&uri, &id_on("sub", 1, 2), Version::V5, 0).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtMostOnce))
        .await;
    let mut publisher = support::Raw::connect(&uri, &id_on("pub", 0, 2), Version::V5, 1).await;
    // Lane 1 stops taking deliveries, so lane 0 fills its mailbox and the
    // publisher waits for room longer than its keep alive allows.
    ctx.lane(1)
        .run(|| std::thread::sleep(Duration::from_secs(3)))
        .unwrap();
    for _ in 0..100 {
        publisher.pub_("t", b"x", QoS::AtMostOnce, 0).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    tokio::time::sleep(Duration::from_millis(3500)).await;
    let mut ping = Vec::new();
    zzq_proto::encode::pingreq(&mut ping);
    publisher.send(&ping).await;
    assert_eq!(publisher.next().await, support::Got::PingResp);
}

/// An MQTT-SN client on a fresh socket that sent CONNECT and got CONNACK.
async fn sn_connect(addr: &str, id: &str, duration: u16) -> tokio::net::UdpSocket {
    use zzq_proto::sn::{self, Flags, Packet};

    let s = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    s.connect(addr).await.unwrap();
    let connect = Packet::Connect {
        flags: Flags {
            clean_session: true,
            ..Flags::default()
        },
        duration,
        client_id: id,
    };
    let mut out = Vec::new();
    sn::encode(&connect, &mut out);
    s.send(&out).await.unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(WAIT, s.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sn::decode(&buf[..n]).unwrap().0,
        Packet::ConnAck { code: sn::ACCEPTED }
    );
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn an_mqtt_sn_client_without_keep_alive_still_times_out() {
    let opts = BrokerOptions {
        sn: crate::SnOptions {
            max_keep_alive: 1,
            ..crate::SnOptions::default()
        },
        ..BrokerOptions::default()
    };
    let (_ctx, broker, _uri) = setup(1, opts).await;
    let addr = broker.bind("udp://127.0.0.1:0").await.unwrap().to_string();
    // Anyone can send this over UDP, and a client that never comes back
    // sends nothing that ends it.
    let _s = sn_connect(addr.trim_start_matches("udp://"), "silent", 0).await;
    stats_until(&broker, |s| s.connections == 1).await;
    stats_until(&broker, |s| s.connections == 0).await;
}

/// Sends `p` to the gateway.
async fn sn_send(s: &tokio::net::UdpSocket, p: &zzq_proto::sn::Packet<'_>) {
    let mut out = Vec::new();
    zzq_proto::sn::encode(p, &mut out);
    s.send(&out).await.unwrap();
}

/// The next datagram, or `None` within `ms` milliseconds.
async fn sn_recv(s: &tokio::net::UdpSocket, ms: u64) -> Option<Vec<u8>> {
    let mut buf = vec![0u8; 2048];
    let n = tokio::time::timeout(Duration::from_millis(ms), s.recv(&mut buf))
        .await
        .ok()?
        .unwrap();
    buf.truncate(n);
    Some(buf)
}

/// An MQTT-SN client subscribed to `topic` at QoS 0, and the broker.
async fn sn_subscriber(topic: &str) -> (Context, Broker, tokio::net::UdpSocket) {
    use zzq_proto::sn::{Filter, Flags, Packet};

    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (ctx, broker, _uri) = setup(1, opts).await;
    let addr = broker.bind("udp://127.0.0.1:0").await.unwrap().to_string();
    let s = sn_connect(addr.trim_start_matches("udp://"), "sn", 30).await;
    let sub = Packet::Subscribe {
        flags: Flags::default(),
        msg_id: 1,
        topic: Filter::Name(topic),
    };
    sn_send(&s, &sub).await;
    assert!(sn_recv(&s, 5000).await.is_some(), "no SUBACK");
    (ctx, broker, s)
}

/// The REGISTER that the next datagram must be: topic ID and message ID.
fn register_of(d: &[u8]) -> (u16, u16) {
    match zzq_proto::sn::decode(d).unwrap().0 {
        zzq_proto::sn::Packet::Register {
            topic_id, msg_id, ..
        } => (topic_id, msg_id),
        p => panic!("expected REGISTER, got {p:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_register_drops_what_waited_for_it() {
    use zzq_proto::sn::{self, Packet};

    let (_ctx, broker, s) = sn_subscriber("plant/#").await;
    broker
        .publish("plant/valve/1", "a", QoS::AtMostOnce)
        .unwrap();
    let (topic_id, msg_id) = register_of(&sn_recv(&s, 5000).await.unwrap());
    let refuse = Packet::RegAck {
        topic_id,
        msg_id,
        code: sn::REJECTED_INVALID_TOPIC_ID,
    };
    sn_send(&s, &refuse).await;
    assert!(
        sn_recv(&s, 300).await.is_none(),
        "a publish under a refused topic ID"
    );
    // The next one asks again.
    broker
        .publish("plant/valve/1", "b", QoS::AtMostOnce)
        .unwrap();
    register_of(&sn_recv(&s, 5000).await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_waiting_for_a_register_are_bounded() {
    use zzq_proto::sn::{self, Packet};

    let (_ctx, broker, s) = sn_subscriber("plant/#").await;
    for i in 0..1000u32 {
        broker
            .publish("plant/valve/1", i.to_be_bytes(), QoS::AtMostOnce)
            .unwrap();
    }
    let (topic_id, msg_id) = register_of(&sn_recv(&s, 5000).await.unwrap());
    tokio::time::sleep(Duration::from_millis(200)).await;
    let accept = Packet::RegAck {
        topic_id,
        msg_id,
        code: sn::ACCEPTED,
    };
    sn_send(&s, &accept).await;
    let mut got = 0;
    while sn_recv(&s, 300).await.is_some() {
        got += 1;
    }
    assert!(
        got > 0 && got <= 256,
        "{got} publishes held for one REGISTER"
    );
}

/// The MQTT-SN client tasks stop with their gateway, and so do their
/// broker connections.
#[tokio::test(flavor = "multi_thread")]
async fn mqtt_sn_clients_stop_with_their_gateway() {
    use tokio::net::UdpSocket;
    use zzq_proto::sn::{self, Flags, Packet};

    let (_ctx, broker, _uri) = setup(1, BrokerOptions::default()).await;
    let addr = broker
        .bind("udp://127.0.0.1:0")
        .await
        .unwrap()
        .socket_addr_string()
        .unwrap();
    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    s.connect(&addr).await.unwrap();
    let connect = Packet::Connect {
        flags: Flags {
            clean_session: true,
            ..Flags::default()
        },
        duration: 60,
        client_id: "sn",
    };
    let mut out = Vec::new();
    sn::encode(&connect, &mut out);
    s.send(&out).await.unwrap();
    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(WAIT, s.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sn::decode(&buf[..n]).unwrap().0,
        Packet::ConnAck { code: sn::ACCEPTED }
    );
    stats_until(&broker, |s| s.connections == 1).await;
    broker.abort_tasks();
    stats_until(&broker, |s| s.connections == 0).await;
}

/// A client that sends and never reads its replies stops being read once
/// they reach `max_out_bytes`, instead of growing them without bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_does_not_read_its_replies_is_not_read_either() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use zzq_proto::Version;

    let opts = BrokerOptions {
        max_out_bytes: 64 << 10,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let mut deaf = socket
        .connect(uri.trim_start_matches("tcp://").parse().unwrap())
        .await
        .unwrap();
    deaf.write_all(&support::connect_bytes(Version::V5, "deaf", &[]))
        .await
        .unwrap();
    let mut connack = [0u8; 64];
    let _ = deaf.read(&mut connack).await.unwrap();
    // 32 MiB of PINGREQ asks for 32 MiB of PINGRESP.
    let pings = [0xc0u8, 0x00].repeat(16 << 20);
    let _ = tokio::time::timeout(Duration::from_secs(2), deaf.write_all(&pings)).await;
    let waiting = on_lane(&ctx, &broker, 0, |l| {
        l.sessions
            .keys()
            .filter_map(|k| l.sessions.get(k))
            .map(|s| s.out.buf.len())
            .max()
    })
    .await;
    assert!(
        waiting.is_some_and(|n| n < 1 << 20),
        "replies waiting: {waiting:?}"
    );
}

/// Acks held for a congested subscriber do not pile up for publisher
/// connections that ended.
#[tokio::test(flavor = "multi_thread")]
async fn held_acks_of_ended_connections_do_not_pile_up() {
    use zzq_proto::{SubscriptionOptions, Version};

    let opts = BrokerOptions {
        max_inflight: 1,
        qos_queue_high: 2,
        receive_maximum: 8,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    // Never acks, so it stays congested.
    let mut sub = support::Raw::connect(&uri, "sub", Version::V5, 0).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    for round in 1..=40 {
        let mut p = support::Raw::connect(&uri, "pub", Version::V311, 0).await;
        for pkid in 1..=8 {
            p.pub_("t", b"x", QoS::AtLeastOnce, pkid).await;
        }
        stats_until(&broker, |s| s.publishes >= round * 8).await;
        drop(p);
        stats_until(&broker, |s| s.connections == 1).await;
    }
    let held = on_lane(&ctx, &broker, 0, |l| {
        l.sessions
            .keys()
            .filter_map(|k| l.sessions.get(k))
            .find(|s| &*s.client_id == "sub")
            .map(|s| s.backpressure.waiters.len())
    })
    .await;
    assert!(held.is_some_and(|n| n <= 32), "held acks: {held:?}");
}

/// A SUBACK held for a connection that was taken over meanwhile sends
/// neither the ack nor the retained messages of its subscriptions.
#[tokio::test(flavor = "multi_thread")]
async fn a_held_suback_of_a_replaced_connection_sends_no_retained() {
    use zzq_proto::Version;

    // Without the retained `$SYS/` topics, which the count would race with.
    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    broker
        .publish_retained("r/t", "kept", QoS::AtMostOnce)
        .unwrap();
    stats_until(&broker, |s| s.retained == 1).await;
    let _s = support::Raw::connect(&uri, "s", Version::V5, 0).await;
    let sent = on_lane(&ctx, &broker, 0, |l| {
        let key = l
            .sessions
            .keys()
            .find(|&k| l.sessions.get(k).is_some_and(|s| &*s.client_id == "s"))?;
        let conn = l.sessions.get(key)?.conn.as_ref()?.id;
        let held = crate::lane::Held {
            key,
            // An earlier connection of the session.
            conn: conn - 1,
            sync: Some(std::sync::Arc::default()),
            due: crate::durable::Due::Now,
            packet: Box::new([]),
            retained: vec![("r/#".into(), QoS::AtMostOnce, 0)].into(),
        };
        l.release(&held);
        Some(l.sessions.get(key)?.out.buf.len())
    })
    .await;
    assert_eq!(sent, Some(0), "retained sent to the new connection");
}

/// An accept loop that starts after the broker stopped its tasks, as a
/// cancelled bind's can, stops at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_bind_that_lands_after_the_stop_does_not_keep_accepting() {
    let (_ctx, broker, _uri) = setup(1, BrokerOptions::default()).await;
    broker.abort_tasks();
    let ep = broker.bind("tcp://127.0.0.1:0").await.unwrap();
    let addr = ep.to_string();
    let addr = addr.trim_start_matches("tcp://");
    let deadline = tokio::time::Instant::now() + WAIT;
    while tokio::net::TcpStream::connect(addr).await.is_ok() {
        assert!(tokio::time::Instant::now() < deadline, "still accepting");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A connection that waits with its lane to ship before its read loop
/// starts is not kicked for keep alive meanwhile.
#[tokio::test(flavor = "multi_thread")]
async fn a_connection_held_back_at_connect_is_not_kicked_for_keep_alive() {
    use zzq_proto::{SubscriptionOptions, Version};

    let (ctx, _broker, uri) = setup(2, BrokerOptions::default()).await;
    let mut sub = support::Raw::connect(&uri, &id_on("sub", 1, 2), Version::V5, 0).await;
    sub.subscribe("t", SubscriptionOptions::from(QoS::AtMostOnce))
        .await;
    let mut publisher = support::Raw::connect(&uri, &id_on("pub", 0, 2), Version::V5, 0).await;
    // Lane 1 stops taking deliveries, so lane 0 waits for room to ship.
    ctx.lane(1)
        .run(|| std::thread::sleep(Duration::from_secs(3)))
        .unwrap();
    for _ in 0..100 {
        publisher.pub_("t", b"x", QoS::AtMostOnce, 0).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    // A second publisher's batch waits behind the first in the lane's
    // outbox, so a new connection waits for it too.
    let mut second = support::Raw::connect(&uri, &id_on("pub2", 0, 2), Version::V5, 0).await;
    second.pub_("t", b"y", QoS::AtMostOnce, 0).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut late = support::Raw::connect(&uri, &id_on("late", 0, 2), Version::V5, 1).await;
    let mut ping = Vec::new();
    zzq_proto::encode::pingreq(&mut ping);
    late.send(&ping).await;
    assert_eq!(late.next().await, support::Got::PingResp);
}

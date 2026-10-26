//! Sessions, flow control and wills, from other brokers' test suites: QoS
//! state across reconnects, Receive Maximum both ways, takeover, and when
//! wills go out.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests fail loudly"
)]

mod support;

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use support::{
    Got, Pub, Raw, WAIT, client, connect_bytes, next, nothing, publish_bytes, setup, stats_until,
};
use zzq_broker::{
    AuthDecision, Authenticator, Authorizer, BrokerOptions, ClientInfo, ConnectInfo, ReasonCode,
};
use zzq_client::{Client, ClientOptions, LastWill};
use zzq_proto::encode::{ConnectFields, WillFields};
use zzq_proto::property::{
    MAXIMUM_PACKET_SIZE, RECEIVE_MAXIMUM, SESSION_EXPIRY_INTERVAL, WILL_DELAY_INTERVAL,
};
use zzq_proto::reason;
use zzq_proto::{PacketType, Property, QoS, Version};

/// A session that outlives its connection by a minute.
const KEEP: Property<'static> = Property::U32(SESSION_EXPIRY_INTERVAL, 60);

/// CONNECT fields that resume the v5 session `id`.
const fn resume<'a>(id: &'a str, properties: &'a [Property<'a>]) -> ConnectFields<'a> {
    ConnectFields {
        version: Version::V5,
        clean_start: false,
        keep_alive: 0,
        properties,
        client_id: id,
        will: None,
        username: None,
        password: None,
    }
}

/// Like [`resume`], but starting a new session.
const fn fresh<'a>(id: &'a str, properties: &'a [Property<'a>]) -> ConnectFields<'a> {
    ConnectFields {
        clean_start: true,
        ..resume(id, properties)
    }
}

/// Connects, and returns the connection and whether a session was present.
async fn connect(uri: &str, c: &ConnectFields<'_>) -> (Raw, bool) {
    match Raw::connack_with(uri, c).await {
        (
            raw,
            Got::ConnAck {
                session_present,
                code: 0,
                ..
            },
        ) => (raw, session_present),
        (_, got) => panic!("expected CONNACK 0, got {got:?}"),
    }
}

/// A will on `topic` with v5 will properties.
const fn will<'a>(topic: &'a str, properties: &'a [Property<'a>]) -> WillFields<'a> {
    WillFields {
        qos: QoS::AtLeastOnce,
        retain: false,
        properties,
        topic,
        payload: b"gone",
    }
}

fn num(p: &Pub) -> u32 {
    std::str::from_utf8(&p.payload).unwrap().parse().unwrap()
}

const fn ack(kind: PacketType, pkid: u16) -> Got {
    Got::Ack {
        kind,
        pkid,
        reason: 0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn broker_respects_the_client_receive_maximum() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let two = [Property::U16(RECEIVE_MAXIMUM, 2)];
    let (mut raw, _) = connect(&uri, &fresh("rm", &two)).await;
    raw.subscribe("rm/#", QoS::AtLeastOnce.into()).await;
    for i in 0..5 {
        publ.publish("rm/1", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    publ.publish("rm/0", "zero", QoS::AtMostOnce).await.unwrap();
    // Two QoS 1 deliveries in flight [MQTT-3.3.4-9]; QoS 0 is not held back.
    let mut window = VecDeque::new();
    let mut zero = false;
    for _ in 0..3 {
        let p = raw.publish().await;
        if p.qos == QoS::AtMostOnce {
            zero = true;
        } else {
            window.push_back(p);
        }
    }
    assert!(zero && window.len() == 2);
    raw.quiet(300).await;
    // Each PUBACK lets one more through, in order.
    for i in 2..5 {
        let oldest = window.pop_front().unwrap();
        raw.ack(PacketType::PubAck, oldest.pkid).await;
        let p = raw.publish().await;
        assert_eq!(num(&p), i);
        window.push_back(p);
        raw.quiet(100).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_resends_no_more_than_the_new_receive_maximum() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let ten = [KEEP, Property::U16(RECEIVE_MAXIMUM, 10)];
    let (mut raw, _) = connect(&uri, &fresh("shrink", &ten)).await;
    raw.subscribe("s", QoS::AtLeastOnce.into()).await;
    for i in 0..10 {
        publ.publish("s", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    let mut first = Vec::new();
    for _ in 0..10 {
        first.push(raw.publish().await);
    }
    drop(raw);
    stats_until(&broker, |s| s.connections == 1).await;
    let two = [KEEP, Property::U16(RECEIVE_MAXIMUM, 2)];
    let (mut raw, present) = connect(&uri, &resume("shrink", &two)).await;
    assert!(present);
    let mut window = VecDeque::new();
    for _ in 0..2 {
        window.push_back(raw.publish().await);
    }
    raw.quiet(300).await;
    let mut seen = Vec::new();
    while let Some(p) = window.pop_front() {
        let before = &first[seen.len()];
        assert_eq!(
            (p.dup, p.pkid, &p.payload),
            (true, before.pkid, &before.payload)
        );
        seen.push(num(&p));
        raw.ack(PacketType::PubAck, p.pkid).await;
        if seen.len() + window.len() < 10 {
            window.push_back(raw.publish().await);
        }
    }
    assert_eq!(seen, (0..10).collect::<Vec<_>>());
    raw.quiet(200).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unacked_deliveries_resume_with_dup_publish_or_pubrel() {
    let (ctx, broker, uri) = setup(3, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let keep = [KEEP];
    let (mut raw, _) = connect(&uri, &fresh("resume", &keep)).await;
    raw.subscribe("r/#", QoS::ExactlyOnce.into()).await;
    publ.publish("r/1", "one", QoS::AtLeastOnce).await.unwrap();
    publ.publish("r/2", "two", QoS::ExactlyOnce).await.unwrap();
    publ.publish("r/3", "three", QoS::ExactlyOnce)
        .await
        .unwrap();
    let a = raw.publish().await;
    let b = raw.publish().await;
    let c = raw.publish().await;
    raw.ack(PacketType::PubRec, c.pkid).await;
    assert_eq!(raw.next().await, ack(PacketType::PubRel, c.pkid));
    drop(raw);
    stats_until(&broker, |s| s.connections == 1).await;
    // The same packet IDs in the same order: PUBLISH with DUP before PUBREC,
    // PUBREL after it [MQTT-4.4.0-1].
    let (mut raw, present) = connect(&uri, &resume("resume", &keep)).await;
    assert!(present);
    for before in [&a, &b] {
        let again = raw.publish().await;
        assert_eq!(
            (again.pkid, again.dup, &again.payload),
            (before.pkid, true, &before.payload)
        );
    }
    assert_eq!(raw.next().await, ack(PacketType::PubRel, c.pkid));
    // A new delivery does not take an ID still in flight.
    publ.publish("r/4", "four", QoS::ExactlyOnce).await.unwrap();
    let d = raw.publish().await;
    assert!(![a.pkid, b.pkid, c.pkid].contains(&d.pkid));
    raw.ack(PacketType::PubAck, a.pkid).await;
    for p in [&b, &d] {
        raw.ack(PacketType::PubRec, p.pkid).await;
        assert_eq!(raw.next().await, ack(PacketType::PubRel, p.pkid));
        raw.ack(PacketType::PubComp, p.pkid).await;
    }
    raw.ack(PacketType::PubComp, c.pkid).await;
    raw.disconnect(0, &[]).await;
    raw.closed().await;
    // Nothing is owed any more.
    let (mut raw, present) = connect(&uri, &resume("resume", &keep)).await;
    assert!(present);
    raw.quiet(300).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn inbound_qos2_state_survives_reconnect() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut sub = client(&ctx, &uri, "sub", Version::V5).await;
    sub.subscribe("in2", QoS::ExactlyOnce).await.unwrap();
    let keep = [KEEP];
    let (mut raw, _) = connect(&uri, &fresh("pub2", &keep)).await;
    raw.pub_("in2", b"once", QoS::ExactlyOnce, 7).await;
    assert_eq!(raw.next().await, ack(PacketType::PubRec, 7));
    drop(raw);
    stats_until(&broker, |s| s.connections == 1).await;
    let (mut raw, present) = connect(&uri, &resume("pub2", &keep)).await;
    assert!(present);
    // The resent PUBLISH is the same message (4.3.3).
    let mut dup = publish_bytes(Version::V5, "in2", b"once", QoS::ExactlyOnce, 7, &[]);
    dup[0] |= 0x08;
    raw.send(&dup).await;
    assert_eq!(raw.next().await, ack(PacketType::PubRec, 7));
    raw.ack(PacketType::PubRel, 7).await;
    assert_eq!(raw.next().await, ack(PacketType::PubComp, 7));
    assert_eq!(next(&mut sub).await.payload, "once");
    nothing(&mut sub).await;
    // After PUBCOMP the ID is free, and a stale PUBREL is unknown.
    raw.ack(PacketType::PubRel, 7).await;
    assert_eq!(
        raw.next().await,
        Got::Ack {
            kind: PacketType::PubComp,
            pkid: 7,
            reason: reason::PACKET_IDENTIFIER_NOT_FOUND
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn qos2_packet_id_is_reusable_across_reconnects() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut sub_a = client(&ctx, &uri, "a", Version::V5).await;
    sub_a.subscribe("a", QoS::ExactlyOnce).await.unwrap();
    let mut sub_b = client(&ctx, &uri, "b", Version::V5).await;
    sub_b.subscribe("b", QoS::ExactlyOnce).await.unwrap();
    // Mosquitto #1322: packet ID 1 again after a reconnect, to another topic.
    let keep = [KEEP];
    for topic in ["a", "b"] {
        let (mut raw, _) = connect(&uri, &resume("reuse", &keep)).await;
        raw.pub_(topic, topic.as_bytes(), QoS::ExactlyOnce, 1).await;
        assert_eq!(raw.next().await, ack(PacketType::PubRec, 1));
        raw.ack(PacketType::PubRel, 1).await;
        assert_eq!(raw.next().await, ack(PacketType::PubComp, 1));
        raw.disconnect(0, &[]).await;
        raw.closed().await;
    }
    assert_eq!(next(&mut sub_a).await.payload, "a");
    assert_eq!(next(&mut sub_b).await.payload, "b");
    nothing(&mut sub_a).await;
    nothing(&mut sub_b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn publisher_over_the_receive_maximum_is_disconnected() {
    let opts = BrokerOptions {
        receive_maximum: 2,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let mut raw = Raw::connect(&uri, "eager", Version::V5, 0).await;
    for pkid in 1..=2 {
        raw.pub_("t", b"x", QoS::ExactlyOnce, pkid).await;
        assert_eq!(raw.next().await, ack(PacketType::PubRec, pkid));
    }
    // A third QoS 2 publish before any PUBREL [MQTT-3.3.4-7].
    raw.pub_("t", b"x", QoS::ExactlyOnce, 3).await;
    raw.disconnected(&[reason::RECEIVE_MAXIMUM_EXCEEDED]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn packet_ids_are_separate_per_direction() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let one = [Property::U16(RECEIVE_MAXIMUM, 1)];
    let (mut raw, _) = connect(&uri, &fresh("dir", &one)).await;
    raw.subscribe("dir", QoS::AtLeastOnce.into()).await;
    publ.publish("dir", "out", QoS::AtLeastOnce).await.unwrap();
    let out = raw.publish().await;
    // The client uses the same ID for its own PUBLISH and SUBSCRIBE (2.2.1).
    raw.pub_("elsewhere", b"in", QoS::AtLeastOnce, out.pkid)
        .await;
    match raw.next().await {
        Got::Ack {
            kind: PacketType::PubAck,
            pkid,
            reason,
        } => assert!(pkid == out.pkid && reason < 0x80, "{reason:#04x}"),
        got => panic!("expected PUBACK, got {got:?}"),
    }
    raw.subscribe_with(out.pkid, &[], &[("other", QoS::AtMostOnce.into())])
        .await;
    // The PUBACK for the delivery still completes it: the window opens.
    raw.ack(PacketType::PubAck, out.pkid).await;
    publ.publish("dir", "next", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(raw.publish().await.payload, b"next");
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_sent_just_before_disconnect_is_kept() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let keep = [KEEP];
    let (mut raw, _) = connect(&uri, &fresh("kept", &keep)).await;
    raw.subscribe("kept", QoS::AtLeastOnce.into()).await;
    for i in 0..3 {
        publ.publish("kept", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    let first = raw.publish().await;
    raw.publish().await;
    raw.publish().await;
    // PUBACK and DISCONNECT in one write (EMQX t_puback_not_lost_on_disconnect).
    let mut bytes = vec![0x40, 2];
    bytes.extend(first.pkid.to_be_bytes());
    bytes.extend([0xE0, 0]);
    raw.send(&bytes).await;
    raw.closed().await;
    stats_until(&broker, |s| s.connections == 1).await;
    let (mut raw, _) = connect(&uri, &resume("kept", &keep)).await;
    let again: Vec<_> = [raw.publish().await, raw.publish().await]
        .iter()
        .map(num)
        .collect();
    assert_eq!(again, [1, 2]);
    raw.quiet(300).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_qos1_delivery_is_skipped_cleanly() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let small = [
        Property::U16(RECEIVE_MAXIMUM, 1),
        Property::U32(MAXIMUM_PACKET_SIZE, 64),
    ];
    let (mut raw, _) = connect(&uri, &fresh("small", &small)).await;
    raw.subscribe("big", QoS::AtLeastOnce.into()).await;
    // Too large for the client [MQTT-3.1.2-25]: dropped without taking the
    // only window slot.
    publ.publish("big", vec![0u8; 200], QoS::AtLeastOnce)
        .await
        .unwrap();
    publ.publish("big", "ok", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(raw.publish().await.payload, b"ok");
    stats_until(&broker, |s| s.dropped.too_large == 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn qos0_publisher_keeps_flowing_past_a_stopped_subscriber() {
    let opts = BrokerOptions {
        max_out_bytes: 256 << 10,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let mut stopped = Raw::connect(&uri, "stopped", Version::V5, 0).await;
    stopped.subscribe("flood", QoS::AtMostOnce.into()).await;
    let mut healthy = client(&ctx, &uri, "healthy", Version::V5).await;
    healthy.subscribe("flood", QoS::AtMostOnce).await.unwrap();
    let reader = tokio::spawn(async move {
        loop {
            if next(&mut healthy).await.payload == "end" {
                // Still connected for the count below.
                return healthy;
            }
        }
    });
    let mut publ = Raw::connect(&uri, "flooder", Version::V5, 0).await;
    let one = publish_bytes(Version::V5, "flood", &[7; 1024], QoS::AtMostOnce, 0, &[]);
    let batch = one.repeat(100);
    tokio::time::timeout(Duration::from_secs(10), async {
        for _ in 0..200 {
            publ.send(&batch).await;
        }
    })
    .await
    .expect("the broker stopped reading the publisher");
    tokio::time::sleep(Duration::from_millis(200)).await;
    publ.pub_("flood", b"end", QoS::AtMostOnce, 0).await;
    let _healthy = tokio::time::timeout(WAIT, reader)
        .await
        .expect("the healthy subscriber starved")
        .unwrap();
    // The stopped subscriber's QoS 0 messages were dropped and counted, and
    // it stays connected.
    let stats = stats_until(&broker, |s| s.dropped.buffer_full > 0).await;
    assert_eq!(stats.connections, 3);
    assert!(matches!(stopped.next().await, Got::Publish(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_subscriber_slows_qos2_and_v311_publishers() {
    for lanes in [1, 3] {
        let opts = BrokerOptions {
            max_queued: 200,
            qos_queue_high: 64,
            max_inflight: 32,
            ..BrokerOptions::default()
        };
        let (ctx, broker, uri) = setup(lanes, opts).await;
        let slow = ClientOptions {
            inbox: 16,
            version: Version::V311,
            ..ClientOptions::new("slow")
        };
        let mut sub = Client::connect(&ctx, &uri, slow).await.unwrap();
        sub.subscribe("in/+", QoS::ExactlyOnce).await.unwrap();
        let mut tasks = Vec::new();
        for i in 0..4 {
            let c = client(&ctx, &uri, &format!("p{i}"), Version::V5).await;
            tasks.push(tokio::spawn(async move {
                for n in 0..500 {
                    c.publish(&format!("in/{i}"), format!("{n}"), QoS::ExactlyOnce)
                        .await
                        .unwrap();
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        for count in 1..=2000 {
            next(&mut sub).await;
            if count % 250 == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        for t in tasks {
            t.await.unwrap();
        }
        let stats = broker.stats().await.unwrap();
        assert_eq!(stats.dropped.queue_full, 0, "lanes={lanes}");
        nothing(&mut sub).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_session_expiry_rules() {
    let (_ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let expiry = |s| [Property::U32(SESSION_EXPIRY_INTERVAL, s)];
    // DISCONNECT may extend the session (3.14.2.2.2).
    let one = expiry(1);
    let (mut raw, _) = connect(&uri, &fresh("extend", &one)).await;
    raw.disconnect(0, &expiry(5)).await;
    raw.closed().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (_extended, present) = connect(&uri, &resume("extend", &one)).await;
    assert!(present);
    // Or end it.
    let keep = [KEEP];
    let (mut raw, _) = connect(&uri, &fresh("end", &keep)).await;
    raw.disconnect(0, &expiry(0)).await;
    raw.closed().await;
    let (_ended, present) = connect(&uri, &resume("end", &keep)).await;
    assert!(!present);
    // But not start one.
    let mut raw = Raw::connect(&uri, "start", Version::V5, 0).await;
    raw.disconnect(0, &expiry(5)).await;
    raw.disconnected(&[reason::PROTOCOL_ERROR]).await;
    // The largest expiry neither overflows nor ends the session at once
    // (Mosquitto #1525).
    let huge = expiry(u32::MAX - 1);
    let (raw, _) = connect(&uri, &fresh("huge", &huge)).await;
    drop(raw);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_huge, present) = connect(&uri, &resume("huge", &huge)).await;
    assert!(present);
}

#[tokio::test(flavor = "multi_thread")]
async fn will_waits_for_the_shorter_of_delay_and_session_expiry() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut watch = client(&ctx, &uri, "watch", Version::V5).await;
    watch.subscribe("wills/#", QoS::AtLeastOnce).await.unwrap();
    // [MQTT-3.1.2-8], (will delay, session expiry, seconds to the will).
    for (delay, expiry, after) in [(5, 1, 1), (30, 0, 0), (1, 5, 1)] {
        let topic = format!("wills/{delay}-{expiry}");
        let will_props = [Property::U32(WILL_DELAY_INTERVAL, delay)];
        let props = [Property::U32(SESSION_EXPIRY_INTERVAL, expiry)];
        let c = ConnectFields {
            will: Some(will(&topic, &will_props)),
            ..fresh(&topic, &props)
        };
        let (raw, _) = connect(&uri, &c).await;
        let started = Instant::now();
        drop(raw);
        assert_eq!(next(&mut watch).await.topic, topic);
        let took = started.elapsed();
        let want = Duration::from_secs(after);
        assert!(
            took + Duration::from_millis(300) >= want && took < want + Duration::from_millis(1500),
            "{topic}: {took:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnect_with_will_reason_publishes_the_will() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut watch = client(&ctx, &uri, "watch", Version::V5).await;
    watch.subscribe("dw/#", QoS::AtLeastOnce).await.unwrap();
    // [MQTT-3.14.4-3]
    for (code, sent) in [(reason::DISCONNECT_WITH_WILL, true), (0, false)] {
        let topic = format!("dw/{code}");
        let c = ConnectFields {
            will: Some(will(&topic, &[])),
            ..fresh(&topic, &[])
        };
        let (mut raw, _) = connect(&uri, &c).await;
        raw.disconnect(code, &[]).await;
        raw.closed().await;
        if sent {
            assert_eq!(next(&mut watch).await.topic, topic);
        } else {
            nothing(&mut watch).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn broker_closed_connections_publish_the_will() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut watch = client(&ctx, &uri, "watch", Version::V5).await;
    watch.subscribe("bc/#", QoS::AtLeastOnce).await.unwrap();
    // A keep alive timeout [MQTT-3.1.2-8].
    let c = ConnectFields {
        keep_alive: 1,
        will: Some(will("bc/keep-alive", &[])),
        ..fresh("silent", &[])
    };
    let (mut raw, _) = connect(&uri, &c).await;
    raw.disconnected(&[reason::KEEP_ALIVE_TIMEOUT]).await;
    assert_eq!(next(&mut watch).await.topic, "bc/keep-alive");
    // A protocol error.
    let c = ConnectFields {
        will: Some(will("bc/protocol", &[])),
        ..fresh("rude", &[])
    };
    let (mut raw, _) = connect(&uri, &c).await;
    raw.send(&connect_bytes(Version::V5, "rude", &[])).await;
    raw.disconnected(&[reason::PROTOCOL_ERROR]).await;
    assert_eq!(next(&mut watch).await.topic, "bc/protocol");
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_of_a_session_without_expiry_sends_its_delayed_will_at_once() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut watch = client(&ctx, &uri, "watch", Version::V5).await;
    watch.subscribe("tw", QoS::AtLeastOnce).await.unwrap();
    let delayed = [Property::U32(WILL_DELAY_INTERVAL, 10)];
    let c = ConnectFields {
        will: Some(will("tw", &delayed)),
        ..fresh("taken", &[])
    };
    let (mut old, _) = connect(&uri, &c).await;
    old.subscribe("old/sub", QoS::AtMostOnce.into()).await;
    // The old session ends with its connection, so the will goes out now
    // and nothing is resumed (EMQX #18565).
    let started = Instant::now();
    let (mut new, present) = connect(&uri, &resume("taken", &[])).await;
    assert!(!present);
    old.disconnected(&[reason::SESSION_TAKEN_OVER]).await;
    assert_eq!(next(&mut watch).await.topic, "tw");
    assert!(started.elapsed() < Duration::from_secs(2));
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish("old/sub", "x", QoS::AtMostOnce).await.unwrap();
    new.quiet(300).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_tells_only_v5_clients_why() {
    let (_ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    // [MQTT-3.1.4-3]; a 3.1.1 connection just closes.
    for version in [Version::V5, Version::V311] {
        let mut old = Raw::connect(&uri, "taken", version, 0).await;
        let _new = Raw::connect(&uri, "taken", Version::V5, 0).await;
        old.disconnected(&[reason::SESSION_TAKEN_OVER]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_under_load_loses_and_reorders_nothing() {
    let (ctx, _broker, uri) = setup(3, BrokerOptions::default()).await;
    let keep = [KEEP];
    let (mut a, _) = connect(&uri, &fresh("loaded", &keep)).await;
    a.subscribe("load", QoS::AtLeastOnce.into()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let sender = tokio::spawn(async move {
        for i in 0..2000u32 {
            publ.publish("load", i.to_string(), QoS::AtLeastOnce)
                .await
                .unwrap();
        }
    });
    let mut seen_a = Vec::new();
    while seen_a.len() < 500 {
        let p = a.publish().await;
        a.ack(PacketType::PubAck, p.pkid).await;
        seen_a.push(num(&p));
    }
    let (mut b, present) = connect(&uri, &resume("loaded", &keep)).await;
    assert!(present);
    loop {
        match a.try_next().await {
            Some(Got::Publish(_)) => {}
            Some(Got::Disconnect { reason }) => {
                assert_eq!(reason, reason::SESSION_TAKEN_OVER);
                break;
            }
            got => panic!("old connection: {got:?}"),
        }
    }
    let mut seen_b = Vec::new();
    loop {
        let p = b.publish().await;
        b.ack(PacketType::PubAck, p.pkid).await;
        seen_b.push(num(&p));
        if seen_b.last() == Some(&1999) {
            break;
        }
    }
    sender.await.unwrap();
    // [MQTT-4.6.0-6]: each connection sees the stream in order, B resumes
    // at or before the first message A did not ack, and nothing is lost.
    assert!(seen_a.windows(2).all(|w| w[0] < w[1]));
    assert!(seen_b.windows(2).all(|w| w[0] < w[1]), "{seen_b:?}");
    assert!(seen_b[0] <= 500, "{}", seen_b[0]);
    let mut all: Vec<_> = seen_a.iter().chain(&seen_b).copied().collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all, (0..2000).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_connects_with_one_client_id_leave_one_session() {
    let opts = BrokerOptions {
        max_connections: 4,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(3, opts).await;
    let tasks: Vec<_> = (0..20)
        .map(|_| {
            let uri = uri.clone();
            tokio::spawn(async move {
                let mut raw = Raw::open(&uri).await;
                raw.send(&connect_bytes(Version::V5, "same", &[])).await;
                raw
            })
        })
        .collect();
    let mut raws = Vec::new();
    for t in tasks {
        raws.push(t.await.unwrap());
    }
    let mut open = 0;
    for raw in &mut raws {
        // Taken over before its CONNACK, a connection just closes
        // [MQTT-3.2.0-1].
        match raw.try_next().await {
            Some(Got::ConnAck { code: 0, .. }) => {}
            None => continue,
            got => panic!("{got:?}"),
        }
        match raw.try_next_within(Duration::from_millis(300)).await {
            None => open += 1,
            Some(Some(Got::Disconnect {
                reason: reason::SESSION_TAKEN_OVER,
            })) => raw.closed().await,
            got => panic!("{got:?}"),
        }
    }
    assert_eq!(open, 1);
    stats_until(&broker, |s| s.connections == 1 && s.sessions == 1).await;
    drop(raws);
    stats_until(&broker, |s| s.connections == 0).await;
    // No connection slot leaked.
    let mut fits = Vec::new();
    for i in 0..4 {
        fits.push(client(&ctx, &uri, &format!("c{i}"), Version::V5).await);
    }
}

/// Lets in only clients with the password "good".
struct Passwords;

impl Authenticator for Passwords {
    fn authenticate(&self, info: &ConnectInfo<'_>) -> AuthDecision {
        if info.password == Some(b"good") {
            AuthDecision::Allow
        } else {
            AuthDecision::Deny(ReasonCode::BAD_USER_NAME_OR_PASSWORD)
        }
    }
}

/// Keeps everyone off `secret/`.
struct NoSecrets;

impl Authorizer for NoSecrets {
    fn may_publish(&self, _: &ClientInfo<'_>, topic: &str) -> bool {
        !topic.starts_with("secret/")
    }

    fn may_subscribe(&self, _: &ClientInfo<'_>, _: &str) -> bool {
        true
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn refused_connect_leaves_the_existing_session_alone() {
    let opts = BrokerOptions {
        authenticator: Some(Arc::new(Passwords)),
        authorizer: Some(Arc::new(NoSecrets)),
        ..BrokerOptions::default()
    };
    let (ctx, _broker, uri) = setup(1, opts).await;
    let good = |id: &str| ClientOptions {
        password: Some(Bytes::from_static(b"good")),
        ..ClientOptions::new(id)
    };
    let victim = ClientOptions {
        will: Some(LastWill {
            topic: "victim/will".into(),
            payload: Bytes::from_static(b"gone"),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 0,
        }),
        ..good("victim")
    };
    let mut victim = Client::connect(&ctx, &uri, victim).await.unwrap();
    victim
        .subscribe("victim/in", QoS::AtMostOnce)
        .await
        .unwrap();
    let mut watch = Client::connect(&ctx, &uri, good("watch")).await.unwrap();
    watch
        .subscribe("victim/will", QoS::AtMostOnce)
        .await
        .unwrap();
    // Authentication comes before takeover (3.1.4): a wrong password, or a
    // will the authorizer denies, leaves the victim alone.
    let wrong = ConnectFields {
        password: Some(b"bad"),
        ..fresh("victim", &[])
    };
    let secret_will = ConnectFields {
        password: Some(b"good"),
        will: Some(will("secret/x", &[])),
        ..fresh("victim", &[])
    };
    for (c, code) in [
        (wrong, reason::BAD_USER_NAME_OR_PASSWORD),
        (secret_will, reason::NOT_AUTHORIZED),
    ] {
        let (mut raw, got) = Raw::connect_with(&uri, &c).await;
        assert_eq!(got, code);
        raw.closed().await;
    }
    watch
        .publish("victim/in", "still", QoS::AtMostOnce)
        .await
        .unwrap();
    assert_eq!(next(&mut victim).await.payload, "still");
    nothing(&mut watch).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_start_discards_an_offline_session_and_its_queue() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let keep = ClientOptions {
        session_expiry: 60,
        ..ClientOptions::new("offline")
    };
    let sub = Client::connect(&ctx, &uri, keep).await.unwrap();
    sub.subscribe("q", QoS::AtLeastOnce).await.unwrap();
    sub.disconnect().await;
    stats_until(&broker, |s| s.connections == 1).await;
    publ.publish("q", "queued", QoS::AtLeastOnce).await.unwrap();
    // [MQTT-3.1.2-4]
    let (mut raw, present) = connect(&uri, &fresh("offline", &[])).await;
    assert!(!present);
    raw.quiet(300).await;
    publ.publish("q", "after", QoS::AtLeastOnce).await.unwrap();
    raw.quiet(300).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ended_session_counts_the_messages_it_held() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let keep = [KEEP];
    // Queued while offline, then ended by a clean start.
    let (mut raw, _) = connect(&uri, &fresh("queued", &keep)).await;
    raw.subscribe("held/q", QoS::AtLeastOnce.into()).await;
    drop(raw);
    stats_until(&broker, |s| s.connections == 1).await;
    for i in 0..50 {
        publ.publish("held/q", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    let (_clean, present) = connect(&uri, &fresh("queued", &[])).await;
    assert!(!present);
    stats_until(&broker, |s| s.dropped.session_ended == 50).await;
    // Sent and not acked, then ended by DISCONNECT with expiry 0.
    let (mut raw, _) = connect(&uri, &fresh("unacked", &keep)).await;
    raw.subscribe("held/u", QoS::AtLeastOnce.into()).await;
    for i in 0..3 {
        publ.publish("held/u", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
        raw.publish().await;
    }
    raw.disconnect(0, &[Property::U32(SESSION_EXPIRY_INTERVAL, 0)])
        .await;
    raw.closed().await;
    stats_until(&broker, |s| s.dropped.session_ended == 53).await;
    // Ended by expiry.
    let one = [Property::U32(SESSION_EXPIRY_INTERVAL, 1)];
    let (mut raw, _) = connect(&uri, &fresh("expires", &one)).await;
    raw.subscribe("held/e", QoS::AtLeastOnce.into()).await;
    drop(raw);
    stats_until(&broker, |s| s.connections == 2).await;
    for i in 0..7 {
        publ.publish("held/e", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    stats_until(&broker, |s| s.dropped.session_ended == 60).await;
}

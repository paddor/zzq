//! Routing rules from other brokers' test suites: per-subscriber QoS,
//! retain flags and packet IDs, property forwarding, topic matching,
//! retained messages, limits and connection churn.

#![expect(clippy::unwrap_used, clippy::expect_used, reason = "tests fail loudly")]

mod support;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use support::{Got, Pub, Raw, WAIT, client, next, nothing, props, setup, stats_until};
use zzq_broker::{Authorizer, BrokerOptions, ClientInfo};
use zzq_client::{Client, ClientOptions};
use zzq_proto::encode::{self, ConnectFields, WillFields};
use zzq_proto::property::{
    CONTENT_TYPE, CORRELATION_DATA, MESSAGE_EXPIRY_INTERVAL, PAYLOAD_FORMAT_INDICATOR,
    RESPONSE_TOPIC, SESSION_EXPIRY_INTERVAL, SUBSCRIPTION_IDENTIFIER, WILL_DELAY_INTERVAL,
};
use zzq_proto::reason;
use zzq_proto::{PacketType, Properties, Property, QoS, SubscriptionOptions, Version};

const KEEP: Property<'static> = Property::U32(SESSION_EXPIRY_INTERVAL, 60);

const fn fields<'a>(
    id: &'a str,
    clean_start: bool,
    properties: &'a [Property<'a>],
) -> ConnectFields<'a> {
    ConnectFields {
        version: Version::V5,
        clean_start,
        keep_alive: 0,
        properties,
        client_id: id,
        will: None,
        username: None,
        password: None,
    }
}

/// The Subscription Identifiers a delivery carries.
fn sub_ids(p: &Pub) -> BTreeSet<u32> {
    p.properties()
        .iter()
        .filter_map(|p| match p {
            Property::VarInt(SUBSCRIPTION_IDENTIFIER, id) => Some(id),
            _ => None,
        })
        .collect()
}

/// A delivery's properties a broker forwards unchanged, in order.
fn forwarded(p: &Pub) -> Vec<String> {
    p.properties()
        .iter()
        .filter(|p| ![MESSAGE_EXPIRY_INTERVAL, SUBSCRIPTION_IDENTIFIER].contains(&p.id()))
        .map(|p| format!("{p:?}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn fan_out_gives_each_subscriber_its_own_qos_and_packet_id() {
    let (_ctx, _broker, uri) = setup(3, BrokerOptions::default()).await;
    let grants = [
        (Version::V5, QoS::AtMostOnce),
        (Version::V311, QoS::AtLeastOnce),
        (Version::V5, QoS::ExactlyOnce),
        (Version::V31, QoS::AtLeastOnce),
        (Version::V311, QoS::ExactlyOnce),
    ];
    let mut subs = Vec::new();
    for (i, (version, qos)) in grants.iter().enumerate() {
        let mut raw = Raw::connect(&uri, &format!("fan{i}"), *version, 0).await;
        raw.subscribe("fan/#", (*qos).into()).await;
        subs.push(raw);
    }
    // The publisher's packet ID never leaks to subscribers, and each gets
    // the lower of the publish and granted QoS [MQTT-3.8.4-6].
    let mut publ = Raw::connect(&uri, "source", Version::V5, 0).await;
    publ.pub_("fan/live", b"x", QoS::ExactlyOnce, 4242).await;
    assert!(matches!(publ.next().await, Got::Ack { pkid: 4242, .. }));
    publ.ack(PacketType::PubRel, 4242).await;
    // Then the same for a will.
    let c = ConnectFields {
        will: Some(WillFields {
            qos: QoS::ExactlyOnce,
            retain: false,
            properties: &[],
            topic: "fan/will",
            payload: b"gone",
        }),
        ..fields("doomed", true, &[])
    };
    let (doomed, _) = Raw::connect_with(&uri, &c).await;
    drop(doomed);
    for topic in ["fan/live", "fan/will"] {
        for (raw, (_, qos)) in subs.iter_mut().zip(grants) {
            let p = raw.publish().await;
            assert_eq!((&p.topic[..], p.qos), (topic, qos), "{:?}", raw.version);
            if qos != QoS::AtMostOnce {
                assert_ne!(p.pkid, 4242);
            }
            raw.complete(&p).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn retain_flag_follows_each_subscribers_retain_as_published() {
    let (_ctx, _broker, uri) = setup(2, BrokerOptions::default()).await;
    let rap = SubscriptionOptions {
        retain_as_published: true,
        ..QoS::AtLeastOnce.into()
    };
    let mut keeps = Raw::connect(&uri, "keeps", Version::V5, 0).await;
    keeps.subscribe("rap", rap).await;
    let mut clears = Raw::connect(&uri, "clears", Version::V5, 0).await;
    clears.subscribe("rap", QoS::AtLeastOnce.into()).await;
    let mut old = Raw::connect(&uri, "old", Version::V311, 0).await;
    old.subscribe("rap", QoS::AtLeastOnce.into()).await;
    // A live retained publish [MQTT-3.3.1-12, -13], at QoS 0 and 1.
    let mut publ = Raw::connect(&uri, "p", Version::V5, 0).await;
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce] {
        let mut bytes = support::publish_bytes(Version::V5, "rap", b"r", qos, 1, &[]);
        bytes[0] |= 0x01;
        publ.send(&bytes).await;
        for (raw, retain) in [(&mut keeps, true), (&mut clears, false), (&mut old, false)] {
            let p = raw.publish().await;
            assert_eq!((p.qos, p.retain), (qos, retain), "{:?}", raw.version);
            raw.complete(&p).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delivery_qos_is_the_lower_of_publish_and_grant() {
    let (ctx, _broker, uri) = setup(3, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish_retained("low/kept", "kept", QoS::ExactlyOnce)
        .await
        .unwrap();
    let mut raw = Raw::connect(&uri, "low", Version::V5, 0).await;
    raw.subscribe("low/#", QoS::AtLeastOnce.into()).await;
    // [MQTT-3.8.4-8], for a retained delivery and live ones.
    let kept = raw.publish().await;
    assert_eq!((kept.qos, kept.retain), (QoS::AtLeastOnce, true));
    raw.complete(&kept).await;
    for (qos, want) in [
        (QoS::AtMostOnce, QoS::AtMostOnce),
        (QoS::AtLeastOnce, QoS::AtLeastOnce),
        (QoS::ExactlyOnce, QoS::AtLeastOnce),
    ] {
        publ.publish("low/live", "x", qos).await.unwrap();
        let p = raw.publish().await;
        assert_eq!(p.qos, want, "published at {qos:?}");
        raw.complete(&p).await;
    }
}

/// Properties a broker must pass on unchanged [MQTT-3.3.2-4, -15..-18,
/// -20], User Properties in order.
fn app_properties() -> Vec<u8> {
    props(&[
        Property::Byte(PAYLOAD_FORMAT_INDICATOR, 1),
        Property::Str(CONTENT_TYPE, "text/plain"),
        Property::Str(RESPONSE_TOPIC, "reply/1"),
        Property::Binary(CORRELATION_DATA, &[1, 2]),
        Property::User("a", "1"),
        Property::User("b", "2"),
        Property::User("a", "3"),
    ])
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_properties_reach_subscribers_unchanged() {
    let (_ctx, broker, uri) = setup(2, BrokerOptions::default()).await;
    let sent = app_properties();
    let want: Vec<String> = Properties::try_from(&sent[..])
        .unwrap()
        .iter()
        .map(|p| format!("{p:?}"))
        .collect();
    let mut live = Raw::connect(&uri, "live", Version::V5, 0).await;
    live.subscribe("props/#", QoS::ExactlyOnce.into()).await;
    let mut old = Raw::connect(&uri, "old", Version::V311, 0).await;
    old.subscribe("props/#", QoS::ExactlyOnce.into()).await;
    let keep = [KEEP];
    let (mut offline, _) = Raw::connect_with(&uri, &fields("offline", true, &keep)).await;
    offline.subscribe("props/#", QoS::AtLeastOnce.into()).await;
    drop(offline);
    stats_until(&broker, |s| s.connections == 2).await;
    let mut publ = Raw::connect(&uri, "p", Version::V5, 0).await;
    for (pkid, qos) in [
        (0, QoS::AtMostOnce),
        (1, QoS::AtLeastOnce),
        (2, QoS::ExactlyOnce),
    ] {
        publ.pub_props("props/live", b"payload", qos, pkid, &sent)
            .await;
    }
    let mut retained = support::publish_bytes(
        Version::V5,
        "props/kept",
        b"payload",
        QoS::AtMostOnce,
        0,
        &sent,
    );
    retained[0] |= 0x01;
    publ.send(&retained).await;
    for _ in 0..4 {
        let p = live.publish().await;
        assert_eq!(forwarded(&p), want, "live {:?}", p.qos);
        live.complete(&p).await;
        // A 3.1.1 subscriber gets the same message without properties.
        let p = old.publish().await;
        assert_eq!(p.payload, b"payload");
        old.complete(&p).await;
    }
    // A retained delivery, and the queued QoS 1 and 2 ones (QoS 0 is not
    // queued for offline sessions).
    let mut late = Raw::connect(&uri, "late", Version::V5, 0).await;
    late.subscribe("props/kept", QoS::AtMostOnce.into()).await;
    assert_eq!(forwarded(&late.publish().await), want, "retained");
    let (mut offline, _) = Raw::connect_with(&uri, &fields("offline", false, &keep)).await;
    for _ in 0..2 {
        let p = offline.publish().await;
        assert_eq!(forwarded(&p), want, "queued");
        offline.complete(&p).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn will_properties_reach_subscribers() {
    let (_ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut watch = Raw::connect(&uri, "watch", Version::V5, 0).await;
    watch.subscribe("wp", QoS::AtLeastOnce.into()).await;
    let app = app_properties();
    let mut will_props: Vec<Property<'_>> =
        Properties::try_from(&app[..]).unwrap().iter().collect();
    will_props.push(Property::U32(WILL_DELAY_INTERVAL, 0));
    let c = ConnectFields {
        will: Some(WillFields {
            qos: QoS::AtLeastOnce,
            retain: false,
            properties: &will_props,
            topic: "wp",
            payload: b"gone",
        }),
        ..fields("doomed", true, &[])
    };
    let (doomed, _) = Raw::connect_with(&uri, &c).await;
    drop(doomed);
    // Will Delay is not a PUBLISH property; the rest pass unchanged
    // [MQTT-3.1.3-10].
    let p = watch.publish().await;
    let want: Vec<String> = Properties::try_from(&app[..])
        .unwrap()
        .iter()
        .map(|p| format!("{p:?}"))
        .collect();
    assert_eq!(forwarded(&p), want);
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_delivery_carries_the_reduced_message_expiry() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let keep = [KEEP];
    let (mut raw, _) = Raw::connect_with(&uri, &fields("away", true, &keep)).await;
    raw.subscribe("exp", QoS::AtLeastOnce.into()).await;
    drop(raw);
    stats_until(&broker, |s| s.connections == 0).await;
    let mut publ = Raw::connect(&uri, "p", Version::V5, 0).await;
    let ten = props(&[Property::U32(MESSAGE_EXPIRY_INTERVAL, 10)]);
    publ.pub_props("exp", b"x", QoS::AtLeastOnce, 1, &ten).await;
    publ.next().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    // [MQTT-3.3.2-6]
    let (mut raw, _) = Raw::connect_with(&uri, &fields("away", false, &keep)).await;
    let p = raw.publish().await;
    let left = p.properties().get_int(MESSAGE_EXPIRY_INTERVAL).unwrap();
    assert!((7..=8).contains(&left), "{left}");
    drop(ctx);
}

#[tokio::test(flavor = "multi_thread")]
async fn resubscribe_replaces_the_subscription() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish_retained("re", "kept", QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut raw = Raw::connect(&uri, "re", Version::V5, 0).await;
    let with_id = |id| [Property::VarInt(SUBSCRIPTION_IDENTIFIER, id)];
    let opts = |qos: QoS, retain_handling| SubscriptionOptions {
        retain_handling,
        ..qos.into()
    };
    raw.subscribe_with(1, &with_id(5), &[("re", opts(QoS::ExactlyOnce, 0))])
        .await;
    assert_eq!(sub_ids(&raw.publish().await), BTreeSet::from([5]));
    // Resubscribing replaces QoS and identifier [MQTT-3.8.4-3], and with
    // Retain Handling 0 sends the retained message again.
    raw.subscribe_with(2, &with_id(7), &[("re", opts(QoS::AtLeastOnce, 0))])
        .await;
    let again = raw.publish().await;
    assert!(again.retain);
    assert_eq!(sub_ids(&again), BTreeSet::from([7]));
    raw.complete(&again).await;
    publ.publish("re", "live", QoS::ExactlyOnce).await.unwrap();
    let p = raw.publish().await;
    assert_eq!(
        (p.qos, sub_ids(&p)),
        (QoS::AtLeastOnce, BTreeSet::from([7]))
    );
    raw.complete(&p).await;
    raw.quiet(200).await;
    // Retain Handling 1 skips it for an existing subscription; no
    // identifier clears it.
    raw.subscribe_with(3, &[], &[("re", opts(QoS::AtMostOnce, 1))])
        .await;
    raw.quiet(200).await;
    publ.publish("re", "live", QoS::AtMostOnce).await.unwrap();
    assert!(sub_ids(&raw.publish().await).is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_matching_handles_empty_levels() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let filters = ["foo/#", "foo/+/baz", "test/topic/+", "/#", "+/+", "+"];
    // (topic, the indexes of the filters that match it)
    let cases: [(&str, &[u32]); 6] = [
        ("foo", &[0, 5]),
        ("foo/", &[0, 4]),
        ("foo//baz", &[0, 1]),
        ("test/topic/", &[2]),
        ("////foo///bar", &[3]),
        ("/", &[3, 4]),
    ];
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    for (topic, _) in cases {
        publ.publish_retained(topic, topic, QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    // Live deliveries: one copy per topic, with the identifiers of every
    // matching filter.
    let mut live = Raw::connect(&uri, "live", Version::V5, 0).await;
    let rh2 = SubscriptionOptions {
        retain_handling: 2,
        ..QoS::AtMostOnce.into()
    };
    for (i, f) in filters.iter().enumerate() {
        let id = [Property::VarInt(SUBSCRIPTION_IDENTIFIER, i as u32 + 1)];
        live.subscribe_with(1, &id, &[(f, rh2)]).await;
    }
    for (topic, want) in cases {
        publ.publish(topic, topic, QoS::AtMostOnce).await.unwrap();
        let p = live.publish().await;
        let got: BTreeSet<u32> = sub_ids(&p).iter().map(|id| id - 1).collect();
        assert_eq!((&p.topic[..], got), (topic, want.iter().copied().collect()));
    }
    // Retained deliveries use their own matcher: the same answers.
    for (i, f) in filters.iter().enumerate() {
        let mut raw = Raw::connect(&uri, &format!("kept{i}"), Version::V5, 0).await;
        raw.subscribe(f, QoS::AtMostOnce.into()).await;
        let want: BTreeSet<&str> = cases
            .iter()
            .filter(|(_, m)| m.contains(&(i as u32)))
            .map(|(t, _)| *t)
            .collect();
        let mut got = BTreeSet::new();
        for _ in 0..want.len() {
            got.insert(raw.publish().await.topic);
        }
        raw.quiet(200).await;
        assert_eq!(
            got.iter().map(String::as_str).collect::<BTreeSet<_>>(),
            want,
            "{f}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unsubscribing_a_plain_filter_keeps_the_shared_one() {
    let (ctx, broker, uri) = setup(2, BrokerOptions::default()).await;
    let mut both = client(&ctx, &uri, "both", Version::V5).await;
    both.subscribe("$share/g/test", QoS::AtMostOnce)
        .await
        .unwrap();
    both.subscribe("test", QoS::AtMostOnce).await.unwrap();
    both.unsubscribe(&["test"]).await.unwrap();
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish("test", "once", QoS::AtMostOnce).await.unwrap();
    assert_eq!(next(&mut both).await.payload, "once");
    nothing(&mut both).await;
    both.disconnect().await;
    stats_until(&broker, |s| s.sessions == 1 && s.subscriptions == 0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_subscriptions_get_no_retained_messages() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish_retained("sh/x", "kept", QoS::AtLeastOnce)
        .await
        .unwrap();
    // 4.8.2: retained messages are not sent for a shared subscription.
    let mut shared = client(&ctx, &uri, "shared", Version::V5).await;
    shared
        .subscribe("$share/g/sh/x", QoS::AtMostOnce)
        .await
        .unwrap();
    nothing(&mut shared).await;
    let mut plain = client(&ctx, &uri, "plain", Version::V5).await;
    plain.subscribe("sh/x", QoS::AtMostOnce).await.unwrap();
    assert_eq!(next(&mut plain).await.payload, "kept");
}

#[tokio::test(flavor = "multi_thread")]
async fn max_subscriptions_refuses_only_new_filters() {
    let opts = BrokerOptions {
        max_subscriptions: 2,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let qos0 = SubscriptionOptions::from(QoS::AtMostOnce);
    for (version, quota) in [
        (Version::V5, reason::QUOTA_EXCEEDED),
        (Version::V311, reason::UNSPECIFIED_ERROR),
    ] {
        let mut raw = Raw::connect(&uri, "many", version, 0).await;
        // Codes come back per filter, in order [MQTT-3.9.3-1].
        let codes = raw
            .subscribe_with(1, &[], &[("a", qos0), ("b", qos0), ("c", qos0)])
            .await;
        assert_eq!(codes, [0, 0, quota], "{version:?}");
        // Replacing a filter needs no new slot; an unsubscribe frees one.
        assert_eq!(raw.subscribe("a", qos0).await, [0]);
        let mut out = Vec::new();
        encode::unsubscribe(version, 2, &[], &["b"], &mut out);
        raw.send(&out).await;
        assert!(matches!(raw.next().await, Got::UnsubAck { pkid: 2, .. }));
        assert_eq!(raw.subscribe("c", qos0).await, [0]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn max_session_expiry_clamps_connect_and_disconnect() {
    let opts = BrokerOptions {
        max_session_expiry: 1,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let long = [Property::U32(SESSION_EXPIRY_INTERVAL, 100)];
    // CONNACK says what the broker uses (3.2.2.3.2).
    let (mut raw, connack) = Raw::connack_with(&uri, &fields("clamped", true, &long)).await;
    let Got::ConnAck { props: p, .. } = connack else {
        panic!("{connack:?}");
    };
    let p = Properties::try_from(&p[..]).unwrap();
    assert_eq!(p.get_int(SESSION_EXPIRY_INTERVAL), Some(1));
    // So does a DISCONNECT that asks for more.
    raw.disconnect(0, &long).await;
    raw.closed().await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let (_raw, connack) = Raw::connack_with(&uri, &fields("clamped", false, &long)).await;
    assert!(matches!(
        connack,
        Got::ConnAck {
            session_present: false,
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn non_retained_publish_leaves_the_retained_message() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let mut watch = client(&ctx, &uri, "watch", Version::V5).await;
    watch.subscribe("nr", QoS::AtMostOnce).await.unwrap();
    publ.publish_retained("nr", "kept", QoS::AtMostOnce)
        .await
        .unwrap();
    publ.publish("nr", "live", QoS::AtMostOnce).await.unwrap();
    publ.publish("nr", "", QoS::AtMostOnce).await.unwrap();
    for _ in 0..3 {
        next(&mut watch).await;
    }
    // 3.3.1.3: only RETAIN 1 replaces or clears the retained message.
    let mut late = client(&ctx, &uri, "late", Version::V5).await;
    late.subscribe("nr", QoS::AtMostOnce).await.unwrap();
    assert_eq!(next(&mut late).await.payload, "kept");
    // An empty retained publish clears it, and still reaches live
    // subscribers [MQTT-3.3.1-10].
    publ.publish_retained("nr", "", QoS::AtMostOnce)
        .await
        .unwrap();
    assert_eq!(next(&mut watch).await.payload, "");
    let mut later = client(&ctx, &uri, "later", Version::V5).await;
    later.subscribe("nr", QoS::AtMostOnce).await.unwrap();
    nothing(&mut later).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_message_arrives_once_when_subscribing_during_publish() {
    let (ctx, _broker, uri) = setup(3, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish_retained("race", "0", QoS::AtMostOnce)
        .await
        .unwrap();
    let writer = tokio::spawn(async move {
        for i in 1..=3000u32 {
            publ.publish_retained("race", i.to_string(), QoS::AtMostOnce)
                .await
                .unwrap();
            if i % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });
    let mut readers = Vec::new();
    for i in 0..20 {
        let uri = uri.clone();
        readers.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(i * 7)).await;
            let mut raw = Raw::connect(&uri, &format!("r{i}"), Version::V5, 0).await;
            raw.subscribe("race", QoS::AtMostOnce.into()).await;
            let mut values = Vec::new();
            let mut retained = 0;
            while let Some(got) = raw.try_next_within(Duration::from_millis(300)).await {
                let Some(Got::Publish(p)) = got else {
                    panic!("{got:?}");
                };
                retained += usize::from(p.retain);
                values.push(
                    std::str::from_utf8(&p.payload)
                        .unwrap()
                        .parse::<u32>()
                        .unwrap(),
                );
            }
            (retained, values)
        }));
    }
    writer.await.unwrap();
    // VerneMQ #1633: one retained copy, then only newer values.
    for r in readers {
        let (retained, values) = r.await.unwrap();
        assert!(retained <= 1, "{retained} retained copies");
        assert!(values.windows(2).all(|w| w[0] < w[1]), "{values:?}");
    }
    drop(ctx);
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_messages_reach_a_session_resumed_on_another_transport() {
    let (ctx, broker, uri) = setup(2, BrokerOptions::default()).await;
    let ws = broker.bind("ws://127.0.0.1:0").await.unwrap().to_string();
    let path = std::env::temp_dir().join(format!("zzq-resume-{}.sock", std::process::id()));
    let ipc = broker
        .bind(&format!("ipc://{}", path.display()))
        .await
        .unwrap()
        .to_string();
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let session = |id: &str| ClientOptions {
        session_expiry: 60,
        clean_start: false,
        ..ClientOptions::new(id)
    };
    // Mosquitto #2618: queued messages reach the session on its new transport.
    for (id, to) in [("to-ws", &ws), ("to-ipc", &ipc)] {
        let sub = Client::connect(&ctx, &uri, session(id)).await.unwrap();
        sub.subscribe(id, QoS::AtLeastOnce).await.unwrap();
        sub.disconnect().await;
        for i in 0..100 {
            publ.publish(id, format!("{i}"), QoS::AtLeastOnce)
                .await
                .unwrap();
        }
        let mut sub = Client::connect(&ctx, to, session(id)).await.unwrap();
        for i in 0..100 {
            assert_eq!(next(&mut sub).await.payload, format!("{i}"), "{to}");
        }
    }
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_churn_leaves_no_connections_or_sessions() {
    let (_ctx, broker, uri) = setup(3, BrokerOptions::default()).await;
    let mut tasks = Vec::new();
    for worker in 0..8 {
        let uri = uri.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..100 {
                let id = format!("churn{worker}-{i}");
                let mut raw = Raw::open(&uri).await;
                let connect = support::connect_bytes(Version::V5, &id, &[]);
                match i % 5 {
                    // Half a CONNECT.
                    0 => raw.send(&connect[..5]).await,
                    // CONNECT, then a reset.
                    1 => {
                        raw.send(&connect).await;
                        raw.next().await;
                        raw.reset();
                        continue;
                    }
                    // CONNECT and SUBSCRIBE, then a close.
                    2 => {
                        raw.send(&connect).await;
                        raw.next().await;
                        raw.subscribe("churn/#", QoS::AtLeastOnce.into()).await;
                    }
                    // A clean DISCONNECT.
                    3 => {
                        raw.send(&connect).await;
                        raw.next().await;
                        raw.disconnect(0, &[]).await;
                        raw.closed().await;
                    }
                    // CONNECT and nothing else.
                    _ => {
                        raw.send(&connect).await;
                        raw.next().await;
                    }
                }
                drop(raw);
            }
        }));
    }
    for t in tasks {
        tokio::time::timeout(WAIT * 4, t).await.unwrap().unwrap();
    }
    // NanoMQ #2349: every connection, session and subscription goes.
    stats_until(&broker, |s| {
        s.connections == 0 && s.sessions == 0 && s.subscriptions == 0
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_group_skips_offline_members() {
    let (ctx, broker, uri) = setup(2, BrokerOptions::default()).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let keep = |id: &str, clean_start| ClientOptions {
        session_expiry: 60,
        clean_start,
        ..ClientOptions::new(id)
    };
    let away = Client::connect(&ctx, &uri, keep("away", true))
        .await
        .unwrap();
    away.subscribe("$share/g/work", QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut here = Client::connect(&ctx, &uri, keep("here", true))
        .await
        .unwrap();
    here.subscribe("$share/g/work", QoS::AtLeastOnce)
        .await
        .unwrap();
    away.disconnect().await;
    stats_until(&broker, |s| s.connections == 2).await;
    // Mosquitto #2662: the connected member gets every message (4.8.2).
    for i in 0..20 {
        publ.publish("work", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    for i in 0..20 {
        assert_eq!(next(&mut here).await.payload, format!("{i}"));
    }
    let mut away = Client::connect(&ctx, &uri, keep("away", false))
        .await
        .unwrap();
    nothing(&mut away).await;
    // With no member connected, the message waits at one of them.
    here.disconnect().await;
    away.disconnect().await;
    stats_until(&broker, |s| s.connections == 1).await;
    publ.publish("work", "later", QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut got = 0;
    for id in ["here", "away"] {
        let mut c = Client::connect(&ctx, &uri, keep(id, false)).await.unwrap();
        if let Ok(Some(m)) = tokio::time::timeout(Duration::from_millis(500), c.recv()).await {
            assert_eq!(m.payload, "later");
            got += 1;
        }
    }
    assert_eq!(got, 1);
}

/// Keeps everyone off `secret/`.
struct NoSecrets;

impl Authorizer for NoSecrets {
    fn may_publish(&self, _: &ClientInfo<'_>, _: &str) -> bool {
        true
    }

    fn may_subscribe(&self, _: &ClientInfo<'_>, filter: &str) -> bool {
        !filter.starts_with("secret/")
    }
}

/// Keeps everyone out of the share group `nope`.
struct NoNope;

impl Authorizer for NoNope {
    fn may_publish(&self, _: &ClientInfo<'_>, _: &str) -> bool {
        true
    }

    fn may_subscribe(&self, _: &ClientInfo<'_>, _: &str) -> bool {
        true
    }

    fn may_subscribe_shared(&self, _: &ClientInfo<'_>, group: &str, _: &str) -> bool {
        group != "nope"
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authorizer_sees_shared_filters_without_the_prefix() {
    let topics: Arc<dyn Authorizer> = Arc::new(NoSecrets);
    let groups: Arc<dyn Authorizer> = Arc::new(NoNope);
    // NanoMQ #2463: `$share/{group}/` must not get a filter past the rules.
    for (acl, filter, allowed) in [
        (&topics, "secret/x", false),
        (&topics, "$share/g/secret/#", false),
        (&topics, "$share/g/public/#", true),
        (&groups, "$share/nope/a", false),
        (&groups, "$share/ok/a", true),
    ] {
        let opts = BrokerOptions {
            authorizer: Some(acl.clone()),
            ..BrokerOptions::default()
        };
        let (_ctx, _broker, uri) = setup(1, opts).await;
        let mut raw = Raw::connect(&uri, "acl", Version::V5, 0).await;
        let want = if allowed { 0 } else { reason::NOT_AUTHORIZED };
        assert_eq!(
            raw.subscribe(filter, QoS::AtMostOnce.into()).await,
            [want],
            "{filter}"
        );
    }
}

//! Protocol rules from other brokers' test suites (Mosquitto, Paho, EMQX,
//! `VerneMQ`, `FlashMQ` and others): what a broker must refuse before and after
//! CONNECT, and the valid forms it must accept.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests fail loudly"
)]

mod support;

use std::time::{Duration, Instant};

use support::{Got, Raw, client, connect_bytes, next, props, publish_bytes, setup};
use zzq_broker::{BrokerOptions, Context};
use zzq_proto::encode::{self, ConnectFields, WillFields};
use zzq_proto::property::{
    AUTHENTICATION_DATA, AUTHENTICATION_METHOD, CONTENT_TYPE, MAXIMUM_PACKET_SIZE, REASON_STRING,
    RECEIVE_MAXIMUM, RESPONSE_TOPIC, SERVER_REFERENCE, SESSION_EXPIRY_INTERVAL,
    SUBSCRIPTION_IDENTIFIER, TOPIC_ALIAS,
};
use zzq_proto::reason::{self, v311};
use zzq_proto::{PacketType, Property, QoS, SubscriptionOptions, Version};

async fn broker() -> (Context, zzq_broker::Broker, String) {
    setup(1, BrokerOptions::default()).await
}

/// Opens a connection as `version`, sends `bytes`, and returns it.
async fn send_raw(uri: &str, version: Version, bytes: &[u8]) -> Raw {
    let mut raw = Raw::open(uri).await;
    raw.version = version;
    raw.send(bytes).await;
    raw
}

/// Connects, sends `bytes`, and expects a v5 DISCONNECT with one of
/// `reasons` (or a close on 3.1.1).
async fn after_connect(uri: &str, version: Version, bytes: &[u8], reasons: &[u8]) {
    let mut raw = Raw::connect(uri, "rule", version, 0).await;
    raw.send_lossy(bytes).await;
    raw.disconnected(reasons).await;
}

/// Expects a refused CONNECT: a close, possibly after a CONNACK error.
async fn connect_refused(raw: &mut Raw, case: &str) {
    match raw.try_next().await {
        None => {}
        Some(Got::ConnAck { code, .. }) => {
            assert_ne!(code, 0, "{case}: CONNECT accepted");
            raw.closed().await;
        }
        Some(got) => panic!("{case}: expected a refusal, got {got:?}"),
    }
}

/// A v5 SUBSCRIBE of `filter` as raw bytes.
fn subscribe_bytes(
    filter: &str,
    opts: SubscriptionOptions,
    properties: &[Property<'_>],
) -> Vec<u8> {
    let mut out = Vec::new();
    encode::subscribe(Version::V5, 1, properties, &[(filter, opts)], &mut out);
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn first_packet_other_than_connect_closes() {
    let (_ctx, _broker, uri) = broker().await;
    let firsts = [
        vec![0xC0, 0],
        vec![0x82, 6, 0, 1, 0, 1, b't', 0],
        publish_bytes(Version::V5, "t", b"x", QoS::AtMostOnce, 0, &[]),
    ];
    for first in firsts {
        send_raw(&uri, Version::V5, &first).await.closed().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bogus_first_packet_is_refused_from_its_header() {
    let opts = BrokerOptions {
        max_packet_size: 1 << 20,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    // A PUBLISH announcing 500 KB, and a CONNECT over max_packet_size. Both
    // fail on the fixed header, without waiting for the body.
    for header in [
        &[0x30, 0xA0, 0xC2, 0x1E][..],
        &[0x10, 0x80, 0x80, 0x80, 0x01],
    ] {
        let started = Instant::now();
        send_raw(&uri, Version::V5, header).await.closed().await;
        assert!(started.elapsed() < Duration::from_secs(2), "{header:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn second_connect_is_a_protocol_error() {
    let (_ctx, _broker, uri) = broker().await;
    for version in [Version::V5, Version::V311] {
        let again = connect_bytes(version, "twice", &[]);
        after_connect(&uri, version, &again, &[reason::PROTOCOL_ERROR]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn unsupported_protocol_gets_connack_0x01_or_close() {
    let (_ctx, _broker, uri) = broker().await;
    for level in [0, 2, 6, 7] {
        let mut bytes = connect_bytes(Version::V311, "level", &[]);
        bytes[8] = level;
        let mut raw = send_raw(&uri, Version::V311, &bytes).await;
        raw.refused(&[v311::UNACCEPTABLE_PROTOCOL_VERSION]).await;
    }
    // A wrong protocol name closes without CONNACK [MQTT-3.1.2-1].
    let mut bytes = connect_bytes(Version::V311, "name", &[]);
    bytes[7] = b'U';
    send_raw(&uri, Version::V311, &bytes)
        .await
        .refused(&[])
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_connect_flags_are_refused() {
    let (_ctx, _broker, uri) = broker().await;
    let will = WillFields {
        qos: QoS::AtLeastOnce,
        retain: false,
        properties: &[],
        topic: "will",
        payload: b"gone",
    };
    let fields = |version, will, password| ConnectFields {
        version,
        clean_start: true,
        keep_alive: 0,
        properties: &[],
        client_id: "flags",
        will,
        username: None,
        password,
    };
    let encoded = |c: &ConnectFields<'_>, flags: u8| {
        let mut out = Vec::new();
        encode::connect(c, &mut out);
        assert!(out[1] < 0x80, "one-byte length");
        out[9] |= flags;
        out
    };
    for version in [Version::V311, Version::V5] {
        let cases = [
            // Reserved flag [MQTT-3.1.2-3].
            encoded(&fields(version, None, None), 0x01),
            // Will QoS or Will Retain without the Will flag [MQTT-3.1.2-11, -13].
            encoded(&fields(version, None, None), 0x08),
            encoded(&fields(version, None, None), 0x20),
            // Will QoS 3 [MQTT-3.1.2-14].
            encoded(&fields(version, Some(will), None), 0x18),
        ];
        for (i, bytes) in cases.iter().enumerate() {
            let case = format!("{version:?} case {i}");
            connect_refused(&mut send_raw(&uri, version, bytes).await, &case).await;
        }
    }
    // 3.1.1 forbids a password without a user name [MQTT-3.1.2-22]; 5.0
    // allows it.
    let bytes = encoded(&fields(Version::V311, None, Some(&b"pw"[..])), 0);
    connect_refused(
        &mut send_raw(&uri, Version::V311, &bytes).await,
        "3.1.1 password",
    )
    .await;
    let bytes = encoded(&fields(Version::V5, None, Some(&b"pw"[..])), 0);
    let mut raw = send_raw(&uri, Version::V5, &bytes).await;
    assert!(matches!(raw.next().await, Got::ConnAck { code: 0, .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn zero_receive_maximum_or_packet_size_is_a_protocol_error() {
    let (_ctx, _broker, uri) = broker().await;
    for p in [
        Property::U16(RECEIVE_MAXIMUM, 0),
        Property::U32(MAXIMUM_PACKET_SIZE, 0),
    ] {
        let bytes = connect_bytes(Version::V5, "zero", &[p]);
        let mut raw = send_raw(&uri, Version::V5, &bytes).await;
        raw.refused(&[reason::PROTOCOL_ERROR]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_with_an_authentication_method_is_refused() {
    let (_ctx, _broker, uri) = broker().await;
    // zzq supports no enhanced authentication [MQTT-4.12.0-1].
    let bytes = connect_bytes(
        Version::V5,
        "auth",
        &[Property::Str(AUTHENTICATION_METHOD, "SCRAM-SHA-1")],
    );
    let mut raw = send_raw(&uri, Version::V5, &bytes).await;
    raw.refused(&[reason::BAD_AUTHENTICATION_METHOD]).await;
    // Authentication Data without a method is a protocol error.
    let bytes = connect_bytes(
        Version::V5,
        "auth",
        &[Property::Binary(AUTHENTICATION_DATA, b"x")],
    );
    let mut raw = send_raw(&uri, Version::V5, &bytes).await;
    raw.refused(&[reason::PROTOCOL_ERROR]).await;
    // So is AUTH on a connection that did not ask for it.
    after_connect(&uri, Version::V5, &[0xF0, 0], &[reason::PROTOCOL_ERROR]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn silent_or_partial_connect_is_closed_after_connect_timeout() {
    let opts = BrokerOptions {
        connect_timeout: Duration::from_millis(500),
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let half = connect_bytes(Version::V5, "half", &[]);
    for sent in [&[][..], &half[..6]] {
        let started = Instant::now();
        send_raw(&uri, Version::V5, sent).await.closed().await;
        let took = started.elapsed();
        assert!(
            took >= Duration::from_millis(400) && took < Duration::from_secs(3),
            "{took:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn huge_declared_length_is_refused_before_the_body() {
    let opts = BrokerOptions {
        max_packet_size: 1 << 20,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    for version in [Version::V5, Version::V311] {
        let started = Instant::now();
        // A PUBLISH header announcing 200 MB.
        let header = [0x30, 0x80, 0x84, 0xAF, 0x5F];
        after_connect(&uri, version, &header, &[reason::PACKET_TOO_LARGE]).await;
        assert!(started.elapsed() < Duration::from_secs(2));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn packets_pipelined_behind_connect_are_processed() {
    let (_ctx, _broker, uri) = broker().await;
    let mut bytes = connect_bytes(Version::V5, "pipe", &[]);
    let qos1 = SubscriptionOptions::from(QoS::AtLeastOnce);
    encode::subscribe(Version::V5, 1, &[], &[("pipe/t", qos1)], &mut bytes);
    let payload = vec![7u8; 100_000];
    bytes.extend(publish_bytes(
        Version::V5,
        "pipe/t",
        &payload,
        QoS::AtLeastOnce,
        2,
        &[],
    ));
    let mut raw = send_raw(&uri, Version::V5, &bytes).await;
    assert!(matches!(raw.next().await, Got::ConnAck { code: 0, .. }));
    assert!(matches!(raw.next().await, Got::SubAck { pkid: 1, .. }));
    let mut puback = false;
    let mut delivered = false;
    for _ in 0..2 {
        match raw.next().await {
            Got::Ack {
                kind: PacketType::PubAck,
                pkid: 2,
                ..
            } => puback = true,
            Got::Publish(p) if p.topic == "pipe/t" && p.payload == payload => delivered = true,
            got => panic!("unexpected {got:?}"),
        }
    }
    assert!(puback && delivered);
}

#[tokio::test(flavor = "multi_thread")]
async fn packet_dribbled_across_seconds_is_read_whole() {
    let (_ctx, _broker, uri) = broker().await;
    let gap = Duration::from_millis(1200);
    let mut raw = Raw::open(&uri).await;
    let connect = connect_bytes(Version::V5, "slow", &[]);
    for piece in connect.chunks(connect.len() / 3 + 1) {
        raw.send(piece).await;
        tokio::time::sleep(gap).await;
    }
    assert!(matches!(raw.next().await, Got::ConnAck { code: 0, .. }));
    raw.subscribe("slow/t", QoS::AtMostOnce.into()).await;
    let publish = publish_bytes(Version::V5, "slow/t", b"dribbled", QoS::AtMostOnce, 0, &[]);
    for piece in publish.chunks(publish.len() / 3 + 1) {
        raw.send(piece).await;
        tokio::time::sleep(gap).await;
    }
    assert_eq!(raw.publish().await.payload, b"dribbled");
}

#[tokio::test(flavor = "multi_thread")]
async fn server_only_packets_from_a_client_close_only_that_connection() {
    let (ctx, _broker, uri) = broker().await;
    let mut witness = client(&ctx, &uri, "witness", Version::V5).await;
    witness.subscribe("w", QoS::AtMostOnce).await.unwrap();
    let v5 = [
        vec![0x20, 3, 0, 0, 0],
        vec![0x90, 4, 0, 1, 0, 0],
        vec![0xB0, 4, 0, 1, 0, 0],
        vec![0xD0, 0],
        vec![0x00, 0],
    ];
    for packet in v5 {
        let reasons = [reason::MALFORMED_PACKET, reason::PROTOCOL_ERROR];
        after_connect(&uri, Version::V5, &packet, &reasons).await;
    }
    // 3.1.1 has no AUTH.
    for packet in [
        vec![0x20, 2, 0, 0],
        vec![0xD0, 0],
        vec![0xF0, 0],
        vec![0x00, 0],
    ] {
        after_connect(&uri, Version::V311, &packet, &[]).await;
    }
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish("w", "still here", QoS::AtMostOnce)
        .await
        .unwrap();
    assert_eq!(next(&mut witness).await.payload, "still here");
}

#[tokio::test(flavor = "multi_thread")]
async fn fixed_header_reserved_flags_are_malformed() {
    let (_ctx, _broker, uri) = broker().await;
    let cases = [
        // SUBSCRIBE, UNSUBSCRIBE and PUBREL need flags 0b0010.
        vec![0x80, 7, 0, 1, 0, 0, 1, b't', 0],
        vec![0xA0, 6, 0, 1, 0, 0, 1, b't'],
        vec![0x60, 2, 0, 1],
        // PINGREQ and DISCONNECT need flags 0.
        vec![0xC1, 0],
        vec![0xE1, 0],
        // PUBLISH with QoS 3 [MQTT-3.3.1-4].
        vec![0x36, 7, 0, 1, b't', 0, 1, 0, b'x'],
    ];
    for packet in cases {
        after_connect(&uri, Version::V5, &packet, &[reason::MALFORMED_PACKET]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_properties_are_a_protocol_error() {
    let (_ctx, _broker, uri) = broker().await;
    let twice = props(&[
        Property::Str(CONTENT_TYPE, "a"),
        Property::Str(CONTENT_TYPE, "b"),
    ]);
    let publish = publish_bytes(Version::V5, "t", b"x", QoS::AtMostOnce, 0, &twice);
    after_connect(&uri, Version::V5, &publish, &[reason::PROTOCOL_ERROR]).await;
    let sub = subscribe_bytes(
        "t",
        QoS::AtMostOnce.into(),
        &[
            Property::VarInt(SUBSCRIPTION_IDENTIFIER, 1),
            Property::VarInt(SUBSCRIPTION_IDENTIFIER, 2),
        ],
    );
    after_connect(&uri, Version::V5, &sub, &[reason::PROTOCOL_ERROR]).await;
    let connect = connect_bytes(
        Version::V5,
        "dup",
        &[
            Property::U32(SESSION_EXPIRY_INTERVAL, 1),
            Property::U32(SESSION_EXPIRY_INTERVAL, 2),
        ],
    );
    let mut raw = send_raw(&uri, Version::V5, &connect).await;
    raw.refused(&[reason::PROTOCOL_ERROR]).await;
    // User Properties may repeat, and keep their order.
    let mut raw = Raw::connect(&uri, "users", Version::V5, 0).await;
    raw.subscribe("u", QoS::AtMostOnce.into()).await;
    let users = props(&[Property::User("k", "1"), Property::User("k", "2")]);
    raw.pub_props("u", b"x", QoS::AtMostOnce, 0, &users).await;
    let got = raw.publish().await;
    let pairs: Vec<_> = got
        .properties()
        .iter()
        .filter_map(|p| match p {
            Property::User(k, v) => Some((k.to_owned(), v.to_owned())),
            _ => None,
        })
        .collect();
    assert_eq!(pairs, [("k".into(), "1".into()), ("k".into(), "2".into())]);
}

#[tokio::test(flavor = "multi_thread")]
async fn properties_not_allowed_for_the_packet_are_malformed() {
    let (_ctx, _broker, uri) = broker().await;
    let server_ref = props(&[Property::Str(SERVER_REFERENCE, "elsewhere")]);
    let publish = publish_bytes(Version::V5, "t", b"x", QoS::AtMostOnce, 0, &server_ref);
    after_connect(&uri, Version::V5, &publish, &[reason::MALFORMED_PACKET]).await;
    let sub = subscribe_bytes(
        "t",
        QoS::AtMostOnce.into(),
        &[Property::U32(SESSION_EXPIRY_INTERVAL, 5)],
    );
    after_connect(&uri, Version::V5, &sub, &[reason::MALFORMED_PACKET]).await;
    let connect = connect_bytes(Version::V5, "alias", &[Property::U16(TOPIC_ALIAS, 1)]);
    let mut raw = send_raw(&uri, Version::V5, &connect).await;
    raw.refused(&[reason::MALFORMED_PACKET]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_strings_and_wildcard_topics_disconnect() {
    let (_ctx, _broker, uri) = broker().await;
    let bad_name = [
        reason::MALFORMED_PACKET,
        reason::PROTOCOL_ERROR,
        reason::TOPIC_NAME_INVALID,
    ];
    // Wildcards in a topic name [MQTT-3.3.2-2].
    for topic in ["a/+", "a/#", "#"] {
        let publish = publish_bytes(Version::V5, topic, b"x", QoS::AtMostOnce, 0, &[]);
        after_connect(&uri, Version::V5, &publish, &bad_name).await;
    }
    // U+0000 in a topic name or filter [MQTT-1.5.4-2].
    let publish = publish_bytes(Version::V5, "a\0b", b"x", QoS::AtMostOnce, 0, &[]);
    after_connect(&uri, Version::V5, &publish, &[reason::MALFORMED_PACKET]).await;
    let sub = subscribe_bytes("a\0b", QoS::AtMostOnce.into(), &[]);
    after_connect(&uri, Version::V5, &sub, &[reason::MALFORMED_PACKET]).await;
    // A UTF-16 surrogate, U+D800, in a topic name [MQTT-1.5.4-1].
    let surrogate = [0x30, 7, 0, 3, 0xED, 0xA0, 0x80, 0, b'x'];
    after_connect(&uri, Version::V5, &surrogate, &[reason::MALFORMED_PACKET]).await;
    // An empty topic name needs a topic alias.
    let empty = publish_bytes(Version::V5, "", b"x", QoS::AtMostOnce, 0, &[]);
    after_connect(&uri, Version::V5, &empty, &[reason::PROTOCOL_ERROR]).await;
    // Characters outside the Basic Multilingual Plane are fine.
    let mut raw = Raw::connect(&uri, "astral", Version::V5, 0).await;
    raw.subscribe("\u{2A6D4}/#", QoS::AtMostOnce.into()).await;
    raw.pub_("\u{2A6D4}/x", b"ok", QoS::AtMostOnce, 0).await;
    assert_eq!(raw.publish().await.topic, "\u{2A6D4}/x");
}

#[tokio::test(flavor = "multi_thread")]
async fn response_topic_with_wildcards_is_a_protocol_error() {
    let (_ctx, _broker, uri) = broker().await;
    // [MQTT-3.3.2-14]
    let wild = props(&[Property::Str(RESPONSE_TOPIC, "reply/#")]);
    let publish = publish_bytes(Version::V5, "t", b"x", QoS::AtMostOnce, 0, &wild);
    let reasons = [reason::PROTOCOL_ERROR, reason::TOPIC_NAME_INVALID];
    after_connect(&uri, Version::V5, &publish, &reasons).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_subscription_option_errors() {
    let (_ctx, _broker, uri) = broker().await;
    // No Local on a shared subscription [MQTT-3.8.3-4].
    let opts = SubscriptionOptions {
        no_local: true,
        ..QoS::AtMostOnce.into()
    };
    let sub = subscribe_bytes("$share/g/t", opts, &[]);
    after_connect(&uri, Version::V5, &sub, &[reason::PROTOCOL_ERROR]).await;
    // Share names must be non-empty, without wildcards, and followed by a
    // filter (4.8.2). The broker refuses the filter or the packet.
    for filter in [
        "$share/g",
        "$share/g/",
        "$share//t",
        "$share/g+/t",
        "$share/g#/t",
    ] {
        let mut raw = Raw::connect(&uri, "share", Version::V5, 0).await;
        raw.send(&subscribe_bytes(filter, QoS::AtMostOnce.into(), &[]))
            .await;
        match raw.try_next().await {
            Some(Got::SubAck { codes, .. }) => {
                assert_eq!(codes, [reason::TOPIC_FILTER_INVALID], "{filter}");
            }
            Some(Got::Disconnect { reason }) => {
                assert!(
                    [
                        reason::MALFORMED_PACKET,
                        reason::PROTOCOL_ERROR,
                        reason::TOPIC_FILTER_INVALID
                    ]
                    .contains(&reason),
                    "{filter}: {reason:#04x}"
                );
            }
            got => panic!("{filter}: {got:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_subscribe_and_packet_id_zero_are_malformed() {
    let (_ctx, _broker, uri) = broker().await;
    // SUBSCRIBE and UNSUBSCRIBE without filters [MQTT-3.8.3-2, 3.10.3-2].
    for packet in [vec![0x82, 3, 0, 1, 0], vec![0xA2, 3, 0, 1, 0]] {
        after_connect(&uri, Version::V5, &packet, &[reason::PROTOCOL_ERROR]).await;
    }
    // Packet ID 0 [MQTT-2.2.1-3].
    let bad = [reason::MALFORMED_PACKET, reason::PROTOCOL_ERROR];
    let sub = vec![0x82, 7, 0, 0, 0, 0, 1, b't', 0];
    after_connect(&uri, Version::V5, &sub, &bad).await;
    let unsub = vec![0xA2, 6, 0, 0, 0, 0, 1, b't'];
    after_connect(&uri, Version::V5, &unsub, &bad).await;
    let publish = publish_bytes(Version::V5, "t", b"x", QoS::AtLeastOnce, 0, &[]);
    after_connect(&uri, Version::V5, &publish, &bad).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_alias_above_the_maximum_or_after_reconnect_is_invalid() {
    let opts = BrokerOptions {
        topic_alias_maximum: 4,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    // [MQTT-3.3.2-9]
    let alias = |n| props(&[Property::U16(TOPIC_ALIAS, n)]);
    let publish = publish_bytes(Version::V5, "t", b"x", QoS::AtMostOnce, 0, &alias(5));
    after_connect(&uri, Version::V5, &publish, &[reason::TOPIC_ALIAS_INVALID]).await;
    // Aliases end with the connection [MQTT-3.3.2-7].
    let mut raw = Raw::connect(&uri, "alias", Version::V5, 0).await;
    raw.subscribe("t", QoS::AtMostOnce.into()).await;
    raw.pub_props("t", b"set", QoS::AtMostOnce, 0, &alias(1))
        .await;
    raw.pub_props("", b"used", QoS::AtMostOnce, 0, &alias(1))
        .await;
    assert_eq!(raw.publish().await.payload, b"set");
    assert_eq!(raw.publish().await.payload, b"used");
    raw.disconnect(0, &[]).await;
    raw.closed().await;
    let publish = publish_bytes(Version::V5, "", b"stale", QoS::AtMostOnce, 0, &alias(1));
    after_connect(&uri, Version::V5, &publish, &[reason::PROTOCOL_ERROR]).await;
}

/// A raw v5 subscriber with Receive Maximum 1, so each delivery waits for
/// the ack of the one before.
async fn one_at_a_time(uri: &str, id: &str, filter: &str, qos: QoS) -> Raw {
    let c = ConnectFields {
        version: Version::V5,
        clean_start: true,
        keep_alive: 0,
        properties: &[Property::U16(RECEIVE_MAXIMUM, 1)],
        client_id: id,
        will: None,
        username: None,
        password: None,
    };
    let (mut raw, code) = Raw::connect_with(uri, &c).await;
    assert_eq!(code, 0);
    raw.subscribe(filter, qos.into()).await;
    raw
}

/// An ack of `kind` for `pkid` in each valid v5 form: short, with a reason
/// code, with an empty and a filled property block.
fn ack_forms(kind: PacketType, pkid: u16) -> [Vec<u8>; 4] {
    let byte1 = kind.header_byte();
    let [hi, lo] = pkid.to_be_bytes();
    let reason_string = props(&[Property::Str(REASON_STRING, "ok")]);
    let mut long = vec![byte1, 4 + reason_string.len() as u8, hi, lo, 0];
    long.push(reason_string.len() as u8);
    long.extend(&reason_string);
    [
        vec![byte1, 2, hi, lo],
        vec![byte1, 3, hi, lo, 0],
        vec![byte1, 4, hi, lo, 0, 0],
        long,
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn acks_of_every_length_are_accepted() {
    let (ctx, _broker, uri) = broker().await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let mut raw = one_at_a_time(&uri, "acks", "acks/#", QoS::ExactlyOnce).await;
    for i in 0..4 {
        publ.publish("acks/1", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    for i in 0..4 {
        let p = raw.publish().await;
        assert_eq!(p.payload, format!("{i}").as_bytes());
        raw.send(&ack_forms(PacketType::PubAck, p.pkid)[i]).await;
    }
    for i in 0..4 {
        publ.publish("acks/2", format!("{i}"), QoS::ExactlyOnce)
            .await
            .unwrap();
    }
    for i in 0..4 {
        let p = raw.publish().await;
        assert_eq!(p.payload, format!("{i}").as_bytes());
        raw.send(&ack_forms(PacketType::PubRec, p.pkid)[i]).await;
        assert_eq!(
            raw.next().await,
            Got::Ack {
                kind: PacketType::PubRel,
                pkid: p.pkid,
                reason: 0
            }
        );
        raw.send(&ack_forms(PacketType::PubComp, p.pkid)[i]).await;
    }
    // An unknown PUBREL gets PUBCOMP "packet identifier not found"; an
    // unknown PUBACK is ignored.
    raw.ack(PacketType::PubRel, 999).await;
    assert_eq!(
        raw.next().await,
        Got::Ack {
            kind: PacketType::PubComp,
            pkid: 999,
            reason: reason::PACKET_IDENTIFIER_NOT_FOUND
        }
    );
    raw.ack(PacketType::PubAck, 998).await;
    publ.publish("acks/3", "after", QoS::AtLeastOnce)
        .await
        .unwrap();
    assert_eq!(raw.publish().await.payload, b"after");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_ack_type_is_a_protocol_error() {
    let (ctx, _broker, uri) = broker().await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let persistent = ConnectFields {
        version: Version::V5,
        clean_start: false,
        keep_alive: 0,
        properties: &[Property::U32(SESSION_EXPIRY_INTERVAL, 60)],
        client_id: "wrong",
        will: None,
        username: None,
        password: None,
    };
    let (mut raw, _) = Raw::connect_with(&uri, &persistent).await;
    raw.subscribe("wrong/#", QoS::ExactlyOnce.into()).await;
    // PUBACK for a QoS 2 delivery.
    publ.publish("wrong/2", "two", QoS::ExactlyOnce)
        .await
        .unwrap();
    let p = raw.publish().await;
    raw.ack(PacketType::PubAck, p.pkid).await;
    raw.disconnected(&[reason::PROTOCOL_ERROR]).await;
    // The delivery is still owed, and comes again on resume.
    let (mut raw, _) = Raw::connect_with(&uri, &persistent).await;
    let again = raw.publish().await;
    assert_eq!(
        (again.pkid, again.dup, &again.payload[..]),
        (p.pkid, true, &b"two"[..])
    );
    raw.ack(PacketType::PubRec, p.pkid).await;
    assert!(matches!(
        raw.next().await,
        Got::Ack {
            kind: PacketType::PubRel,
            ..
        }
    ));
    raw.ack(PacketType::PubComp, p.pkid).await;
    // PUBREC for a QoS 1 delivery.
    raw.subscribe("one/#", QoS::AtLeastOnce.into()).await;
    publ.publish("one/1", "one", QoS::AtLeastOnce)
        .await
        .unwrap();
    let p = raw.publish().await;
    raw.ack(PacketType::PubRec, p.pkid).await;
    raw.disconnected(&[reason::PROTOCOL_ERROR]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pubrec_error_code_ends_the_qos2_flow() {
    let (ctx, _broker, uri) = broker().await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    let mut raw = one_at_a_time(&uri, "rec", "rec", QoS::ExactlyOnce).await;
    publ.publish("rec", "refused", QoS::ExactlyOnce)
        .await
        .unwrap();
    publ.publish("rec", "next", QoS::ExactlyOnce).await.unwrap();
    let p = raw.publish().await;
    let [hi, lo] = p.pkid.to_be_bytes();
    raw.send(&[0x50, 3, hi, lo, reason::UNSPECIFIED_ERROR])
        .await;
    // No PUBREL, and the window slot is free for the next message.
    let next = raw.publish().await;
    assert_eq!(next.payload, b"next");
}

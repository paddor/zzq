//! Seed inputs for each target, built with the real encoders, so no binary
//! files are checked in.

use std::time::{Duration, UNIX_EPOCH};

use zzq_broker::{FilePersistence, Persistence, Retained};
use zzq_proto::encode::{self, AckFields, ConnectFields, PublishHeader, WillFields};
use zzq_proto::property::{self, Property};
use zzq_proto::sn::{self, Flags, SnQoS, Topic};
use zzq_proto::{PacketType, QoS, SubscriptionOptions, Version};

/// Seeds for the target `name`.
#[must_use]
pub fn for_target(name: &str) -> Vec<Vec<u8>> {
    match name {
        "mqtt" => mqtt(),
        "mqtt_sn" => mqtt_sn(),
        "websocket" => websocket(),
        "retained_log" => retained_log(),
        "endpoint" => text(&[
            "tcp://localhost",
            "tls://[::1]:8883",
            "ws://h:9/mqtt?x",
            "wss://h",
            "quic://h:1",
            "udp://0.0.0.0",
            "ipc://@x",
            "inproc://n",
        ]),
        "topics" => text(&["a/+\na/b", "#\n$SYS/x", "a/#\na", "+/+\n/a", "a//+\na//c"]),
        _ => Vec::new(),
    }
}

fn text(items: &[&str]) -> Vec<Vec<u8>> {
    items.iter().map(|s| s.as_bytes().to_vec()).collect()
}

fn mqtt() -> Vec<Vec<u8>> {
    let props = [
        Property::U32(property::MESSAGE_EXPIRY_INTERVAL, 9),
        Property::User("k", "v"),
    ];
    let mut raw = Vec::new();
    property::encode_all(&props, &mut raw);
    let mut seeds = Vec::new();
    let mut add = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = Vec::new();
        f(&mut b);
        seeds.push(b);
    };
    for version in [Version::V311, Version::V5] {
        add(&|b| {
            let c = ConnectFields {
                version,
                clean_start: false,
                keep_alive: 30,
                properties: &props,
                client_id: "fuzz",
                will: Some(WillFields {
                    qos: QoS::AtLeastOnce,
                    retain: true,
                    properties: &props,
                    topic: "w",
                    payload: b"x",
                }),
                username: Some("u"),
                password: Some(b"p"),
            };
            encode::connect(&c, b);
        });
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            add(&|b| {
                let h = PublishHeader {
                    version,
                    dup: false,
                    qos,
                    retain: true,
                    topic: "a/b",
                    pkid: 3,
                    properties: &raw,
                };
                encode::publish(&h, b"payload", b);
            });
        }
        add(&|b| {
            let opts = SubscriptionOptions::from(QoS::ExactlyOnce);
            encode::subscribe(version, 4, &[], &[("a/#", opts), ("$share/g/b", opts)], b);
        });
        add(&|b| encode::unsubscribe(version, 5, &[], &["a/#"], b));
        add(&|b| encode::ack(&AckFields::new(PacketType::PubRel, 6), version, b));
        add(&|b| encode::reason_only(PacketType::Disconnect, version, 0x04, &raw, b));
        add(&|b| encode::pingreq(b));
    }
    let stream = seeds.concat();
    seeds.push(stream);
    seeds
}

fn mqtt_sn() -> Vec<Vec<u8>> {
    let flags = Flags {
        qos: SnQoS::AtLeastOnce,
        retain: true,
        will: true,
        clean_session: true,
        ..Flags::default()
    };
    let packets = [
        sn::Packet::Connect {
            flags,
            duration: 30,
            client_id: "sn",
        },
        sn::Packet::Register {
            topic_id: 0,
            msg_id: 1,
            topic: "a/b",
        },
        sn::Packet::Publish {
            flags,
            topic: Topic::Id(7),
            msg_id: 2,
            data: b"x",
        },
        sn::Packet::Publish {
            flags,
            topic: Topic::Short(*b"ab"),
            msg_id: 3,
            data: b"y",
        },
        sn::Packet::Subscribe {
            flags,
            msg_id: 4,
            topic: Topic::Name("a/#"),
        },
        sn::Packet::WillTopic {
            flags: Some(flags),
            topic: "w",
        },
        sn::Packet::PingReq {
            client_id: Some("sn"),
        },
        sn::Packet::Disconnect { duration: Some(60) },
        sn::Packet::Encapsulated {
            ctrl: 1,
            node_id: b"node",
            message: &[3, 0x18, 0],
        },
    ];
    packets
        .iter()
        .map(|p| {
            let mut b = Vec::new();
            sn::encode(p, &mut b);
            b
        })
        .collect()
}

fn websocket() -> Vec<Vec<u8>> {
    let upgrade = "GET /mqtt HTTP/1.1\r\nHost: h\r\nUpgrade: websocket\r\n\
                   Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                   Sec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n";
    let mask = [1u8, 2, 3, 4];
    let mut frame = vec![0x82, 0x80 | 4];
    frame.extend_from_slice(&mask);
    frame.extend(b"ping".iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
    let mut long = vec![0x82, 0x80 | 0x7e, 0, 200];
    long.extend_from_slice(&mask);
    long.extend(std::iter::repeat_n(0, 200));
    [frame, long]
        .into_iter()
        .map(|f| [upgrade.as_bytes(), &f].concat())
        .collect()
}

fn retained_log() -> Vec<Vec<u8>> {
    let path = std::env::temp_dir().join(format!("zzq-fuzz-seed-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);
    {
        let Ok(p) = FilePersistence::open(&path) else {
            return Vec::new();
        };
        for topic in ["a/b", "c"] {
            p.set_retained(&Retained {
                topic: topic.to_owned(),
                payload: "v".into(),
                qos: QoS::AtLeastOnce,
                properties: vec![1, 1].into(),
                expires_at: Some(UNIX_EPOCH + Duration::from_secs(4_000_000_000)),
            });
        }
        p.remove_retained("c");
        p.flush();
    }
    let log = std::fs::read(&path).unwrap_or_default();
    let _ = std::fs::remove_file(&path);
    vec![log]
}

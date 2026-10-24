//! Decoding arbitrary bytes never panics.

// The samples are encoded into `Vec`, a `Sink` only with `alloc`.
#![cfg(feature = "alloc")]

use zzq_proto::encode::{self, ConnectFields, PublishHeader, WillFields};
use zzq_proto::property::{self, Property};
use zzq_proto::{Packet, QoS, SubscriptionOptions, Version, decode};

struct Rng(u64);

impl Rng {
    const fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    const fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn samples() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut b = Vec::new();
    let props = [
        Property::User("k", "v"),
        Property::U32(property::MESSAGE_EXPIRY_INTERVAL, 9),
    ];
    let mut raw = Vec::new();
    property::encode_all(&props, &mut raw);
    for version in [Version::V311, Version::V5] {
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            let h = PublishHeader {
                version,
                dup: false,
                qos,
                retain: true,
                topic: "a/b/c",
                pkid: 3,
                properties: &raw,
            };
            encode::publish(&h, b"payload", &mut b);
            out.push(std::mem::take(&mut b));
        }
        let c = ConnectFields {
            version,
            clean_start: false,
            keep_alive: 10,
            properties: &props,
            client_id: "fuzz",
            will: Some(WillFields {
                qos: QoS::AtLeastOnce,
                retain: false,
                properties: &props,
                topic: "w",
                payload: b"x",
            }),
            username: Some("u"),
            password: Some(b"p"),
        };
        encode::connect(&c, &mut b);
        out.push(std::mem::take(&mut b));
        encode::subscribe(
            version,
            1,
            &props,
            &[
                ("a/+", SubscriptionOptions::default()),
                ("#", SubscriptionOptions::from(QoS::ExactlyOnce)),
            ],
            &mut b,
        );
        out.push(std::mem::take(&mut b));
        encode::unsubscribe(version, 2, &[], &["a/#"], &mut b);
        out.push(std::mem::take(&mut b));
    }
    out
}

fn check(buf: &[u8]) {
    for version in [Version::V311, Version::V5] {
        if let Ok(Some((packet, n))) = decode(buf, version, 1 << 20) {
            assert!(n <= buf.len());
            if let Packet::Subscribe(s) = packet {
                for (f, _) in s.filters() {
                    assert!(!f.is_empty());
                }
            }
            if let Packet::Publish(p) = packet {
                let _ = p.properties.iter().count();
            }
        }
    }
}

#[test]
fn random_bytes() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut buf = Vec::new();
    for _ in 0..200_000 {
        buf.clear();
        let len = rng.below(64);
        buf.extend((0..len).map(|_| rng.next() as u8));
        if !buf.is_empty() {
            buf[0] = (buf[0] & 0xf0) | (rng.below(16) as u8);
        }
        check(&buf);
    }
}

#[test]
fn mutated_packets() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    let base = samples();
    for _ in 0..200_000 {
        let mut buf = base[rng.below(base.len())].clone();
        for _ in 0..=rng.below(4) {
            match rng.below(3) {
                0 => {
                    let i = rng.below(buf.len());
                    buf[i] = rng.next() as u8;
                }
                1 => buf.truncate(rng.below(buf.len() + 1)),
                _ => {
                    let i = rng.below(buf.len() + 1);
                    buf.insert(i, rng.next() as u8);
                }
            }
            if buf.is_empty() {
                break;
            }
        }
        check(&buf);
    }
}

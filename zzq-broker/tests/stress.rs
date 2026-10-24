//! Random traffic from misbehaving clients must not panic the broker or
//! leave it unusable. `ZZQ_STRESS_SECS` sets the duration (default 3).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use zzq_broker::{Broker, BrokerOptions, Config, Context};
use zzq_client::{Client, ClientOptions};
use zzq_proto::encode::{self, AckFields, ConnectFields, PublishHeader, WillFields};
use zzq_proto::{PacketType, QoS, SubscriptionOptions, Version};

/// xorshift64*, so failures replay from the seed.
struct Rng(u64);

impl Rng {
    const fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    const fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    const fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

const TOPICS: &[&str] = &[
    "a", "a/b", "a/b/c", "x/y", "$SYS/x", "s/1", "s/2", "/", "a//b",
];
const FILTERS: &[&str] = &[
    "#",
    "a/#",
    "a/+",
    "+/+",
    "s/+",
    "$share/g/s/#",
    "$share/h/a/+",
    "$SYS/#",
    "x/y",
];
const VERSIONS: &[Version] = &[Version::V5, Version::V311, Version::V31];

const fn qos(r: &mut Rng) -> QoS {
    *r.pick(&[QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce])
}

fn connect(r: &mut Rng, out: &mut Vec<u8>) -> Version {
    let version = *r.pick(VERSIONS);
    let id = format!("c{}", r.below(6));
    let will = (r.below(3) == 0).then(|| WillFields {
        qos: QoS::AtLeastOnce,
        retain: r.below(2) == 0,
        properties: &[],
        topic: "a/will",
        payload: b"bye",
    });
    let expiry = [zzq_proto::Property::U32(
        zzq_proto::property::SESSION_EXPIRY_INTERVAL,
        30,
    )];
    let c = ConnectFields {
        version,
        clean_start: r.below(2) == 0,
        keep_alive: [0, 1, 30][r.below(3)],
        properties: if version.is_v5() && r.below(2) == 0 {
            &expiry
        } else {
            &[]
        },
        client_id: &id,
        will,
        username: None,
        password: None,
    };
    encode::connect(&c, out);
    version
}

/// One random packet, valid or not, for a connected client.
fn packet(r: &mut Rng, v: Version, out: &mut Vec<u8>) {
    let pkid = 1 + r.below(8) as u16;
    match r.below(12) {
        0..=3 => {
            let q = qos(r);
            let h = PublishHeader {
                version: v,
                dup: r.below(4) == 0,
                qos: q,
                retain: r.below(4) == 0,
                topic: r.pick::<&str>(TOPICS),
                pkid: if q == QoS::AtMostOnce { 0 } else { pkid },
                properties: &[],
            };
            let payload = vec![b'p'; [0, 1, 100, 5000][r.below(4)]];
            encode::publish(&h, &payload, out);
        }
        4 | 5 => {
            let opts = SubscriptionOptions::qos(qos(r));
            encode::subscribe(v, pkid, &[], &[(*r.pick(FILTERS), opts)], out);
        }
        6 => encode::unsubscribe(v, pkid, &[], &[*r.pick(FILTERS)], out),
        7 => {
            let kind = *r.pick(&[
                PacketType::PubAck,
                PacketType::PubRec,
                PacketType::PubRel,
                PacketType::PubComp,
            ]);
            encode::ack(&AckFields::new(kind, pkid), v, out);
        }
        8 => encode::pingreq(out),
        9 => {
            // Garbage.
            for _ in 0..r.below(16) {
                out.push(r.next() as u8);
            }
        }
        10 => encode::reason_only(PacketType::Disconnect, v, 0, &[], out),
        _ => {
            connect(r, out);
        }
    }
}

async fn chaos(addr: String, seed: u64, until: Instant) {
    let mut r = Rng(seed | 1);
    while Instant::now() < until {
        let Ok(mut s) = TcpStream::connect(&addr).await else {
            continue;
        };
        let mut out = Vec::new();
        let v = connect(&mut r, &mut out);
        for _ in 0..r.below(40) {
            packet(&mut r, v, &mut out);
        }
        // Split writes at random points to exercise partial reads.
        while !out.is_empty() {
            let n = (1 + r.below(64)).min(out.len());
            if s.write_all(&out[..n]).await.is_err() {
                break;
            }
            out.drain(..n);
        }
        let mut sink = vec![0u8; 64 << 10];
        let linger = Duration::from_millis(r.below(20) as u64);
        let _ = timeout(linger, async {
            while matches!(s.read(&mut sink).await, Ok(n) if n > 0) {}
        })
        .await;
    }
}

/// From `ZZQ_STRESS_SECS`, default 3 s.
fn duration() -> Duration {
    let secs = std::env::var("ZZQ_STRESS_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    Duration::from_secs(secs)
}

#[tokio::test(flavor = "multi_thread")]
async fn random_traffic_keeps_the_broker_alive() {
    let panics = Arc::new(AtomicUsize::new(0));
    let seen = panics.clone();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        seen.fetch_add(1, Ordering::Relaxed);
        previous(info);
    }));
    let ctx = Context::with_config(&Config {
        io_threads: 3,
        ..Config::default()
    })
    .unwrap();
    let opts = BrokerOptions {
        max_queued: 50,
        receive_maximum: 4,
        max_packet_size: 4096,
        connect_timeout: Duration::from_millis(300),
        ..BrokerOptions::default()
    };
    let broker = Broker::new(&ctx, opts).await.unwrap();
    let uri = broker.bind("tcp://127.0.0.1:0").await.unwrap().to_string();
    let addr = uri.trim_start_matches("tcp://").to_owned();
    let until = Instant::now() + duration();
    let tasks: Vec<_> = (0..12u64)
        .map(|i| {
            tokio::spawn(chaos(
                addr.clone(),
                0x9E37_79B9_7F4A_7C15 ^ (i * 7919),
                until,
            ))
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(panics.load(Ordering::Relaxed), 0, "broker panicked");
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("after"))
        .await
        .unwrap();
    c.subscribe("after/#", QoS::AtLeastOnce).await.unwrap();
    c.publish("after/x", "ok", QoS::AtLeastOnce, false)
        .await
        .unwrap();
    let m = timeout(Duration::from_secs(5), c.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.payload, "ok");
    c.disconnect().await;
    let mut connections = u64::MAX;
    for _ in 0..100 {
        connections = broker.stats().await.unwrap().connections;
        if connections == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(connections, 0, "connections left after all clients closed");
}

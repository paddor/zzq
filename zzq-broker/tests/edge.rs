//! Broker edge cases: limits, timers, QoS 2 and retain options.

#![expect(clippy::unwrap_used, clippy::expect_used, reason = "tests fail loudly")]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use zzq_broker::{Broker, BrokerOptions, Config, Context};
use zzq_client::{Client, ClientOptions, LastWill, Message};
use zzq_proto::encode::{self, AckFields, ConnectFields, PublishHeader};
use zzq_proto::{PacketType, QoS, ReasonCode, SubscriptionOptions, Version, decode};

const WAIT: Duration = Duration::from_secs(5);

async fn setup(lanes: usize, opts: BrokerOptions) -> (Context, Broker, String) {
    let ctx = Context::with_config(&Config {
        io_threads: lanes,
        ..Config::default()
    })
    .unwrap();
    let broker = Broker::new(&ctx, opts).await.unwrap();
    let uri = broker.bind("tcp://127.0.0.1:0").await.unwrap().to_string();
    (ctx, broker, uri)
}

async fn next(c: &mut Client) -> Message {
    timeout(WAIT, c.recv())
        .await
        .expect("timed out")
        .expect("closed")
}

async fn nothing(c: &mut Client) {
    assert!(
        timeout(Duration::from_millis(300), c.recv()).await.is_err(),
        "unexpected message"
    );
}

/// A raw MQTT connection for packets the client never sends.
struct Raw {
    s: TcpStream,
    buf: Vec<u8>,
    version: Version,
}

impl Raw {
    async fn connect(uri: &str, id: &str, version: Version, keep_alive: u16) -> Raw {
        Raw::connect_ack(uri, id, version, keep_alive).await.0
    }

    /// Connects and also returns the CONNACK packet.
    async fn connect_ack(uri: &str, id: &str, version: Version, keep_alive: u16) -> (Raw, Vec<u8>) {
        let c = ConnectFields {
            version,
            clean_start: true,
            keep_alive,
            properties: &[],
            client_id: id,
            will: None,
            username: None,
            password: None,
        };
        Raw::connect_with(uri, &c).await
    }

    /// Sends `c` and returns the connection and its CONNACK, which must be
    /// the first packet.
    async fn connect_with(uri: &str, c: &ConnectFields<'_>) -> (Raw, Vec<u8>) {
        let addr = uri.trim_start_matches("tcp://");
        let s = TcpStream::connect(addr).await.unwrap();
        let mut raw = Raw {
            s,
            buf: Vec::new(),
            version: c.version,
        };
        let mut out = Vec::new();
        encode::connect(c, &mut out);
        raw.s.write_all(&out).await.unwrap();
        let (byte1, connack) = raw.packet().await.expect("CONNACK");
        assert_eq!(byte1 >> 4, 2, "first packet must be CONNACK");
        (raw, connack)
    }

    async fn subscribe(&mut self, filter: &str, qos: QoS) {
        let mut out = Vec::new();
        let opts = SubscriptionOptions::from(qos);
        encode::subscribe(self.version, 1, &[], &[(filter, opts)], &mut out);
        self.write(&out).await;
        let (byte1, _) = self.packet().await.expect("SUBACK");
        assert_eq!(byte1 >> 4, 9);
    }

    async fn write(&mut self, bytes: &[u8]) {
        self.s.write_all(bytes).await.unwrap();
    }

    /// Next packet as (first byte, whole packet), `None` once closed.
    async fn packet(&mut self) -> Option<(u8, Vec<u8>)> {
        loop {
            if let Ok(Some((_, n))) = decode(&self.buf, self.version, usize::MAX) {
                let p: Vec<u8> = self.buf.drain(..n).collect();
                return Some((p[0], p));
            }
            let mut chunk = [0u8; 4096];
            let n = timeout(WAIT, self.s.read(&mut chunk))
                .await
                .expect("read timed out")
                .ok()?;
            if n == 0 {
                return None;
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_alive_timeout_closes() {
    let (_ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut raw = Raw::connect(&uri, "idle", Version::V5, 1).await;
    let started = std::time::Instant::now();
    let p = raw.packet().await;
    if let Some((byte1, packet)) = p {
        assert_eq!(byte1 >> 4, 14, "expected DISCONNECT");
        assert_eq!(packet[2], zzq_proto::reason::KEEP_ALIVE_TIMEOUT);
        assert!(raw.packet().await.is_none());
    }
    assert!(started.elapsed() >= Duration::from_millis(1400));
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_packet_disconnects() {
    let opts = BrokerOptions {
        max_packet_size: 1024,
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let mut raw = Raw::connect(&uri, "big", Version::V5, 0).await;
    let h = PublishHeader {
        version: Version::V5,
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "t",
        pkid: 0,
        properties: &[],
    };
    let mut out = Vec::new();
    encode::publish(&h, &[0; 2000], &mut out);
    raw.write(&out).await;
    let (byte1, packet) = raw.packet().await.expect("DISCONNECT");
    assert_eq!(byte1 >> 4, 14);
    assert_eq!(packet[2], zzq_proto::reason::PACKET_TOO_LARGE);
}

#[tokio::test(flavor = "multi_thread")]
async fn qos2_duplicate_is_delivered_once() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut sub = Client::connect(&ctx, &uri, ClientOptions::new("s"))
        .await
        .unwrap();
    sub.subscribe("q2", QoS::ExactlyOnce).await.unwrap();
    let mut raw = Raw::connect(&uri, "p", Version::V311, 0).await;
    for dup in [false, true] {
        let h = PublishHeader {
            version: Version::V311,
            dup,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "q2",
            pkid: 7,
            properties: &[],
        };
        let mut out = Vec::new();
        encode::publish(&h, b"once", &mut out);
        raw.write(&out).await;
        let (byte1, _) = raw.packet().await.unwrap();
        assert_eq!(byte1 >> 4, 5, "PUBREC");
    }
    let mut rel = Vec::new();
    encode::ack(
        &AckFields::new(PacketType::PubRel, 7),
        Version::V311,
        &mut rel,
    );
    raw.write(&rel).await;
    let (byte1, _) = raw.packet().await.unwrap();
    assert_eq!(byte1 >> 4, 7, "PUBCOMP");
    assert_eq!(next(&mut sub).await.payload, "once");
    nothing(&mut sub).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn retain_handling_and_retain_as_published() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    broker
        .publish_retained("r", "kept", QoS::AtMostOnce)
        .unwrap();
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("c"))
        .await
        .unwrap();
    let never = SubscriptionOptions {
        retain_handling: 2,
        ..SubscriptionOptions::default()
    };
    c.subscribe_with(&[("r", never)], 0).await.unwrap();
    nothing(&mut c).await;
    let if_new = SubscriptionOptions {
        retain_handling: 1,
        retain_as_published: true,
        ..SubscriptionOptions::default()
    };
    c.unsubscribe(&["r"]).await.unwrap();
    c.subscribe_with(&[("r", if_new)], 0).await.unwrap();
    assert!(next(&mut c).await.retain);
    c.subscribe_with(&[("r", if_new)], 0).await.unwrap();
    nothing(&mut c).await;
    c.publish_retained("r", "live", QoS::AtMostOnce)
        .await
        .unwrap();
    let m = next(&mut c).await;
    assert!(m.retain, "retain_as_published keeps the flag");
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_queue_drops_oldest() {
    let opts = BrokerOptions {
        max_queued: 3,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let durable = ClientOptions {
        clean_start: false,
        session_expiry: 60,
        ..ClientOptions::new("d")
    };
    let c = Client::connect(&ctx, &uri, durable.clone()).await.unwrap();
    c.subscribe("q", QoS::AtLeastOnce).await.unwrap();
    c.disconnect().await;
    let p = Client::connect(&ctx, &uri, ClientOptions::new("p"))
        .await
        .unwrap();
    for i in 0..5 {
        p.publish("q", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    let mut c = Client::connect(&ctx, &uri, durable).await.unwrap();
    for i in 2..5 {
        assert_eq!(next(&mut c).await.payload, format!("{i}"));
    }
    nothing(&mut c).await;
    assert_eq!(broker.stats().await.unwrap().dropped_queue_full, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn unsuback_means_unsubscribed_on_every_lane() {
    let (ctx, _broker, uri) = setup(4, BrokerOptions::default()).await;
    let mut publishers = Vec::new();
    for i in 0..4 {
        publishers.push(
            Client::connect(&ctx, &uri, ClientOptions::new(format!("p{i}")))
                .await
                .unwrap(),
        );
    }
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("u"))
        .await
        .unwrap();
    for round in 0..20 {
        c.subscribe("x", QoS::AtLeastOnce).await.unwrap();
        for p in &publishers {
            p.publish("x", format!("{round}"), QoS::AtLeastOnce)
                .await
                .unwrap();
        }
        for _ in &publishers {
            assert_eq!(next(&mut c).await.payload, format!("{round}"));
        }
        c.unsubscribe(&["x"]).await.unwrap();
        for p in &publishers {
            p.publish("x", "late", QoS::AtLeastOnce).await.unwrap();
        }
    }
    nothing(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_subscription_across_lanes() {
    let (ctx, _broker, uri) = setup(3, BrokerOptions::default()).await;
    let mut members = Vec::new();
    for i in 0..3 {
        let m = Client::connect(&ctx, &uri, ClientOptions::new(format!("m{i}")))
            .await
            .unwrap();
        m.subscribe("$share/g/work", QoS::AtLeastOnce)
            .await
            .unwrap();
        members.push(m);
    }
    let p = Client::connect(&ctx, &uri, ClientOptions::new("p"))
        .await
        .unwrap();
    for i in 0..30 {
        p.publish("work", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    let mut seen = Vec::new();
    for m in &mut members {
        while let Ok(Some(msg)) = timeout(Duration::from_millis(300), m.recv()).await {
            seen.push(String::from_utf8(msg.payload.to_vec()).unwrap());
        }
    }
    seen.sort_by_key(|s| s.parse::<u32>().unwrap());
    let want: Vec<String> = (0..30).map(|i| format!("{i}")).collect();
    assert_eq!(seen, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn will_delay_is_cancelled_by_reconnect() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut watcher = Client::connect(&ctx, &uri, ClientOptions::new("w"))
        .await
        .unwrap();
    watcher
        .subscribe("wills/#", QoS::AtLeastOnce)
        .await
        .unwrap();
    for (id, reconnect) in [("back", true), ("gone", false)] {
        let opts = ClientOptions {
            clean_start: false,
            session_expiry: 60,
            will: Some(LastWill {
                topic: format!("wills/{id}"),
                payload: "x".into(),
                qos: QoS::AtLeastOnce,
                retain: false,
                delay: 1,
            }),
            ..ClientOptions::new(id)
        };
        let c = Client::connect(&ctx, &uri, opts.clone()).await.unwrap();
        drop(c);
        if reconnect {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let again = ClientOptions { will: None, ..opts };
            std::mem::forget(Client::connect(&ctx, &uri, again).await.unwrap());
        }
    }
    let m = timeout(Duration::from_secs(4), watcher.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.topic, "wills/gone");
    nothing(&mut watcher).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn session_expires() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let opts = ClientOptions {
        clean_start: false,
        session_expiry: 1,
        ..ClientOptions::new("e")
    };
    Client::connect(&ctx, &uri, opts.clone())
        .await
        .unwrap()
        .disconnect()
        .await;
    let c = Client::connect(&ctx, &uri, opts.clone()).await.unwrap();
    assert!(c.connected().session_present);
    c.disconnect().await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let c = Client::connect(&ctx, &uri, opts).await.unwrap();
    assert!(!c.connected().session_present);
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_messages_expire() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let durable = ClientOptions {
        clean_start: false,
        session_expiry: 60,
        ..ClientOptions::new("exp")
    };
    let c = Client::connect(&ctx, &uri, durable.clone()).await.unwrap();
    c.subscribe("exp", QoS::AtLeastOnce).await.unwrap();
    c.disconnect().await;
    let mut raw = Raw::connect(&uri, "exp-pub", Version::V5, 0).await;
    for (seconds, payload) in [(1u32, &b"short"[..]), (30, &b"long"[..])] {
        let mut props = Vec::new();
        zzq_proto::property::Property::U32(zzq_proto::property::MESSAGE_EXPIRY_INTERVAL, seconds)
            .encode(&mut props);
        let h = PublishHeader {
            version: Version::V5,
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic: "exp",
            pkid: 1,
            properties: &props,
        };
        let mut out = Vec::new();
        encode::publish(&h, payload, &mut out);
        raw.write(&out).await;
        let (byte1, _) = raw.packet().await.unwrap();
        assert_eq!(byte1 >> 4, 4, "PUBACK");
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let mut c = Client::connect(&ctx, &uri, durable.clone()).await.unwrap();
    assert_eq!(next(&mut c).await.payload, "long");
    nothing(&mut c).await;
    assert_eq!(broker.stats().await.unwrap().dropped_expired, 1);
    // The count outlives the session.
    c.disconnect().await;
    let clean = ClientOptions {
        clean_start: true,
        ..durable
    };
    Client::connect(&ctx, &uri, clean).await.unwrap();
    assert_eq!(broker.stats().await.unwrap().dropped_expired, 1);
}

struct OwnTree;

impl zzq_broker::Authorizer for OwnTree {
    fn may_publish(&self, client: &zzq_broker::ClientInfo<'_>, topic: &str) -> bool {
        topic.starts_with(&format!("{}/", client.client_id))
    }

    fn may_subscribe(&self, _client: &zzq_broker::ClientInfo<'_>, filter: &str) -> bool {
        !filter.starts_with('#')
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authorizer_limits_publish_and_subscribe() {
    let opts = BrokerOptions {
        authorizer: Some(std::sync::Arc::new(OwnTree)),
        ..BrokerOptions::default()
    };
    let (ctx, _broker, uri) = setup(1, opts).await;
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("alice"))
        .await
        .unwrap();
    let any = c.subscribe("#", QoS::AtLeastOnce).await.unwrap();
    let own = c.subscribe("+/#", QoS::AtLeastOnce).await.unwrap();
    let want = (ReasonCode::NOT_AUTHORIZED, ReasonCode::GRANTED_QOS_1);
    assert_eq!((any, own), want);
    c.publish("alice/x", "mine", QoS::AtLeastOnce)
        .await
        .unwrap();
    assert_eq!(next(&mut c).await.payload, "mine");
    let err = c
        .publish("bob/x", "theirs", QoS::AtLeastOnce)
        .await
        .unwrap_err();
    assert!(
        matches!(err, zzq_client::Error::Rejected(ReasonCode::NOT_AUTHORIZED)),
        "{err}"
    );
    c.publish("bob/x", "theirs", QoS::AtMostOnce).await.unwrap();
    nothing(&mut c).await;
    // v3 cannot be told no: the publish is acked and dropped.
    let old = ClientOptions {
        version: Version::V311,
        ..ClientOptions::new("carol")
    };
    let carol = Client::connect(&ctx, &uri, old).await.unwrap();
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
        carol.publish("bob/x", "theirs", qos).await.unwrap();
    }
    carol
        .publish("carol/x", "mine", QoS::ExactlyOnce)
        .await
        .unwrap();
    assert_eq!(next(&mut c).await.payload, "mine");
    nothing(&mut c).await;
    let sneaky = ClientOptions {
        will: Some(LastWill {
            topic: "bob/will".to_owned(),
            payload: "x".into(),
            qos: QoS::AtMostOnce,
            retain: false,
            delay: 0,
        }),
        ..ClientOptions::new("dave")
    };
    let refused = Client::connect(&ctx, &uri, sneaky).await;
    assert!(
        matches!(
            refused,
            Err(zzq_client::Error::Refused(ReasonCode::NOT_AUTHORIZED))
        ),
        "{refused:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn clients_cannot_publish_to_sys() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("sys"))
        .await
        .unwrap();
    c.subscribe("$SYS/fake", QoS::AtLeastOnce).await.unwrap();
    let err = c
        .publish("$SYS/fake", "x", QoS::AtLeastOnce)
        .await
        .unwrap_err();
    assert!(
        matches!(err, zzq_client::Error::Rejected(ReasonCode::NOT_AUTHORIZED)),
        "{err}"
    );
    nothing(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_subscriber_slows_qos1_publishers_instead_of_dropping() {
    for lanes in [1, 2] {
        let opts = BrokerOptions {
            max_queued: 200,
            qos_queue_high: 64,
            max_inflight: 32,
            ..BrokerOptions::default()
        };
        let (ctx, broker, uri) = setup(lanes, opts).await;
        let slow = ClientOptions {
            inbox: 16,
            ..ClientOptions::new("slow")
        };
        let mut sub = Client::connect(&ctx, &uri, slow).await.unwrap();
        sub.subscribe("in/+", QoS::AtLeastOnce).await.unwrap();
        let mut tasks = Vec::new();
        for i in 0..4 {
            let c = Client::connect(&ctx, &uri, ClientOptions::new(format!("p{i}")))
                .await
                .unwrap();
            tasks.push(tokio::spawn(async move {
                for n in 0..1000 {
                    c.publish(&format!("in/{i}"), format!("{n}"), QoS::AtLeastOnce)
                        .await
                        .unwrap();
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let mut count = 0;
        while count < 4000 {
            next(&mut sub).await;
            count += 1;
            if count % 500 == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        for t in tasks {
            t.await.unwrap();
        }
        let stats = broker.stats().await.unwrap();
        assert_eq!(stats.dropped_queue_full, 0, "lanes={lanes}");
        nothing(&mut sub).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sys_topics_report_status() {
    let opts = BrokerOptions {
        sys_interval: Some(Duration::from_millis(50)),
        ..BrokerOptions::default()
    };
    let (ctx, _broker, uri) = setup(2, opts).await;
    let mut all = Client::connect(&ctx, &uri, ClientOptions::new("all"))
        .await
        .unwrap();
    all.subscribe("#", QoS::AtMostOnce).await.unwrap();
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("sys-watch"))
        .await
        .unwrap();
    c.subscribe("$SYS/broker/clients/connected", QoS::AtMostOnce)
        .await
        .unwrap();
    while next(&mut c).await.payload != "2" {}
    c.subscribe("$SYS/broker/version", QoS::AtMostOnce)
        .await
        .unwrap();
    let m = next(&mut c).await;
    assert_eq!(m.topic, "$SYS/broker/version");
    assert!(m.payload.starts_with(b"zzq "), "{:?}", m.payload);
    nothing(&mut all).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn max_connections_refuses_extra_clients() {
    let opts = BrokerOptions {
        max_connections: 2,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(2, opts).await;
    let a = Client::connect(&ctx, &uri, ClientOptions::new("a"))
        .await
        .unwrap();
    let b = Client::connect(&ctx, &uri, ClientOptions::new("b"))
        .await
        .unwrap();
    let refused = Client::connect(&ctx, &uri, ClientOptions::new("c")).await;
    assert!(
        matches!(
            refused,
            Err(zzq_client::Error::Refused(ReasonCode::QUOTA_EXCEEDED))
        ),
        "{refused:?}"
    );
    // Taking over a connected session needs no new slot.
    let _a2 = Client::connect(&ctx, &uri, ClientOptions::new("a"))
        .await
        .unwrap();
    drop(a);
    let mut stats = broker.stats().await.unwrap();
    for _ in 0..50 {
        if stats.connections == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        stats = broker.stats().await.unwrap();
    }
    assert_eq!(stats.connections, 2);
    b.disconnect().await;
    let mut c = None;
    for _ in 0..50 {
        if let Ok(client) = Client::connect(&ctx, &uri, ClientOptions::new("c")).await {
            c = Some(client);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(c.is_some(), "a freed slot lets the next client in");
}

#[tokio::test(flavor = "multi_thread")]
async fn max_keep_alive_applies_to_silent_clients() {
    let opts = BrokerOptions {
        max_keep_alive: Some(1),
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, uri) = setup(1, opts).await;
    let (mut raw, connack) = Raw::connect_ack(&uri, "forever", Version::V5, 0).await;
    let props = zzq_proto::Properties::try_from(&connack[5..]).unwrap();
    assert_eq!(
        props.get_int(zzq_proto::property::SERVER_KEEP_ALIVE),
        Some(1)
    );
    let started = std::time::Instant::now();
    let (byte1, packet) = raw.packet().await.expect("DISCONNECT");
    assert_eq!(byte1 >> 4, 14);
    assert_eq!(packet[2], zzq_proto::reason::KEEP_ALIVE_TIMEOUT);
    assert!(started.elapsed() >= Duration::from_millis(1400));
}

#[tokio::test(flavor = "multi_thread")]
async fn deep_topics_are_rejected() {
    let (ctx, broker, uri) = setup(2, BrokerOptions::default()).await;
    let deep = |levels: usize| vec!["a"; levels].join("/");
    let c = Client::connect(&ctx, &uri, ClientOptions::new("deep"))
        .await
        .unwrap();
    assert_eq!(
        c.subscribe(&deep(200), QoS::AtMostOnce).await.unwrap(),
        ReasonCode::TOPIC_FILTER_INVALID
    );
    assert_eq!(
        c.subscribe(&deep(128), QoS::AtMostOnce).await.unwrap(),
        ReasonCode::SUCCESS
    );
    // Deep enough to overflow a recursive matcher's stack.
    let mut raw = Raw::connect(&uri, "deeper", Version::V5, 0).await;
    let topic = deep(30_000);
    let h = PublishHeader {
        version: Version::V5,
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic: &topic,
        pkid: 0,
        properties: &[],
    };
    let mut out = Vec::new();
    encode::publish(&h, b"x", &mut out);
    raw.write(&out).await;
    let (byte1, packet) = raw.packet().await.expect("DISCONNECT");
    assert_eq!(byte1 >> 4, 14);
    assert_eq!(packet[2], zzq_proto::reason::TOPIC_NAME_INVALID);
    assert!(
        broker
            .publish_retained(&topic, "x", QoS::AtMostOnce)
            .is_err()
    );
    c.publish("a/b", "still up", QoS::AtLeastOnce)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_publishes_the_old_will() {
    let (ctx, _broker, uri) = setup(2, BrokerOptions::default()).await;
    let mut watch = Client::connect(&ctx, &uri, ClientOptions::new("watch"))
        .await
        .unwrap();
    watch.subscribe("wills/#", QoS::AtLeastOnce).await.unwrap();
    let with_will = || ClientOptions {
        will: Some(LastWill {
            topic: "wills/twin".to_owned(),
            payload: "old".into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 0,
        }),
        ..ClientOptions::new("twin")
    };
    let _old = Client::connect(&ctx, &uri, with_will()).await.unwrap();
    let new = Client::connect(&ctx, &uri, with_will()).await.unwrap();
    assert_eq!(next(&mut watch).await.payload, "old");
    new.disconnect().await;
    nothing(&mut watch).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn no_local_needs_every_overlapping_subscription() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("self"))
        .await
        .unwrap();
    let mut no_local = SubscriptionOptions::from(QoS::AtLeastOnce);
    no_local.no_local = true;
    c.subscribe_with(&[("o/#", no_local)], 0).await.unwrap();
    c.publish("o/x", "hidden", QoS::AtLeastOnce).await.unwrap();
    nothing(&mut c).await;
    c.subscribe("o/x", QoS::AtMostOnce).await.unwrap();
    c.publish("o/x", "seen", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(next(&mut c).await.payload, "seen");
    nothing(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn overlapping_subscriptions_carry_every_identifier() {
    let (ctx, _broker, uri) = setup(2, BrokerOptions::default()).await;
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("ids"))
        .await
        .unwrap();
    let q1 = SubscriptionOptions::from(QoS::AtLeastOnce);
    c.subscribe_with(&[("m/#", q1)], 1).await.unwrap();
    c.subscribe_with(&[("m/+", SubscriptionOptions::from(QoS::AtMostOnce))], 2)
        .await
        .unwrap();
    c.publish("m/x", "both", QoS::AtLeastOnce).await.unwrap();
    let m = next(&mut c).await;
    let mut ids = m.subscription_ids.clone();
    ids.sort_unstable();
    assert_eq!((m.qos, ids), (QoS::AtLeastOnce, vec![1, 2]));
    nothing(&mut c).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn max_retained_refuses_new_topics() {
    let opts = BrokerOptions {
        max_retained: 2,
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("keep"))
        .await
        .unwrap();
    c.subscribe("k/#", QoS::AtLeastOnce).await.unwrap();
    for (topic, payload) in [("k/1", "a"), ("k/2", "b"), ("k/3", "c"), ("k/1", "d")] {
        c.publish_retained(topic, payload, QoS::AtLeastOnce)
            .await
            .unwrap();
        assert_eq!(next(&mut c).await.payload, payload);
    }
    let stats = broker.stats().await.unwrap();
    assert_eq!((stats.retained, stats.retained_rejected), (2, 1));
    let mut late = Client::connect(&ctx, &uri, ClientOptions::new("late"))
        .await
        .unwrap();
    late.subscribe("k/1", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(next(&mut late).await.payload, "d");
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_cancels_a_delayed_will() {
    let (ctx, _broker, uri) = setup(2, BrokerOptions::default()).await;
    let mut watch = Client::connect(&ctx, &uri, ClientOptions::new("watch2"))
        .await
        .unwrap();
    watch.subscribe("wills/#", QoS::AtLeastOnce).await.unwrap();
    let delayed = |clean_start| ClientOptions {
        clean_start,
        session_expiry: 60,
        will: Some(LastWill {
            topic: "wills/late".to_owned(),
            payload: "old".into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 1,
        }),
        ..ClientOptions::new("delayed")
    };
    let _old = Client::connect(&ctx, &uri, delayed(false)).await.unwrap();
    let new = Client::connect(&ctx, &uri, delayed(false)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    nothing(&mut watch).await;
    // A clean start ends the session, and with it the delay.
    let _clean = Client::connect(&ctx, &uri, delayed(true)).await.unwrap();
    let m = timeout(Duration::from_millis(700), watch.recv())
        .await
        .expect("will on session end")
        .unwrap();
    assert_eq!(m.payload, "old");
    drop(new);
    nothing(&mut watch).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn will_timer_of_an_earlier_connection_does_not_fire() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut watch = Client::connect(&ctx, &uri, ClientOptions::new("watch3"))
        .await
        .unwrap();
    watch.subscribe("wills/#", QoS::AtLeastOnce).await.unwrap();
    let opts = |payload: &str| ClientOptions {
        clean_start: false,
        session_expiry: 60,
        will: Some(LastWill {
            topic: "wills/twice".to_owned(),
            payload: payload.to_owned().into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 2,
        }),
        ..ClientOptions::new("twice")
    };
    // Lost at t0, back at t0 + 0.3 s, lost again at t0 + 1 s.
    drop(Client::connect(&ctx, &uri, opts("old")).await.unwrap());
    tokio::time::sleep(Duration::from_millis(300)).await;
    let again = Client::connect(&ctx, &uri, opts("new")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    drop(again);
    let m = timeout(Duration::from_secs(4), watch.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(m.payload, "new");
    nothing(&mut watch).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn will_expiry_starts_when_the_will_goes_out() {
    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let mut watch = Client::connect(&ctx, &uri, ClientOptions::new("watch4"))
        .await
        .unwrap();
    watch.subscribe("wills/#", QoS::AtLeastOnce).await.unwrap();
    let props = [zzq_proto::property::Property::U32(
        zzq_proto::property::MESSAGE_EXPIRY_INTERVAL,
        1,
    )];
    let c = ConnectFields {
        version: Version::V5,
        clean_start: true,
        keep_alive: 0,
        properties: &[],
        client_id: "expiring-will",
        will: Some(encode::WillFields {
            qos: QoS::AtLeastOnce,
            retain: true,
            properties: &props,
            topic: "wills/exp",
            payload: b"bye",
        }),
        username: None,
        password: None,
    };
    let (raw, _) = Raw::connect_with(&uri, &c).await;
    // Older than its expiry before it goes out.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    drop(raw);
    assert_eq!(next(&mut watch).await.payload, "bye");
    let mut late = Client::connect(&ctx, &uri, ClientOptions::new("late4"))
        .await
        .unwrap();
    late.subscribe("wills/exp", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(next(&mut late).await.payload, "bye");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(broker.stats().await.unwrap().retained, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_qos0_delivery_carries_the_time_left() {
    let (_ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut publ = Raw::connect(&uri, "ttl-pub", Version::V5, 0).await;
    let mut props = Vec::new();
    zzq_proto::property::Property::U32(zzq_proto::property::MESSAGE_EXPIRY_INTERVAL, 10)
        .encode(&mut props);
    let h = PublishHeader {
        version: Version::V5,
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: true,
        topic: "ttl",
        pkid: 1,
        properties: &props,
    };
    let mut out = Vec::new();
    encode::publish(&h, b"x", &mut out);
    publ.write(&out).await;
    assert_eq!(publ.packet().await.unwrap().0 >> 4, 4, "PUBACK");
    tokio::time::sleep(Duration::from_millis(2200)).await;
    let mut sub = Raw::connect(&uri, "ttl-sub", Version::V5, 0).await;
    sub.subscribe("ttl", QoS::AtMostOnce).await;
    let (_, packet) = sub.packet().await.expect("retained PUBLISH");
    let Some((zzq_proto::Packet::Publish(p), _)) =
        decode(&packet, Version::V5, usize::MAX).unwrap()
    else {
        panic!("expected PUBLISH");
    };
    let left = p
        .properties
        .get_int(zzq_proto::property::MESSAGE_EXPIRY_INTERVAL)
        .unwrap();
    assert!(left <= 8, "expiry {left}");
}

#[tokio::test(flavor = "multi_thread")]
async fn publisher_subscription_identifier_is_a_protocol_error() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let mut sub = Client::connect(&ctx, &uri, ClientOptions::new("spoofed"))
        .await
        .unwrap();
    sub.subscribe_with(&[("spoof", SubscriptionOptions::from(QoS::AtMostOnce))], 1)
        .await
        .unwrap();
    let mut raw = Raw::connect(&uri, "spoofer", Version::V5, 0).await;
    let mut props = Vec::new();
    zzq_proto::property::Property::VarInt(zzq_proto::property::SUBSCRIPTION_IDENTIFIER, 7)
        .encode(&mut props);
    let h = PublishHeader {
        version: Version::V5,
        dup: false,
        qos: QoS::AtMostOnce,
        retain: false,
        topic: "spoof",
        pkid: 0,
        properties: &props,
    };
    let mut out = Vec::new();
    encode::publish(&h, b"x", &mut out);
    raw.write(&out).await;
    let (byte1, packet) = raw.packet().await.expect("DISCONNECT");
    assert_eq!(byte1 >> 4, 14);
    assert_eq!(packet[2], zzq_proto::reason::PROTOCOL_ERROR);
    nothing(&mut sub).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_drops_output_queued_for_the_old_connection() {
    let (ctx, _broker, uri) = setup(1, BrokerOptions::default()).await;
    let fields = ConnectFields {
        version: Version::V311,
        clean_start: false,
        keep_alive: 0,
        properties: &[],
        client_id: "stuck",
        will: None,
        username: None,
        password: None,
    };
    let (mut old, _) = Raw::connect_with(&uri, &fields).await;
    old.subscribe("flood", QoS::AtMostOnce).await;
    // The old connection stops reading, so output piles up for it.
    let publ = Client::connect(&ctx, &uri, ClientOptions::new("flooder"))
        .await
        .unwrap();
    let chunk = vec![b'f'; 64 << 10];
    for _ in 0..400 {
        publ.publish("flood", chunk.clone(), QoS::AtMostOnce)
            .await
            .unwrap();
    }
    publ.publish("barrier", "", QoS::AtLeastOnce).await.unwrap();
    let (mut new, _) = Raw::connect_with(&uri, &fields).await;
    let mut ping = Vec::new();
    encode::pingreq(&mut ping);
    new.write(&ping).await;
    let (byte1, _) = new.packet().await.expect("PINGRESP");
    assert_eq!(byte1 >> 4, 13, "stale output after CONNACK");
    drop(old);
}

#[tokio::test(flavor = "multi_thread")]
#[expect(clippy::too_many_lines, reason = "one exchange with a fake broker")]
async fn client_waits_for_a_free_packet_id() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("tcp://{}", listener.local_addr().unwrap());
    let ctx = Context::with_config(&Config {
        io_threads: 1,
        ..Config::default()
    })
    .unwrap();
    let opts = ClientOptions {
        version: Version::V311,
        ..ClientOptions::new("ids")
    };
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move { Client::connect(&ctx, &uri, opts).await.unwrap() }
    });
    let mut broker = Raw {
        s: listener.accept().await.unwrap().0,
        buf: Vec::new(),
        version: Version::V311,
    };
    assert_eq!(broker.packet().await.unwrap().0 >> 4, 1, "CONNECT");
    let mut out = Vec::new();
    encode::connack(&encode::ConnAckFields::default(), Version::V311, &mut out);
    broker.write(&out).await;
    let c = std::sync::Arc::new(connecting.await.unwrap());
    // No Receive Maximum: every packet ID goes to an unacked publish.
    for _ in 0..u16::MAX {
        let c = c.clone();
        tokio::spawn(async move { c.publish("t", "x", QoS::AtLeastOnce).await });
    }
    for _ in 0..u16::MAX {
        assert_eq!(broker.packet().await.unwrap().0 >> 4, 3, "PUBLISH");
    }
    let sub = tokio::spawn({
        let c = c.clone();
        async move { c.subscribe("s", QoS::AtMostOnce).await }
    });
    // The SUBSCRIBE waits for an ID.
    let mut byte = [0u8; 1];
    let early = timeout(Duration::from_millis(300), broker.s.read(&mut byte)).await;
    assert!(early.is_err(), "SUBSCRIBE without a free packet ID");
    let mut puback = Vec::new();
    encode::ack(
        &AckFields::new(PacketType::PubAck, 1),
        Version::V311,
        &mut puback,
    );
    broker.write(&puback).await;
    let (byte1, packet) = broker.packet().await.expect("SUBSCRIBE");
    assert_eq!(byte1 >> 4, 8);
    assert_eq!(&packet[2..4], &[0, 1], "the freed packet ID");
    let mut suback = Vec::new();
    encode::suback(
        &encode::SubAckFields {
            kind: PacketType::SubAck,
            pkid: 1,
            properties: &[],
            codes: &[0],
        },
        Version::V311,
        &mut suback,
    );
    broker.write(&suback).await;
    assert_eq!(sub.await.unwrap().unwrap(), ReasonCode::SUCCESS);
}

#[tokio::test(flavor = "multi_thread")]
async fn client_and_broker_refuse_what_mqtt_cannot_encode() {
    use zzq_client::Error;
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let c = Client::connect(&ctx, &uri, ClientOptions::new("limits"))
        .await
        .unwrap();
    let long = "t".repeat(70_000);
    // Zeroed and never touched, so the allocation stays cheap.
    let huge = bytes::Bytes::from(vec![0u8; zzq_proto::varint::MAX]);
    for (topic, payload) in [
        ("a/+", bytes::Bytes::new()),
        (long.as_str(), bytes::Bytes::new()),
    ] {
        let r = c.publish(topic, payload, QoS::AtMostOnce).await;
        assert!(matches!(r, Err(Error::InvalidTopic(_))), "{r:?}");
    }
    let r = c.publish("a", huge.clone(), QoS::AtLeastOnce).await;
    assert!(matches!(r, Err(Error::TooLarge)), "{r:?}");
    let r = c.subscribe("a/#/b", QoS::AtMostOnce).await;
    assert!(matches!(r, Err(Error::InvalidTopic(_))), "{r:?}");
    let r = c.unsubscribe(&["a+"]).await;
    assert!(matches!(r, Err(Error::InvalidTopic(_))), "{r:?}");
    let opts = ClientOptions {
        password: Some(long.clone().into()),
        ..ClientOptions::new("long-password")
    };
    let r = Client::connect(&ctx, &uri, opts).await;
    assert!(matches!(r, Err(Error::TooLong("password"))), "{r:?}");
    let r = broker.publish("a", huge, QoS::AtMostOnce);
    assert!(matches!(r, Err(zzq_broker::Error::TooLarge)), "{r:?}");
    // Still usable.
    c.publish("a", "fine", QoS::AtLeastOnce).await.unwrap();
}

/// A store whose disk is gone.
#[derive(Debug)]
struct Unreadable;

impl zzq_broker::Persistence for Unreadable {
    fn load_retained(&self) -> std::io::Result<Vec<zzq_broker::Retained>> {
        Err(std::io::Error::other("disk gone"))
    }

    fn set_retained(&self, _: &zzq_broker::Retained) {}

    fn remove_retained(&self, _: &str) {}
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_say_what_failed_and_keep_the_cause() {
    use std::error::Error as _;
    let ctx = Context::new().unwrap();
    let e = Client::connect(&ctx, "mqtt://x", ClientOptions::new("c"))
        .await
        .unwrap_err();
    assert!(matches!(e, zzq_client::Error::Transport(_)), "{e:?}");
    assert_eq!(e.to_string(), "transport failed");
    assert!(e.source().unwrap().to_string().contains("unknown scheme"));
    let opts = BrokerOptions {
        persistence: Some(std::sync::Arc::new(Unreadable)),
        ..BrokerOptions::default()
    };
    let e = Broker::new(&ctx, opts).await.unwrap_err();
    assert!(matches!(e, zzq_broker::Error::Persistence(_)), "{e:?}");
    assert_eq!(e.to_string(), "loading retained messages failed");
    assert_eq!(e.source().unwrap().to_string(), "disk gone");
}

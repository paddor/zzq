//! The state file: sessions, their queues and wills, and retained messages
//! kept over a restart.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests fail loudly"
)]

mod support;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use support::{Got, Raw, WAIT, client, next};
use tokio::time::timeout;
use zzq_broker::{Broker, BrokerOptions, Config, Context, Error, lane_for};
use zzq_client::{Client, ClientOptions, Handle, LastWill};
use zzq_proto::encode::ConnectFields;
use zzq_proto::property::SESSION_EXPIRY_INTERVAL;
use zzq_proto::{Property, QoS, Version, reason};

/// A session that outlives its connection by ten minutes.
const KEEP: Property<'static> = Property::U32(SESSION_EXPIRY_INTERVAL, 600);

/// An empty directory for one test.
fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("zzq-state-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn opts(file: &Path) -> BrokerOptions {
    BrokerOptions {
        state_file: Some(file.to_owned()),
        sys_interval: None,
        ..BrokerOptions::default()
    }
}

fn lanes(n: usize) -> Context {
    Context::with_config(&Config {
        io_threads: n,
        ..Config::default()
    })
    .unwrap()
}

async fn start(ctx: &Context, opts: BrokerOptions) -> (Broker, String) {
    let broker = Broker::new(ctx, opts).await.unwrap();
    let uri = broker.bind("tcp://127.0.0.1:0").await.unwrap().to_string();
    (broker, uri)
}

/// Options for a session that outlives its connection.
fn durable(id: &str) -> ClientOptions {
    ClientOptions {
        clean_start: false,
        session_expiry: 600,
        ..ClientOptions::new(id)
    }
}

/// A raw v5 connection to the session `id`, kept for ten minutes, and
/// whether the broker had it.
async fn raw(uri: &str, id: &str, clean_start: bool) -> (Raw, bool) {
    let props = [KEEP];
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
    match Raw::connack_with(uri, &c).await {
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

#[tokio::test(flavor = "multi_thread")]
async fn sessions_queues_and_retained_survive_a_restart() {
    let d = dir("restart");
    let file = d.join("state");
    let ctx = lanes(2);
    let (broker, uri) = start(&ctx, opts(&file)).await;
    let sub = Client::connect(&ctx, &uri, durable("d")).await.unwrap();
    sub.subscribe("q", QoS::AtLeastOnce).await.unwrap();
    sub.disconnect().await;
    let p = client(&ctx, &uri, "p", Version::V5).await;
    for i in 0..3 {
        p.publish("q", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    p.publish_retained("r", "kept", QoS::AtLeastOnce)
        .await
        .unwrap();
    // Connected at shutdown, with a delivery it has not acked.
    let (mut u, _) = raw(&uri, "u", true).await;
    u.subscribe("u/q", QoS::AtLeastOnce.into()).await;
    p.publish("u/q", "unacked", QoS::AtLeastOnce).await.unwrap();
    let first = u.publish().await;
    broker.shutdown().await.unwrap();
    u.disconnected(&[reason::SERVER_SHUTTING_DOWN]).await;
    assert!(file.exists());

    let (broker, uri) = start(&ctx, opts(&file)).await;
    assert!(!file.exists());
    assert!(d.join("state.loaded").exists());
    // The publisher's session ended with its connection.
    let stats = broker.stats().await.unwrap();
    assert_eq!((stats.sessions, stats.retained), (2, 1));
    let mut sub = Client::connect(&ctx, &uri, durable("d")).await.unwrap();
    assert!(sub.connected().session_present);
    for i in 0..3 {
        assert_eq!(next(&mut sub).await.payload, format!("{i}"));
    }
    let (mut u, present) = raw(&uri, "u", false).await;
    assert!(present);
    let again = u.publish().await;
    assert_eq!(
        (again.dup, again.pkid, &again.payload[..]),
        (true, first.pkid, &b"unacked"[..])
    );
    let mut late = client(&ctx, &uri, "late", Version::V5).await;
    late.subscribe("r", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(next(&mut late).await.payload, "kept");
    // The subscription is back on every lane.
    late.publish("q", "live", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(next(&mut sub).await.payload, "live");
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread")]
async fn wills_of_connections_open_at_shutdown_go_out() {
    let d = dir("wills");
    let file = d.join("state");
    let ctx = lanes(2);
    let (broker, uri) = start(&ctx, opts(&file)).await;
    let w = Client::connect(&ctx, &uri, durable("watch")).await.unwrap();
    w.subscribe("will/#", QoS::AtLeastOnce).await.unwrap();
    w.disconnect().await;
    let will = |topic: &str, delay| {
        Some(LastWill {
            topic: topic.to_owned(),
            payload: "gone".into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay,
        })
    };
    let now = ClientOptions {
        will: will("will/now", 0),
        ..durable("now")
    };
    let later = ClientOptions {
        will: will("will/later", 1),
        ..durable("later")
    };
    let _now = Client::connect(&ctx, &uri, now).await.unwrap();
    let _later = Client::connect(&ctx, &uri, later).await.unwrap();
    broker.shutdown().await.unwrap();

    // One went out at shutdown and waited in the queue, the other is due a
    // second later.
    let (_broker, uri) = start(&ctx, opts(&file)).await;
    let started = Instant::now();
    let mut w = Client::connect(&ctx, &uri, durable("watch")).await.unwrap();
    assert_eq!(next(&mut w).await.topic.to_string(), "will/now");
    assert_eq!(next(&mut w).await.topic.to_string(), "will/later");
    assert!(started.elapsed() >= Duration::from_millis(500));
    let _ = std::fs::remove_dir_all(&d);
}

/// Publishes 0, 1, 2, ... at QoS 1 to `stream` until the client closes, and
/// records which numbers got their ack.
async fn stream(h: Handle, acked: Arc<Mutex<Vec<u32>>>) {
    for i in 0u32.. {
        let Ok(ack) = h
            .enqueue("stream", i.to_string(), QoS::AtLeastOnce, false)
            .await
        else {
            return;
        };
        let acked = acked.clone();
        tokio::spawn(async move {
            if ack.await.is_ok() {
                acked.lock().unwrap().push(i);
            }
        });
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn messages_acked_before_shutdown_are_kept() {
    let d = dir("stream");
    let file = d.join("state");
    let ctx = lanes(2);
    let (broker, uri) = start(&ctx, opts(&file)).await;
    // On different lanes, so deliveries cross between them.
    let sub_id = (0..100)
        .map(|i| format!("sub{i}"))
        .find(|id| lane_for(id, 2) != lane_for("pub", 2))
        .unwrap();
    let s = Client::connect(&ctx, &uri, durable(&sub_id)).await.unwrap();
    s.subscribe("stream", QoS::AtLeastOnce).await.unwrap();
    s.disconnect().await;
    let p = Client::connect(&ctx, &uri, ClientOptions::new("pub"))
        .await
        .unwrap();
    let acked = Arc::new(Mutex::new(Vec::new()));
    let sender = tokio::spawn(stream(p.handle(), acked.clone()));
    // Shut down mid-stream.
    let started = Instant::now();
    while acked.lock().unwrap().len() < 1000 {
        assert!(started.elapsed() < WAIT, "too few acks");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    broker.shutdown().await.unwrap();
    timeout(WAIT, sender).await.unwrap().unwrap();
    drop(p);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let acked: HashSet<u32> = acked.lock().unwrap().iter().copied().collect();

    let (_broker, uri) = start(&ctx, opts(&file)).await;
    let mut s = Client::connect(&ctx, &uri, durable(&sub_id)).await.unwrap();
    let mut got = Vec::new();
    while let Ok(Some(m)) = timeout(Duration::from_millis(500), s.recv()).await {
        got.push(
            std::str::from_utf8(&m.payload)
                .unwrap()
                .parse::<u32>()
                .unwrap(),
        );
    }
    assert!(got.windows(2).all(|w| w[0] < w[1]), "out of order");
    let kept: HashSet<u32> = got.into_iter().collect();
    let lost: Vec<u32> = acked.difference(&kept).copied().collect();
    assert!(lost.is_empty(), "{} acked, lost {lost:?}", acked.len());
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_of_many_frames_on_every_lane_survive() {
    let d = dir("frames");
    let file = d.join("state");
    let ctx = lanes(4);
    let (broker, uri) = start(&ctx, opts(&file)).await;
    let ids: Vec<String> = (0..40).map(|i| format!("s{i}")).collect();
    for id in &ids {
        let s = Client::connect(&ctx, &uri, durable(id)).await.unwrap();
        s.subscribe("big", QoS::AtLeastOnce).await.unwrap();
        s.disconnect().await;
    }
    let p = client(&ctx, &uri, "p", Version::V5).await;
    let payload = |i: usize| format!("{i:04}").repeat(256);
    for i in 0..20 {
        p.publish("big", payload(i), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    broker.shutdown().await.unwrap();
    // Zstandard, and much smaller than the 800 KiB of payloads held.
    let image = std::fs::read(&file).unwrap();
    assert!(image.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]));
    assert!(image.len() < 100 << 10, "{} bytes", image.len());

    let (_broker, uri) = start(&ctx, opts(&file)).await;
    for id in &ids {
        let mut s = Client::connect(&ctx, &uri, durable(id)).await.unwrap();
        for i in 0..20 {
            assert_eq!(next(&mut s).await.payload, payload(i), "{id}");
        }
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torn_state_file_stops_the_start_and_stays() {
    let d = dir("torn");
    let file = d.join("state");
    let ctx = lanes(1);
    let (broker, uri) = start(&ctx, opts(&file)).await;
    let s = Client::connect(&ctx, &uri, durable("d")).await.unwrap();
    s.subscribe("q", QoS::AtLeastOnce).await.unwrap();
    s.disconnect().await;
    broker.shutdown().await.unwrap();
    let image = std::fs::read(&file).unwrap();
    for cut in [1, image.len() / 2] {
        std::fs::write(&file, &image[..image.len() - cut]).unwrap();
        let err = Broker::new(&ctx, opts(&file)).await.unwrap_err();
        assert!(matches!(err, Error::State(_)), "{err:?}");
        assert!(file.exists());
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_damaged_state_file_stops_the_start_and_stays() {
    let d = dir("damaged");
    let file = d.join("state");
    std::fs::write(&file, b"ZZQSTATE\x00\x01junk").unwrap();
    let ctx = lanes(1);
    let err = Broker::new(&ctx, opts(&file)).await.unwrap_err();
    assert!(matches!(err, Error::State(_)), "{err:?}");
    assert!(file.exists());
    let _ = std::fs::remove_dir_all(&d);
}

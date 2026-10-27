//! Broker and client end to end.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "tests fail loudly"
)]

use std::sync::Arc;
use std::time::Duration;

use tokio::time::timeout;
use zzq_broker::{
    AuthDecision, Authenticator, Broker, BrokerOptions, Config, ConnectInfo, Context, SnOptions,
};
use zzq_client::{Client, ClientOptions, LastWill, Message};
use zzq_proto::{QoS, ReasonCode, SubscriptionOptions, Version};

const WAIT: Duration = Duration::from_secs(5);

async fn setup(lanes: usize) -> (Context, Broker, String) {
    setup_with(lanes, BrokerOptions::default()).await
}

async fn setup_with(lanes: usize, opts: BrokerOptions) -> (Context, Broker, String) {
    let ctx = Context::with_config(&Config {
        io_threads: lanes,
        ..Config::default()
    })
    .unwrap();
    let broker = Broker::new(&ctx, opts).await.unwrap();
    let ep = broker.bind("tcp://127.0.0.1:0").await.unwrap();
    (ctx, broker, ep.to_string())
}

#[cfg(feature = "tls")]
/// Server and client transport options for a fresh self-signed certificate.
fn test_tls() -> (zzq_broker::TransportOptions, zzq_client::TransportOptions) {
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_pem = key.cert.pem().into_bytes();
    let server = zzq_broker::TransportOptions {
        tls: Some(zzq_broker::TlsOptions {
            cert_chain_pem: Some(cert_pem.clone()),
            key_pem: Some(key.signing_key.serialize_pem().into_bytes()),
            ..Default::default()
        }),
    };
    let client = zzq_client::TransportOptions {
        tls: Some(zzq_client::TlsOptions {
            trust_pem: Some(cert_pem),
            server_name: Some("localhost".to_owned()),
            ..Default::default()
        }),
    };
    (server, client)
}

async fn client(ctx: &Context, uri: &str, id: &str, version: Version) -> Client {
    let opts = ClientOptions {
        version,
        ..ClientOptions::new(id)
    };
    Client::connect(ctx, uri, opts).await.unwrap()
}

async fn next(c: &mut Client) -> Message {
    timeout(WAIT, c.recv())
        .await
        .expect("timed out")
        .expect("closed")
}

async fn nothing(c: &mut Client) {
    assert!(
        timeout(Duration::from_millis(200), c.recv()).await.is_err(),
        "unexpected message"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pub_sub_all_qos_both_versions() {
    let (ctx, _broker, uri) = setup(1).await;
    for version in [Version::V311, Version::V5] {
        let mut sub = client(&ctx, &uri, "sub", version).await;
        let publ = client(&ctx, &uri, "pub", version).await;
        for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
            let granted = sub.subscribe("t/q", qos).await.unwrap();
            assert_eq!(granted, ReasonCode::from(qos as u8));
            publ.publish("t/q", format!("{qos:?}"), qos).await.unwrap();
            let m = next(&mut sub).await;
            assert_eq!((m.topic.as_str(), m.qos, m.retain), ("t/q", qos, false));
            assert_eq!(m.payload, format!("{qos:?}"));
        }
        sub.disconnect().await;
        publ.disconnect().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn versions_interoperate() {
    let (ctx, _broker, uri) = setup(1).await;
    let mut v5 = client(&ctx, &uri, "a", Version::V5).await;
    let mut v4 = client(&ctx, &uri, "b", Version::V311).await;
    v5.subscribe("x", QoS::AtLeastOnce).await.unwrap();
    v4.subscribe("x", QoS::AtLeastOnce).await.unwrap();
    v4.publish("x", "from v4", QoS::AtMostOnce).await.unwrap();
    assert_eq!(next(&mut v5).await.payload, "from v4");
    assert_eq!(next(&mut v4).await.payload, "from v4");
    v5.publish("x", "from v5", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(next(&mut v4).await.payload, "from v5");
    assert_eq!(next(&mut v5).await.payload, "from v5");
}

#[tokio::test(flavor = "multi_thread")]
async fn wildcards_and_unsubscribe() {
    let (ctx, _broker, uri) = setup(1).await;
    let mut sub = client(&ctx, &uri, "s", Version::V5).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    sub.subscribe("a/+/c", QoS::AtMostOnce).await.unwrap();
    sub.subscribe("z/#", QoS::AtMostOnce).await.unwrap();
    for t in ["a/b/c", "a/b/d", "z", "z/1/2", "y"] {
        publ.publish(t, t, QoS::AtMostOnce).await.unwrap();
    }
    publ.publish("end", "", QoS::AtMostOnce).await.unwrap();
    let got: Vec<_> = [
        next(&mut sub).await,
        next(&mut sub).await,
        next(&mut sub).await,
    ]
    .into_iter()
    .map(|m| m.topic)
    .collect();
    assert_eq!(got, ["a/b/c", "z", "z/1/2"]);
    nothing(&mut sub).await;
    assert_eq!(
        sub.unsubscribe(&["z/#", "nope"]).await.unwrap(),
        [ReasonCode::SUCCESS, ReasonCode::NO_SUBSCRIPTION_EXISTED]
    );
    publ.publish("z/1", "x", QoS::AtMostOnce).await.unwrap();
    nothing(&mut sub).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ended_session_leaves_no_subscriptions() {
    for lanes in [1, 2] {
        let (ctx, broker, uri) = setup(lanes).await;
        let sub = client(&ctx, &uri, "s", Version::V5).await;
        sub.subscribe("a", QoS::AtMostOnce).await.unwrap();
        sub.subscribe("b", QoS::AtMostOnce).await.unwrap();
        assert_eq!(broker.stats().await.unwrap().subscriptions, 2);
        sub.disconnect().await;
        let mut stats = broker.stats().await.unwrap();
        for _ in 0..50 {
            if (stats.sessions, stats.subscriptions) == (0, 0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            stats = broker.stats().await.unwrap();
        }
        assert_eq!(
            (stats.sessions, stats.subscriptions),
            (0, 0),
            "{lanes} lanes"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_messages() {
    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup_with(1, opts).await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    publ.publish_retained("r/1", "one", QoS::AtLeastOnce)
        .await
        .unwrap();
    publ.publish_retained("r/2", "two", QoS::AtMostOnce)
        .await
        .unwrap();
    publ.publish_retained("r/2", "", QoS::AtLeastOnce)
        .await
        .unwrap();
    let mut sub = client(&ctx, &uri, "s", Version::V5).await;
    sub.subscribe("r/#", QoS::AtLeastOnce).await.unwrap();
    let m = next(&mut sub).await;
    assert_eq!(
        (m.topic.as_str(), m.retain, m.qos),
        ("r/1", true, QoS::AtLeastOnce)
    );
    nothing(&mut sub).await;
    publ.publish_retained("r/1", "live", QoS::AtMostOnce)
        .await
        .unwrap();
    let m = next(&mut sub).await;
    assert!(!m.retain, "live messages are not marked retained");
    assert_eq!(broker.stats().await.unwrap().retained, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn will_on_lost_connection_only() {
    let (ctx, _broker, uri) = setup(1).await;
    let mut watcher = client(&ctx, &uri, "w", Version::V5).await;
    watcher
        .subscribe("wills/#", QoS::AtLeastOnce)
        .await
        .unwrap();
    for (id, clean) in [("graceful", true), ("lost", false)] {
        let opts = ClientOptions {
            will: Some(LastWill {
                topic: format!("wills/{id}"),
                payload: "gone".into(),
                qos: QoS::AtLeastOnce,
                retain: false,
                delay: 0,
            }),
            ..ClientOptions::new(id)
        };
        let c = Client::connect(&ctx, &uri, opts).await.unwrap();
        if clean {
            c.disconnect().await;
        } else {
            drop(c);
        }
    }
    let m = next(&mut watcher).await;
    assert_eq!(m.topic, "wills/lost");
    nothing(&mut watcher).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn shared_subscription_splits_messages() {
    let (ctx, _broker, uri) = setup(1).await;
    let mut a = client(&ctx, &uri, "a", Version::V5).await;
    let mut b = client(&ctx, &uri, "b", Version::V5).await;
    a.subscribe("$share/g/jobs/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    b.subscribe("$share/g/jobs/+", QoS::AtLeastOnce)
        .await
        .unwrap();
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    for i in 0..10 {
        publ.publish("jobs/x", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    let mut seen = Vec::new();
    for _ in 0..5 {
        seen.push(next(&mut a).await.payload);
        seen.push(next(&mut b).await.payload);
    }
    nothing(&mut a).await;
    seen.sort();
    let mut want: Vec<_> = (0..10)
        .map(|i| bytes::Bytes::from(format!("{i}")))
        .collect();
    want.sort();
    assert_eq!(seen, want);
}

#[tokio::test(flavor = "multi_thread")]
async fn no_local_and_subscription_ids() {
    let (ctx, _broker, uri) = setup(1).await;
    let mut c = client(&ctx, &uri, "c", Version::V5).await;
    let opts = SubscriptionOptions {
        no_local: true,
        ..SubscriptionOptions::from(QoS::AtMostOnce)
    };
    c.subscribe_with(&[("own", opts)], 0).await.unwrap();
    c.subscribe_with(&[("tagged", SubscriptionOptions::default())], 42)
        .await
        .unwrap();
    c.publish("own", "x", QoS::AtMostOnce).await.unwrap();
    c.publish("tagged", "y", QoS::AtMostOnce).await.unwrap();
    let m = next(&mut c).await;
    assert_eq!(
        (m.topic.as_str(), m.subscription_ids.as_slice()),
        ("tagged", &[42][..])
    );
}

async fn persistent_session(lanes: usize) {
    let (ctx, _broker, uri) = setup(lanes).await;
    let opts = ClientOptions {
        clean_start: false,
        session_expiry: 60,
        ..ClientOptions::new("durable")
    };
    let c = Client::connect(&ctx, &uri, opts.clone()).await.unwrap();
    assert!(!c.connected().session_present);
    c.subscribe("jobs", QoS::AtLeastOnce).await.unwrap();
    c.disconnect().await;
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    for i in 0..3 {
        publ.publish("jobs", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    // Several connects so at least one lands on another lane.
    let mut c = Client::connect(&ctx, &uri, opts).await.unwrap();
    assert!(c.connected().session_present);
    for i in 0..3 {
        assert_eq!(next(&mut c).await.payload, format!("{i}"));
    }
    publ.publish("jobs", "live", QoS::AtLeastOnce)
        .await
        .unwrap();
    assert_eq!(next(&mut c).await.payload, "live");
}

#[tokio::test(flavor = "multi_thread")]
async fn persistent_session_one_lane() {
    persistent_session(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn persistent_session_moves_between_lanes() {
    persistent_session(3).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn takeover_closes_old_connection() {
    let (ctx, broker, uri) = setup(2).await;
    let mut first = client(&ctx, &uri, "same", Version::V5).await;
    let mut second = client(&ctx, &uri, "same", Version::V5).await;
    assert!(timeout(WAIT, first.recv()).await.unwrap().is_none());
    second.subscribe("t", QoS::AtMostOnce).await.unwrap();
    broker.publish("t", "hi", QoS::AtMostOnce).unwrap();
    assert_eq!(next(&mut second).await.payload, "hi");
    assert_eq!(broker.stats().await.unwrap().connections, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn cross_lane_fan_out() {
    let (ctx, broker, uri) = setup(4).await;
    let mut subs = Vec::new();
    for i in 0..8 {
        let c = client(&ctx, &uri, &format!("s{i}"), Version::V5).await;
        c.subscribe("fan", QoS::AtLeastOnce).await.unwrap();
        subs.push(c);
    }
    let publ = client(&ctx, &uri, "p", Version::V5).await;
    for i in 0..50 {
        publ.publish("fan", format!("{i}"), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    for s in &mut subs {
        for i in 0..50 {
            assert_eq!(next(s).await.payload, format!("{i}"));
        }
    }
    let stats = broker.stats().await.unwrap();
    assert_eq!(stats.deliveries, 400);
    assert_eq!(
        stats.dropped.offline + stats.dropped.queue_full + stats.dropped.buffer_full,
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ipc_and_inproc() {
    let ctx = Context::new().unwrap();
    let broker = Broker::new(&ctx, BrokerOptions::default()).await.unwrap();
    let path = std::env::temp_dir().join(format!("zzq-test-{}.sock", std::process::id()));
    let ipc = format!("ipc://{}", path.display());
    broker.bind(&ipc).await.unwrap();
    broker.bind("ipc://@zzq-test-abstract").await.unwrap();
    broker.bind("inproc://broker").await.unwrap();
    let mut sub = Client::connect(&ctx, "inproc://broker", ClientOptions::new("s"))
        .await
        .unwrap();
    sub.subscribe("t", QoS::AtMostOnce).await.unwrap();
    for uri in [ipc.as_str(), "ipc://@zzq-test-abstract", "inproc://broker"] {
        let p = Client::connect(&ctx, uri, ClientOptions::new("p"))
            .await
            .unwrap();
        p.publish("t", uri.to_owned(), QoS::AtMostOnce)
            .await
            .unwrap();
        assert_eq!(next(&mut sub).await.payload, uri.to_owned());
        p.disconnect().await;
    }
}

struct OnlyAlice;

impl Authenticator for OnlyAlice {
    fn authenticate(&self, info: &ConnectInfo<'_>) -> AuthDecision {
        if info.username == Some("alice") && info.password == Some(&b"secret"[..]) {
            AuthDecision::Allow
        } else {
            AuthDecision::Deny(ReasonCode::BAD_USER_NAME_OR_PASSWORD)
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn authenticator() {
    let opts = BrokerOptions {
        authenticator: Some(Arc::new(OnlyAlice)),
        ..BrokerOptions::default()
    };
    let (ctx, _broker, uri) = setup_with(1, opts).await;
    let bad = ClientOptions {
        username: Some("bob".into()),
        ..ClientOptions::new("b")
    };
    let err = Client::connect(&ctx, &uri, bad.clone()).await.unwrap_err();
    assert!(
        matches!(
            err,
            zzq_client::Error::Refused(ReasonCode::BAD_USER_NAME_OR_PASSWORD)
        ),
        "{err}"
    );
    let v4 = ClientOptions {
        version: Version::V311,
        ..bad
    };
    let err = Client::connect(&ctx, &uri, v4).await.unwrap_err();
    assert!(
        matches!(
            err,
            zzq_client::Error::Refused(ReasonCode::BAD_USER_NAME_OR_PASSWORD)
        ),
        "{err}"
    );
    let good = ClientOptions {
        username: Some("alice".into()),
        password: Some("secret".into()),
        ..ClientOptions::new("a")
    };
    Client::connect(&ctx, &uri, good).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn assigned_client_id() {
    let (ctx, _broker, uri) = setup(1).await;
    let c = Client::connect(&ctx, &uri, ClientOptions::default())
        .await
        .unwrap();
    assert!(
        c.connected()
            .assigned_client_id
            .as_deref()
            .is_some_and(|id| id.starts_with("zzq-"))
    );
    let v4 = ClientOptions {
        version: Version::V311,
        clean_start: false,
        ..ClientOptions::default()
    };
    let err = Client::connect(&ctx, &uri, v4).await.unwrap_err();
    assert!(
        matches!(
            err,
            zzq_client::Error::Refused(ReasonCode::CLIENT_IDENTIFIER_NOT_VALID)
        ),
        "{err}"
    );
    // An assigned ID finds its session again, whichever lane accepted it.
    let (ctx, _broker, uri) = setup(3).await;
    for _ in 0..6 {
        let keep = ClientOptions {
            session_expiry: 60,
            ..ClientOptions::default()
        };
        let c = Client::connect(&ctx, &uri, keep).await.unwrap();
        let id = c.connected().assigned_client_id.clone().unwrap();
        c.disconnect().await;
        let again = ClientOptions {
            clean_start: false,
            session_expiry: 60,
            ..ClientOptions::new(id)
        };
        let c = Client::connect(&ctx, &uri, again).await.unwrap();
        assert!(c.connected().session_present);
    }
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn tls_pub_sub_on_any_lane() {
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_pem = key.cert.pem().into_bytes();
    for lanes in [1, 2] {
        let opts = BrokerOptions {
            transport: zzq_broker::TransportOptions {
                tls: Some(zzq_broker::TlsOptions {
                    cert_chain_pem: Some(cert_pem.clone()),
                    key_pem: Some(key.signing_key.serialize_pem().into_bytes()),
                    ..Default::default()
                }),
            },
            ..BrokerOptions::default()
        };
        let (ctx, broker, _) = setup_with(lanes, opts).await;
        let uri = broker.bind("tls://127.0.0.1:0").await.unwrap().to_string();
        let tls = |id: &str| ClientOptions {
            transport: zzq_client::TransportOptions {
                tls: Some(zzq_client::TlsOptions {
                    trust_pem: Some(cert_pem.clone()),
                    server_name: Some("localhost".to_owned()),
                    ..Default::default()
                }),
            },
            ..ClientOptions::new(id)
        };
        let mut sub = Client::connect(&ctx, &uri, tls("tls-sub")).await.unwrap();
        sub.subscribe("secure/#", QoS::AtLeastOnce).await.unwrap();
        let publ = Client::connect(&ctx, &uri, tls("tls-pub")).await.unwrap();
        for i in 0..20 {
            publ.publish("secure/x", format!("{i}"), QoS::AtLeastOnce)
                .await
                .unwrap();
        }
        for i in 0..20 {
            assert_eq!(next(&mut sub).await.payload, format!("{i}"));
        }
        let untrusted = Client::connect(&ctx, &uri, ClientOptions::new("x")).await;
        assert!(
            untrusted.is_err(),
            "connect without trust anchors must fail"
        );
    }
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn tls_client_skips_verification_only_when_asked() {
    let (server_tls, _) = test_tls();
    let opts = BrokerOptions {
        transport: server_tls,
        ..BrokerOptions::default()
    };
    let (ctx, broker, _) = setup_with(1, opts).await;
    let uri = broker.bind("tls://127.0.0.1:0").await.unwrap().to_string();
    let tls = |danger| ClientOptions {
        transport: zzq_client::TransportOptions {
            tls: Some(zzq_client::TlsOptions {
                danger_accept_invalid_certs: danger,
                ..Default::default()
            }),
        },
        ..ClientOptions::new("anyone")
    };
    assert!(Client::connect(&ctx, &uri, tls(false)).await.is_err());
    let c = Client::connect(&ctx, &uri, tls(true)).await.unwrap();
    c.publish("t", "x", QoS::AtLeastOnce).await.unwrap();
}

mod mqtt_sn {
    use super::*;
    use tokio::net::UdpSocket;
    use zzq_proto::sn::{self, Filter, Flags, Packet, SnQoS, Topic};

    async fn send(s: &UdpSocket, p: &Packet<'_>) {
        let mut out = Vec::new();
        sn::encode(p, &mut out);
        s.send(&out).await.unwrap();
    }

    async fn recv(s: &UdpSocket) -> Vec<u8> {
        let mut buf = vec![0; 2048];
        let n = timeout(WAIT, s.recv(&mut buf))
            .await
            .expect("no datagram")
            .unwrap();
        buf.truncate(n);
        buf
    }

    fn qos(q: SnQoS) -> Flags {
        Flags {
            qos: q,
            ..Flags::default()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    #[expect(clippy::too_many_lines, reason = "one end-to-end story")]
    async fn gateway_round_trip() {
        let (ctx, broker, uri) = setup(2).await;
        let udp = broker.bind("udp://127.0.0.1:0").await.unwrap();
        let addr = udp.socket_addr_string().unwrap();
        let mut mqtt = client(&ctx, &uri, "mqtt-side", Version::V5).await;
        mqtt.subscribe("sn/#", QoS::AtLeastOnce).await.unwrap();
        mqtt.subscribe("ab", QoS::AtMostOnce).await.unwrap();

        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        s.connect(&addr).await.unwrap();
        let flags = Flags {
            clean_session: true,
            ..Flags::default()
        };
        send(
            &s,
            &Packet::Connect {
                flags,
                duration: 30,
                client_id: "sn-1",
            },
        )
        .await;
        assert_eq!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::ConnAck { code: sn::ACCEPTED }
        );

        send(
            &s,
            &Packet::Register {
                topic_id: 0,
                msg_id: 1,
                topic: "sn/temp",
            },
        )
        .await;
        let reg = recv(&s).await;
        let Packet::RegAck {
            topic_id,
            msg_id: 1,
            code: sn::ACCEPTED,
        } = sn::decode(&reg).unwrap().0
        else {
            panic!("expected REGACK");
        };

        let data = b"21.5";
        send(
            &s,
            &Packet::Publish {
                flags: qos(SnQoS::AtMostOnce),
                topic: Topic::Id(topic_id),
                msg_id: 0,
                data,
            },
        )
        .await;
        let m = next(&mut mqtt).await;
        assert_eq!((m.topic.as_str(), &m.payload[..]), ("sn/temp", &data[..]));

        send(
            &s,
            &Packet::Publish {
                flags: qos(SnQoS::AtLeastOnce),
                topic: Topic::Id(topic_id),
                msg_id: 2,
                data: b"22",
            },
        )
        .await;
        let ack = recv(&s).await;
        assert_eq!(
            sn::decode(&ack).unwrap().0,
            Packet::PubAck {
                topic_id,
                msg_id: 2,
                code: sn::ACCEPTED
            }
        );
        assert_eq!(next(&mut mqtt).await.payload, "22");

        send(
            &s,
            &Packet::Subscribe {
                flags: qos(SnQoS::AtLeastOnce),
                msg_id: 3,
                topic: Filter::Name("cmd/x"),
            },
        )
        .await;
        let suback = recv(&s).await;
        let Packet::SubAck {
            topic_id: cmd_id,
            msg_id: 3,
            code: sn::ACCEPTED,
            ..
        } = sn::decode(&suback).unwrap().0
        else {
            panic!("expected SUBACK");
        };
        assert_ne!(cmd_id, 0);
        mqtt.publish("cmd/x", "on", QoS::AtLeastOnce).await.unwrap();
        let publ = recv(&s).await;
        let Packet::Publish {
            topic: Topic::Id(id),
            data: b"on",
            msg_id,
            flags,
        } = sn::decode(&publ).unwrap().0
        else {
            panic!("expected PUBLISH");
        };
        assert_eq!((id, flags.qos), (cmd_id, SnQoS::AtLeastOnce));
        send(
            &s,
            &Packet::PubAck {
                topic_id: id,
                msg_id,
                code: sn::ACCEPTED,
            },
        )
        .await;

        send(
            &s,
            &Packet::Subscribe {
                flags: qos(SnQoS::AtMostOnce),
                msg_id: 4,
                topic: Filter::Name("w/#"),
            },
        )
        .await;
        recv(&s).await;
        mqtt.publish("w/a", "wild", QoS::AtMostOnce).await.unwrap();
        let reg = recv(&s).await;
        let Packet::Register {
            topic_id: wid,
            msg_id: reg_id,
            topic: "w/a",
        } = sn::decode(&reg).unwrap().0
        else {
            panic!("expected REGISTER");
        };
        send(
            &s,
            &Packet::RegAck {
                topic_id: wid,
                msg_id: reg_id,
                code: sn::ACCEPTED,
            },
        )
        .await;
        let publ = recv(&s).await;
        assert!(
            matches!(sn::decode(&publ).unwrap().0, Packet::Publish { topic: Topic::Id(i), data: b"wild", .. } if i == wid)
        );

        let anon = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        anon.connect(&addr).await.unwrap();
        send(
            &anon,
            &Packet::Publish {
                flags: qos(SnQoS::NoConnection),
                topic: Topic::Short(*b"ab"),
                msg_id: 0,
                data: b"q-1",
            },
        )
        .await;
        let m = next(&mut mqtt).await;
        assert_eq!((m.topic.as_str(), &m.payload[..]), ("ab", &b"q-1"[..]));

        send(&s, &Packet::PingReq { client_id: None }).await;
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, Packet::PingResp);
        send(&s, &Packet::Disconnect { duration: None }).await;
        assert_eq!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::Disconnect { duration: None }
        );
    }

    async fn sn_client(addr: &str, id: &str) -> UdpSocket {
        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        s.connect(addr).await.unwrap();
        let flags = Flags {
            clean_session: true,
            ..Flags::default()
        };
        send(
            &s,
            &Packet::Connect {
                flags,
                duration: 30,
                client_id: id,
            },
        )
        .await;
        assert_eq!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::ConnAck { code: sn::ACCEPTED }
        );
        s
    }

    async fn subscribe(s: &UdpSocket, topic: &str) -> u16 {
        send(
            s,
            &Packet::Subscribe {
                flags: qos(SnQoS::AtLeastOnce),
                msg_id: 1,
                topic: Filter::Name(topic),
            },
        )
        .await;
        let ack = recv(s).await;
        let Packet::SubAck {
            topic_id,
            code: sn::ACCEPTED,
            ..
        } = sn::decode(&ack).unwrap().0
        else {
            panic!("expected SUBACK");
        };
        topic_id
    }

    async fn quiet(s: &UdpSocket) {
        let mut buf = [0u8; 64];
        assert!(
            timeout(Duration::from_millis(300), s.recv(&mut buf))
                .await
                .is_err(),
            "unexpected datagram"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sleeping_client_gets_buffered_messages() {
        let (ctx, broker, uri) = setup(1).await;
        let addr = broker
            .bind("udp://127.0.0.1:0")
            .await
            .unwrap()
            .socket_addr_string()
            .unwrap();
        let s = sn_client(&addr, "sleeper").await;
        let id = subscribe(&s, "sleep/t").await;
        send(&s, &Packet::Disconnect { duration: Some(60) }).await;
        assert_eq!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::Disconnect { duration: None }
        );
        let publ = client(&ctx, &uri, "p", Version::V5).await;
        publ.publish("sleep/t", "a", QoS::AtLeastOnce)
            .await
            .unwrap();
        quiet(&s).await;
        send(
            &s,
            &Packet::PingReq {
                client_id: Some("sleeper"),
            },
        )
        .await;
        let publish = recv(&s).await;
        let Packet::Publish {
            topic: Topic::Id(got),
            data: b"a",
            msg_id,
            ..
        } = sn::decode(&publish).unwrap().0
        else {
            panic!("expected buffered PUBLISH");
        };
        assert_eq!(got, id);
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, Packet::PingResp);
        send(
            &s,
            &Packet::PubAck {
                topic_id: id,
                msg_id,
                code: sn::ACCEPTED,
            },
        )
        .await;
        quiet(&s).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn unacked_publish_is_resent_with_dup() {
        let opts = BrokerOptions {
            sn: SnOptions {
                retry: Duration::from_millis(200),
                ..SnOptions::default()
            },
            ..BrokerOptions::default()
        };
        let (ctx, broker, uri) = setup_with(1, opts).await;
        let addr = broker
            .bind("udp://127.0.0.1:0")
            .await
            .unwrap()
            .socket_addr_string()
            .unwrap();
        let s = sn_client(&addr, "lossy").await;
        let id = subscribe(&s, "lossy/t").await;
        let publ = client(&ctx, &uri, "p", Version::V5).await;
        publ.publish("lossy/t", "x", QoS::AtLeastOnce)
            .await
            .unwrap();
        let first = recv(&s).await;
        let Packet::Publish { flags, msg_id, .. } = sn::decode(&first).unwrap().0 else {
            panic!("expected PUBLISH");
        };
        assert!(!flags.dup);
        let again = recv(&s).await;
        let Packet::Publish {
            flags,
            msg_id: same,
            ..
        } = sn::decode(&again).unwrap().0
        else {
            panic!("expected resent PUBLISH");
        };
        assert!(flags.dup);
        assert_eq!(same, msg_id);
        send(
            &s,
            &Packet::PubAck {
                topic_id: id,
                msg_id,
                code: sn::ACCEPTED,
            },
        )
        .await;
        quiet(&s).await;
    }

    async fn bind_sn(opts: BrokerOptions) -> (Context, Broker, String, String) {
        let (ctx, broker, uri) = setup_with(1, opts).await;
        let addr = broker
            .bind("udp://127.0.0.1:0")
            .await
            .unwrap()
            .socket_addr_string()
            .unwrap();
        (ctx, broker, uri, addr)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invalid_short_topics_are_refused_and_the_client_stays() {
        let (_ctx, _broker, _uri, addr) = bind_sn(BrokerOptions::default()).await;
        let s = sn_client(&addr, "short").await;
        let publish = Packet::Publish {
            flags: qos(SnQoS::AtLeastOnce),
            topic: Topic::Short(*b"a#"),
            msg_id: 7,
            data: b"x",
        };
        send(&s, &publish).await;
        let bytes = recv(&s).await;
        let ack = sn::decode(&bytes).unwrap().0;
        assert!(
            matches!(ack, Packet::PubAck { msg_id: 7, code, .. } if code == sn::REJECTED_INVALID_TOPIC_ID),
            "{ack:?}"
        );
        let subscribe = Packet::Subscribe {
            flags: qos(SnQoS::AtLeastOnce),
            msg_id: 8,
            topic: Filter::Short(*b"a#"),
        };
        send(&s, &subscribe).await;
        let bytes = recv(&s).await;
        let ack = sn::decode(&bytes).unwrap().0;
        assert!(
            matches!(ack, Packet::SubAck { msg_id: 8, code, .. } if code == sn::REJECTED_INVALID_TOPIC_ID),
            "{ack:?}"
        );
        let unsubscribe = Packet::Unsubscribe {
            flags: Flags::default(),
            msg_id: 9,
            topic: Filter::Short(*b"a#"),
        };
        send(&s, &unsubscribe).await;
        assert_eq!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::UnsubAck { msg_id: 9 }
        );
        // A valid one goes to the broker and back.
        let unsubscribe = Packet::Unsubscribe {
            flags: Flags::default(),
            msg_id: 10,
            topic: Filter::Short(*b"ab"),
        };
        send(&s, &unsubscribe).await;
        assert_eq!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::UnsubAck { msg_id: 10 }
        );
        // The client's broker connection is still there.
        send(&s, &Packet::PingReq { client_id: None }).await;
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, Packet::PingResp);
    }

    async fn register(s: &UdpSocket, msg_id: u16, topic: &str) -> (u16, u8) {
        send(
            s,
            &Packet::Register {
                topic_id: 0,
                msg_id,
                topic,
            },
        )
        .await;
        let ack = recv(s).await;
        let Packet::RegAck { topic_id, code, .. } = sn::decode(&ack).unwrap().0 else {
            panic!("expected REGACK");
        };
        (topic_id, code)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn topic_ids_run_out_without_stalling() {
        let opts = BrokerOptions {
            sn: SnOptions {
                max_topics: 2,
                ..SnOptions::default()
            },
            max_inflight: 1,
            ..BrokerOptions::default()
        };
        let (ctx, _broker, uri, addr) = bind_sn(opts).await;
        let s = sn_client(&addr, "few").await;
        let (id1, code) = register(&s, 1, "a/1").await;
        assert_eq!(code, sn::ACCEPTED);
        assert_eq!(register(&s, 2, "a/2").await.1, sn::ACCEPTED);
        assert_eq!(register(&s, 3, "a/3").await, (0, sn::REJECTED_CONGESTION));
        send(
            &s,
            &Packet::Subscribe {
                flags: qos(SnQoS::ExactlyOnce),
                msg_id: 4,
                topic: Filter::Name("a/#"),
            },
        )
        .await;
        assert!(matches!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::SubAck {
                code: sn::ACCEPTED,
                ..
            }
        ));
        // No ID left for a/3: dropped, but acked, so the one-message window
        // lets a/1 through.
        let publ = client(&ctx, &uri, "p", Version::V5).await;
        for topic in ["a/3", "a/1"] {
            publ.publish(topic, topic, QoS::ExactlyOnce).await.unwrap();
        }
        let got = recv(&s).await;
        let Packet::Publish {
            topic: Topic::Id(id),
            data: b"a/1",
            ..
        } = sn::decode(&got).unwrap().0
        else {
            panic!("expected PUBLISH on a/1");
        };
        assert_eq!(id, id1);
    }

    /// Denies publishes to `plant/valve`.
    struct NoValve;

    impl zzq_broker::Authorizer for NoValve {
        fn may_publish(&self, _client: &zzq_broker::ClientInfo<'_>, topic: &str) -> bool {
            topic != "plant/valve"
        }

        fn may_subscribe(&self, _client: &zzq_broker::ClientInfo<'_>, _filter: &str) -> bool {
            true
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connectionless_publishes_are_authorized() {
        let opts = BrokerOptions {
            authorizer: Some(Arc::new(NoValve)),
            sn: SnOptions {
                predefined: vec![
                    (5, "plant/valve".to_owned()),
                    (6, "$SYS/x".to_owned()),
                    (7, "plant/ok".to_owned()),
                ],
                ..SnOptions::default()
            },
            ..BrokerOptions::default()
        };
        let (ctx, _broker, uri, addr) = bind_sn(opts).await;
        let mut watch = client(&ctx, &uri, "watch", Version::V5).await;
        watch.subscribe("plant/#", QoS::AtMostOnce).await.unwrap();
        watch.subscribe("$SYS/x", QoS::AtMostOnce).await.unwrap();
        let anon = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        anon.connect(&addr).await.unwrap();
        for id in [5, 6, 7] {
            let flags = Flags {
                qos: SnQoS::NoConnection,
                retain: true,
                ..Flags::default()
            };
            send(
                &anon,
                &Packet::Publish {
                    flags,
                    topic: Topic::Predefined(id),
                    msg_id: 0,
                    data: b"open",
                },
            )
            .await;
        }
        assert_eq!(next(&mut watch).await.topic, "plant/ok");
        nothing(&mut watch).await;
        let mut late = client(&ctx, &uri, "late", Version::V5).await;
        late.subscribe("plant/valve", QoS::AtMostOnce)
            .await
            .unwrap();
        nothing(&mut late).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publish_after_register_is_retransmitted() {
        let opts = BrokerOptions {
            sn: SnOptions {
                retry: Duration::from_millis(200),
                ..SnOptions::default()
            },
            ..BrokerOptions::default()
        };
        let (ctx, _broker, uri, addr) = bind_sn(opts).await;
        let s = sn_client(&addr, "reg").await;
        send(
            &s,
            &Packet::Subscribe {
                flags: qos(SnQoS::AtLeastOnce),
                msg_id: 1,
                topic: Filter::Name("r/#"),
            },
        )
        .await;
        recv(&s).await;
        let publ = client(&ctx, &uri, "p", Version::V5).await;
        publ.publish("r/a", "x", QoS::AtLeastOnce).await.unwrap();
        let Packet::Register {
            topic_id, msg_id, ..
        } = sn::decode(&recv(&s).await).unwrap().0
        else {
            panic!("expected REGISTER");
        };
        let regack = Packet::RegAck {
            topic_id,
            msg_id,
            code: sn::ACCEPTED,
        };
        send(&s, &regack).await;
        for dup in [false, true] {
            let Packet::Publish { flags, .. } = sn::decode(&recv(&s).await).unwrap().0 else {
                panic!("expected PUBLISH");
            };
            assert_eq!(flags.dup, dup);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn message_too_large_for_a_datagram_is_dropped_and_acked() {
        let opts = BrokerOptions {
            max_inflight: 1,
            ..BrokerOptions::default()
        };
        let (ctx, _broker, uri, addr) = bind_sn(opts).await;
        let s = sn_client(&addr, "small").await;
        send(
            &s,
            &Packet::Subscribe {
                flags: qos(SnQoS::AtLeastOnce),
                msg_id: 1,
                topic: Filter::Name("big/#"),
            },
        )
        .await;
        recv(&s).await;
        let publ = client(&ctx, &uri, "p", Version::V5).await;
        publ.publish("big/a", vec![b'x'; 70_000], QoS::AtLeastOnce)
            .await
            .unwrap();
        // Acked to the broker, so the one-message window lets this through.
        publ.publish("big/a", "small", QoS::AtLeastOnce)
            .await
            .unwrap();
        let Packet::Register {
            topic_id, msg_id, ..
        } = sn::decode(&recv(&s).await).unwrap().0
        else {
            panic!("expected REGISTER");
        };
        let regack = Packet::RegAck {
            topic_id,
            msg_id,
            code: sn::ACCEPTED,
        };
        send(&s, &regack).await;
        let got = recv(&s).await;
        assert!(matches!(
            sn::decode(&got).unwrap().0,
            Packet::Publish { data: b"small", .. }
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn predefined_topics_and_publishes_held_for_register() {
        let opts = BrokerOptions {
            sn: SnOptions {
                predefined: vec![(9, "pre/t".to_owned())],
                ..SnOptions::default()
            },
            ..BrokerOptions::default()
        };
        let (ctx, _broker, uri, addr) = bind_sn(opts).await;
        let s = sn_client(&addr, "pre").await;
        for (msg_id, topic) in [(1, Filter::Predefined(9)), (2, Filter::Name("r/#"))] {
            let sub = Packet::Subscribe {
                flags: qos(SnQoS::AtMostOnce),
                msg_id,
                topic,
            };
            send(&s, &sub).await;
            recv(&s).await;
        }
        let publ = client(&ctx, &uri, "p", Version::V5).await;
        publ.publish("pre/t", "known", QoS::AtMostOnce)
            .await
            .unwrap();
        assert!(matches!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::Publish {
                topic: Topic::Predefined(9),
                data: b"known",
                ..
            }
        ));
        // Both arrive before the REGACK: the second waits with the first.
        for payload in ["one", "two"] {
            publ.publish("r/a", payload, QoS::AtMostOnce).await.unwrap();
        }
        let Packet::Register {
            topic_id, msg_id, ..
        } = sn::decode(&recv(&s).await).unwrap().0
        else {
            panic!("expected REGISTER");
        };
        tokio::time::sleep(Duration::from_millis(200)).await;
        let regack = Packet::RegAck {
            topic_id,
            msg_id,
            code: sn::ACCEPTED,
        };
        send(&s, &regack).await;
        for want in [&b"one"[..], b"two"] {
            let got = recv(&s).await;
            let Packet::Publish { data, .. } = sn::decode(&got).unwrap().0 else {
                panic!("expected PUBLISH");
            };
            assert_eq!(data, want);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resent_connect_gets_connack_again() {
        let (ctx, _broker, uri, addr) = bind_sn(BrokerOptions::default()).await;
        let mut watch = client(&ctx, &uri, "watch", Version::V5).await;
        watch.subscribe("sn/will", QoS::AtMostOnce).await.unwrap();
        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        s.connect(&addr).await.unwrap();
        let connect = Packet::Connect {
            flags: Flags {
                clean_session: true,
                will: true,
                ..Flags::default()
            },
            duration: 30,
            client_id: "lossy-link",
        };
        send(&s, &connect).await;
        // The first WILLTOPICREQ got lost: the client sends CONNECT again.
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, Packet::WillTopicReq);
        send(&s, &connect).await;
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, Packet::WillTopicReq);
        let will_topic = Packet::WillTopic {
            will: Some((Flags::default(), "sn/will")),
        };
        send(&s, &will_topic).await;
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, Packet::WillMsgReq);
        send(&s, &Packet::WillMsg { message: b"gone" }).await;
        let connack = Packet::ConnAck { code: sn::ACCEPTED };
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, connack);
        // The CONNACK got lost too.
        send(&s, &connect).await;
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, connack);
        send(&s, &Packet::PingReq { client_id: None }).await;
        assert_eq!(sn::decode(&recv(&s).await).unwrap().0, Packet::PingResp);
        nothing(&mut watch).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sleep_lasts_no_longer_than_the_keep_alive_cap() {
        let opts = BrokerOptions {
            sn: SnOptions {
                max_keep_alive: 1,
                ..SnOptions::default()
            },
            ..BrokerOptions::default()
        };
        let (_ctx, broker, _uri, addr) = bind_sn(opts).await;
        let s = sn_client(&addr, "sleepy").await;
        send(
            &s,
            &Packet::Disconnect {
                duration: Some(u16::MAX),
            },
        )
        .await;
        assert_eq!(
            sn::decode(&recv(&s).await).unwrap().0,
            Packet::Disconnect { duration: None }
        );
        // Without a ping, the gateway lets go once 1.5 times the cap passed.
        let deadline = tokio::time::Instant::now() + WAIT;
        while broker.stats().await.unwrap().connections > 0 {
            assert!(tokio::time::Instant::now() < deadline, "still asleep");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(feature = "quic")]
#[tokio::test(flavor = "multi_thread")]
async fn quic_refusal_reaches_the_client() {
    let (server_tls, client_tls) = test_tls();
    let opts = BrokerOptions {
        transport: server_tls,
        max_connections: 1,
        ..BrokerOptions::default()
    };
    let (ctx, broker, _tcp) = setup_with(1, opts).await;
    let uri = broker.bind("quic://127.0.0.1:0").await.unwrap().to_string();
    let quic = |id: &str| ClientOptions {
        transport: client_tls.clone(),
        ..ClientOptions::new(id)
    };
    let _first = Client::connect(&ctx, &uri, quic("one")).await.unwrap();
    let refused = Client::connect(&ctx, &uri, quic("two")).await;
    assert!(
        matches!(
            refused,
            Err(zzq_client::Error::Refused(ReasonCode::QUOTA_EXCEEDED))
        ),
        "{refused:?}"
    );
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn websocket_plain_and_secure() {
    let (server_tls, client_tls) = test_tls();
    for lanes in [1, 2] {
        let opts = BrokerOptions {
            transport: server_tls.clone(),
            ..BrokerOptions::default()
        };
        let (ctx, broker, tcp) = setup_with(lanes, opts).await;
        let ws = broker
            .bind("ws://127.0.0.1:0/mqtt")
            .await
            .unwrap()
            .to_string();
        let wss = broker
            .bind("wss://127.0.0.1:0/mqtt")
            .await
            .unwrap()
            .to_string();
        let mut sub = client(&ctx, &tcp, "tcp-sub", Version::V5).await;
        sub.subscribe("web/#", QoS::AtLeastOnce).await.unwrap();
        for (uri, transport) in [
            (&ws, zzq_client::TransportOptions::default()),
            (&wss, client_tls.clone()),
        ] {
            let opts = ClientOptions {
                transport,
                ..ClientOptions::new(format!("web-{lanes}"))
            };
            let mut web = Client::connect(&ctx, uri, opts).await.unwrap();
            web.subscribe("echo", QoS::AtMostOnce).await.unwrap();
            let big = vec![b'z'; 100_000];
            web.publish("web/big", big.clone(), QoS::AtLeastOnce)
                .await
                .unwrap();
            assert_eq!(next(&mut sub).await.payload, big);
            sub.publish("echo", "back", QoS::AtMostOnce).await.unwrap();
            assert_eq!(next(&mut web).await.payload, "back");
            web.disconnect().await;
        }
    }
}

#[cfg(feature = "quic")]
#[tokio::test(flavor = "multi_thread")]
async fn quic_pub_sub_on_any_lane() {
    let (server_tls, client_tls) = test_tls();
    for lanes in [1, 2] {
        let opts = BrokerOptions {
            transport: server_tls.clone(),
            ..BrokerOptions::default()
        };
        let (ctx, broker, tcp) = setup_with(lanes, opts).await;
        let uri = broker.bind("quic://127.0.0.1:0").await.unwrap().to_string();
        let mut watcher = client(&ctx, &tcp, "quic-watch", Version::V5).await;
        watcher.subscribe("quic/#", QoS::AtLeastOnce).await.unwrap();
        for id in ["quic-a", "quic-b", "quic-c"] {
            let opts = ClientOptions {
                transport: client_tls.clone(),
                will: Some(LastWill {
                    topic: "quic/will".to_owned(),
                    payload: "gone".into(),
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    delay: 0,
                }),
                ..ClientOptions::new(id)
            };
            let mut c = Client::connect(&ctx, &uri, opts).await.unwrap();
            c.subscribe("echo/#", QoS::AtLeastOnce).await.unwrap();
            for i in 0..200 {
                c.publish("quic/x", format!("{i}"), QoS::AtLeastOnce)
                    .await
                    .unwrap();
            }
            for i in 0..200 {
                assert_eq!(next(&mut watcher).await.payload, format!("{i}"));
            }
            let big = vec![b'q'; 200_000];
            watcher
                .publish("echo/big", big.clone(), QoS::AtLeastOnce)
                .await
                .unwrap();
            assert_eq!(next(&mut c).await.payload, big);
            c.disconnect().await;
        }
        nothing(&mut watcher).await;
        let untrusted = Client::connect(&ctx, &uri, ClientOptions::new("x")).await;
        assert!(
            untrusted.is_err(),
            "connect without trust anchors must fail"
        );
    }
}

/// Allows only clients that presented `0`.
#[cfg(feature = "tls")]
struct CertIs(Vec<u8>);

#[cfg(feature = "tls")]
impl Authenticator for CertIs {
    fn authenticate(&self, info: &ConnectInfo<'_>) -> AuthDecision {
        if info.peer_cert == Some(self.0.as_slice()) {
            AuthDecision::Allow
        } else {
            AuthDecision::Deny(ReasonCode::NOT_AUTHORIZED)
        }
    }
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn mutual_tls_identifies_clients() {
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();
    let signed = |name: &str| {
        let key = KeyPair::generate().unwrap();
        let cert = CertificateParams::new(vec![name.to_owned()])
            .unwrap()
            .signed_by(&key, &ca)
            .unwrap();
        (cert, key)
    };
    let (server_cert, server_key) = signed("localhost");
    let (client_cert, client_key) = signed("device-7");
    let opts = BrokerOptions {
        transport: zzq_broker::TransportOptions {
            tls: Some(zzq_broker::TlsOptions {
                cert_chain_pem: Some(server_cert.pem().into_bytes()),
                key_pem: Some(server_key.serialize_pem().into_bytes()),
                client_ca_pem: Some(ca.pem().into_bytes()),
                ..Default::default()
            }),
        },
        authenticator: Some(Arc::new(CertIs(client_cert.der().to_vec()))),
        ..BrokerOptions::default()
    };
    let (ctx, broker, _) = setup_with(2, opts).await;
    let mut uris = vec![broker.bind("tls://127.0.0.1:0").await.unwrap()];
    if cfg!(feature = "quic") {
        uris.push(broker.bind("quic://127.0.0.1:0").await.unwrap());
    }
    let tls = |with_cert: bool| zzq_client::TlsOptions {
        trust_pem: Some(ca.pem().into_bytes()),
        server_name: Some("localhost".to_owned()),
        cert_chain_pem: with_cert.then(|| client_cert.pem().into_bytes()),
        key_pem: with_cert.then(|| client_key.serialize_pem().into_bytes()),
        ..Default::default()
    };
    for uri in uris.iter().map(ToString::to_string) {
        let opts = |id: &str, with_cert: bool| ClientOptions {
            transport: zzq_client::TransportOptions {
                tls: Some(tls(with_cert)),
            },
            ..ClientOptions::new(id)
        };
        let mut c = Client::connect(&ctx, &uri, opts("device-7", true))
            .await
            .unwrap();
        c.subscribe("m/#", QoS::AtLeastOnce).await.unwrap();
        c.publish("m/1", "hi", QoS::AtLeastOnce).await.unwrap();
        assert_eq!(next(&mut c).await.payload, "hi");
        c.disconnect().await;
        let anonymous = Client::connect(&ctx, &uri, opts("nobody", false)).await;
        assert!(anonymous.is_err(), "{uri}: connect without a client cert");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn retained_messages_survive_restart() {
    let path = std::env::temp_dir().join(format!("zzq-retained-{}.log", std::process::id()));
    let _ = std::fs::remove_file(&path);
    for run in 0..2 {
        let store = Arc::new(zzq_broker::FilePersistence::open(&path).unwrap());
        let opts = BrokerOptions {
            persistence: Some(store.clone()),
            ..BrokerOptions::default()
        };
        let (ctx, broker, uri) = setup_with(2, opts).await;
        if run == 0 {
            let p = client(&ctx, &uri, "p", Version::V5).await;
            for (topic, payload) in [("keep/a", "A"), ("keep/b", "B"), ("keep/b", "")] {
                p.publish_retained(topic, payload, QoS::AtLeastOnce)
                    .await
                    .unwrap();
            }
            store.flush();
        } else {
            let mut s = client(&ctx, &uri, "s", Version::V5).await;
            s.subscribe("keep/#", QoS::AtLeastOnce).await.unwrap();
            let m = next(&mut s).await;
            assert_eq!((m.topic.as_str(), m.retain), ("keep/a", true));
            assert_eq!(m.payload, "A");
            nothing(&mut s).await;
        }
        assert_eq!(store.write_errors(), 0);
        drop(broker);
        drop(ctx);
    }
    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "multi_thread")]
async fn client_reconnects_and_restores_subscriptions() {
    let (ctx1, broker1, uri) = setup(2).await;
    let client_ctx = Context::with_config(&Config::default()).unwrap();
    let opts = ClientOptions {
        session_expiry: 300,
        reconnect: Some(zzq_client::Reconnect {
            min: Duration::from_millis(20),
            max: Duration::from_millis(200),
        }),
        ..ClientOptions::new("roamer")
    };
    let c = Client::connect(&client_ctx, &uri, opts).await.unwrap();
    c.subscribe("rc/#", QoS::AtLeastOnce).await.unwrap();
    // Once shut down, the old broker acks nothing more.
    broker1.shutdown().await.unwrap();
    drop(ctx1);
    // Published while the broker is down: waits, then goes out. Retained,
    // so the watcher gets it even if the client reconnects first.
    let c = Arc::new(tokio::sync::Mutex::new(c));
    let publisher = c.clone();
    let late = tokio::spawn(async move {
        let c = publisher.lock().await;
        c.publish_retained("rc/late", "after", QoS::AtLeastOnce)
            .await
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let ctx2 = Context::with_config(&Config::default()).unwrap();
    let broker2 = Broker::new(&ctx2, BrokerOptions::default()).await.unwrap();
    let mut bound = None;
    for _ in 0..50 {
        if let Ok(ep) = broker2.bind(&uri).await {
            bound = Some(ep);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(bound.is_some(), "rebind {uri}");
    let mut watch = client(&ctx2, &uri, "watch", Version::V5).await;
    watch.subscribe("rc/late", QoS::AtLeastOnce).await.unwrap();
    broker2
        .publish_retained("rc/kept", "retained", QoS::AtLeastOnce)
        .unwrap();
    timeout(WAIT, late).await.unwrap().unwrap().unwrap();
    let m = timeout(WAIT, async {
        loop {
            let m = c.lock().await.recv().await.unwrap();
            if m.topic == "rc/kept" {
                return m;
            }
        }
    })
    .await
    .expect("resubscribed after reconnect");
    assert_eq!(m.payload, "retained");
    let m = next(&mut watch).await;
    assert_eq!(m.payload, "after");
}

#[test]
fn blocking_client_round_trip() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (ctx, _broker, uri) = rt.block_on(setup(2));
    let mut c =
        zzq_client::blocking::Client::connect(&ctx, &uri, ClientOptions::new("blocky")).unwrap();
    c.subscribe("b/#", QoS::ExactlyOnce).unwrap();
    for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
        c.publish("b/x", format!("{}", qos as u8), qos).unwrap();
    }
    for qos in 0..3 {
        let m = c.recv().unwrap();
        assert_eq!((m.payload, m.qos as u8), (format!("{qos}").into(), qos));
    }
    assert!(!c.connected().session_present);
    c.publish_retained("b/kept", "r", QoS::AtLeastOnce).unwrap();
    let m = c.recv().unwrap();
    assert_eq!((m.topic.as_str(), m.retain), ("b/kept", false));
    let codes = c.unsubscribe(&["b/#"]).unwrap();
    assert_eq!(codes, [zzq_client::ReasonCode::SUCCESS]);
    rt.block_on(async {
        let inside = zzq_client::blocking::Client::connect(&ctx, &uri, ClientOptions::new("x"));
        assert!(matches!(inside, Err(zzq_client::Error::InAsync)));
    });
    c.disconnect();
}

#[tokio::test(flavor = "multi_thread")]
async fn client_respects_receive_maximum() {
    let opts = BrokerOptions {
        receive_maximum: 2,
        ..BrokerOptions::default()
    };
    let (ctx, _broker, uri) = setup_with(1, opts).await;
    let mut sub = client(&ctx, &uri, "rm-sub", Version::V5).await;
    sub.subscribe("rm/#", QoS::AtLeastOnce).await.unwrap();
    let publ = Arc::new(client(&ctx, &uri, "rm-pub", Version::V5).await);
    let sends: Vec<_> = (0..50)
        .map(|i| {
            let p = publ.clone();
            tokio::spawn(async move {
                let qos = if i % 2 == 0 {
                    QoS::AtLeastOnce
                } else {
                    QoS::ExactlyOnce
                };
                p.publish("rm/x", format!("{i}"), qos).await
            })
        })
        .collect();
    for s in sends {
        timeout(WAIT, s).await.unwrap().unwrap().unwrap();
    }
    for _ in 0..50 {
        next(&mut sub).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn client_disconnects_while_offline_and_times_out() {
    let (ctx, broker, uri) = setup(1).await;
    let opts = ClientOptions {
        reconnect: Some(zzq_client::Reconnect::default()),
        ..ClientOptions::new("offline")
    };
    let c = Client::connect(&ctx, &uri, opts).await.unwrap();
    drop(broker);
    tokio::time::sleep(Duration::from_millis(100)).await;
    timeout(Duration::from_secs(2), c.disconnect())
        .await
        .expect("disconnect while offline");
    // A listener that never answers CONNECT.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tcp://{}", silent.local_addr().unwrap());
    let opts = ClientOptions {
        connect_timeout: Duration::from_millis(200),
        ..ClientOptions::new("waits")
    };
    let started = std::time::Instant::now();
    assert!(Client::connect(&ctx, &uri, opts).await.is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn bridge_loop_with_small_inbox_does_not_stall() {
    let (ctx, _broker, uri) = setup(2).await;
    let opts = ClientOptions {
        inbox: 8,
        ..ClientOptions::new("bridge")
    };
    let mut bridge = Client::connect(&ctx, &uri, opts).await.unwrap();
    bridge.subscribe("in/#", QoS::AtLeastOnce).await.unwrap();
    let mut out = client(&ctx, &uri, "out", Version::V5).await;
    out.subscribe("out/#", QoS::AtLeastOnce).await.unwrap();
    // All 500 in flight at once, so they pile up in front of the bridge.
    let source = Arc::new(client(&ctx, &uri, "source", Version::V5).await);
    let flood: Vec<_> = (0..500)
        .map(|i| {
            let s = source.clone();
            tokio::spawn(async move { s.publish("in/x", format!("{i}"), QoS::AtLeastOnce).await })
        })
        .collect();
    timeout(Duration::from_secs(20), async {
        for _ in 0..500 {
            let m = bridge.recv().await.unwrap();
            bridge
                .publish("out/x", m.payload, QoS::AtLeastOnce)
                .await
                .unwrap();
        }
    })
    .await
    .expect("bridge stalled");
    for f in flood {
        f.await.unwrap().unwrap();
    }
    for _ in 0..500 {
        next(&mut out).await;
    }
}

#[cfg(feature = "quic")]
#[tokio::test(flavor = "multi_thread")]
async fn quic_clients_on_every_lane() {
    let (server_tls, client_tls) = test_tls();
    let opts = BrokerOptions {
        transport: server_tls,
        ..BrokerOptions::default()
    };
    let (ctx, broker, _) = setup_with(3, opts).await;
    let uri = broker.bind("quic://127.0.0.1:0").await.unwrap().to_string();
    let clients: Vec<_> = (0..12)
        .map(|i| {
            let (ctx, uri, tls) = (ctx.clone(), uri.clone(), client_tls.clone());
            tokio::spawn(async move {
                let opts = ClientOptions {
                    transport: tls,
                    ..ClientOptions::new(format!("q{i}"))
                };
                let mut c = Client::connect(&ctx, &uri, opts).await.unwrap();
                let topic = format!("q/{i}");
                c.subscribe(&topic, QoS::AtLeastOnce).await.unwrap();
                c.publish(&topic, "hi", QoS::AtLeastOnce).await.unwrap();
                assert_eq!(next(&mut c).await.payload, "hi");
                c.disconnect().await;
            })
        })
        .collect();
    for c in clients {
        timeout(WAIT, c).await.unwrap().unwrap();
    }
}

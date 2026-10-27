//! The client against a scripted broker, for flows a real broker does not
//! produce on demand.

#![expect(clippy::unwrap_used, clippy::panic, reason = "tests fail loudly")]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use zzq_client::{Client, ClientOptions, QoS, Reconnect, SubscriptionOptions, Version};
use zzq_proto::encode::{self, AckFields, ConnAckFields, PublishHeader};
use zzq_proto::{Packet, PacketType, decode};

const WAIT: Duration = Duration::from_secs(5);

/// One accepted connection, read packet by packet.
struct Peer {
    s: TcpStream,
    buf: Vec<u8>,
}

impl Peer {
    async fn accept(l: &TcpListener) -> Peer {
        let s = timeout(WAIT, l.accept()).await.unwrap().unwrap().0;
        let mut peer = Peer { s, buf: Vec::new() };
        assert_eq!(peer.packet().await.0 >> 4, 1, "CONNECT");
        let mut out = Vec::new();
        encode::connack(&ConnAckFields::default(), Version::V5, &mut out);
        peer.s.write_all(&out).await.unwrap();
        peer
    }

    /// Next packet as its first byte and its bytes.
    async fn packet(&mut self) -> (u8, Vec<u8>) {
        loop {
            if let Ok(Some((_, n))) = decode(&self.buf, Version::V5, usize::MAX) {
                let p: Vec<u8> = self.buf.drain(..n).collect();
                return (p[0], p);
            }
            let mut chunk = [0u8; 4096];
            let n = timeout(WAIT, self.s.read(&mut chunk))
                .await
                .unwrap()
                .unwrap();
            assert!(n > 0, "client closed");
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    async fn ack(&mut self, kind: PacketType, pkid: u16) {
        let mut out = Vec::new();
        encode::ack(&AckFields::new(kind, pkid), Version::V5, &mut out);
        self.s.write_all(&out).await.unwrap();
    }

    async fn suback(&mut self, packet: &[u8]) {
        let Ok(Some((Packet::Subscribe(s), _))) = decode(packet, Version::V5, usize::MAX) else {
            panic!("expected SUBSCRIBE");
        };
        let mut out = Vec::new();
        let fields = encode::SubAckFields {
            kind: PacketType::SubAck,
            pkid: s.pkid,
            properties: &[],
            codes: &[1],
        };
        encode::suback(&fields, Version::V5, &mut out);
        self.s.write_all(&out).await.unwrap();
    }
}

async fn listen() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("tcp://{}", l.local_addr().unwrap());
    (l, uri)
}

#[tokio::test]
async fn reconnect_resends_unacked_publishes_and_restores_subscriptions() {
    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let opts = ClientOptions {
        session_expiry: 60,
        reconnect: Some(Reconnect {
            min: Duration::from_millis(10),
            max: Duration::from_millis(50),
        }),
        ..ClientOptions::new("resend")
    };
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move { Client::connect(&ctx, &uri, opts).await.unwrap() }
    });
    let mut first = Peer::accept(&l).await;
    let c = std::sync::Arc::new(connecting.await.unwrap());
    // Two Subscription Identifiers: one SUBSCRIBE each on restore.
    for id in [1, 2] {
        let filter = format!("f/{id}");
        let sub = tokio::spawn({
            let c = c.clone();
            async move {
                let opts = SubscriptionOptions::from(QoS::AtLeastOnce);
                c.subscribe_with(&[(&filter, opts)], id).await.unwrap()
            }
        });
        let (_, packet) = first.packet().await;
        first.suback(&packet).await;
        sub.await.unwrap();
    }
    let publish = tokio::spawn({
        let c = c.clone();
        async move { c.publish("t", "x", QoS::AtLeastOnce).await }
    });
    let (byte1, _) = first.packet().await;
    assert_eq!((byte1 >> 4, byte1 & 0x08), (3, 0), "first PUBLISH");
    drop(first);
    // The broker lost the session: subscriptions come back, then the
    // unacked PUBLISH with DUP.
    let mut second = Peer::accept(&l).await;
    let mut subscribes = 0;
    loop {
        let (byte1, packet) = second.packet().await;
        match byte1 >> 4 {
            8 => {
                subscribes += 1;
                second.suback(&packet).await;
            }
            3 => {
                assert_eq!(byte1 & 0x08, 0x08, "DUP");
                let pkid = u16::from_be_bytes([packet[5], packet[6]]);
                second.ack(PacketType::PubAck, pkid).await;
                break;
            }
            other => panic!("unexpected packet type {other}"),
        }
    }
    assert_eq!(subscribes, 2);
    publish.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_resent_qos2_publish_already_taken_is_acked_again() {
    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move {
            Client::connect(&ctx, &uri, ClientOptions::new("qos2"))
                .await
                .unwrap()
        }
    });
    let mut peer = Peer::accept(&l).await;
    let mut c = connecting.await.unwrap();
    for dup in [false, true] {
        let h = PublishHeader {
            version: Version::V5,
            dup,
            qos: QoS::ExactlyOnce,
            retain: false,
            topic: "q2",
            pkid: 5,
            properties: &[],
        };
        let mut out = Vec::new();
        encode::publish(&h, b"once", &mut out);
        peer.s.write_all(&out).await.unwrap();
        if !dup {
            assert_eq!(
                timeout(WAIT, c.recv()).await.unwrap().unwrap().payload,
                "once"
            );
        }
        let (byte1, _) = peer.packet().await;
        assert_eq!(byte1 >> 4, 5, "PUBREC");
    }
    peer.ack(PacketType::PubRel, 5).await;
    assert_eq!(peer.packet().await.0 >> 4, 7, "PUBCOMP");
    assert!(timeout(Duration::from_millis(200), c.recv()).await.is_err());
}

#[tokio::test]
async fn disconnect_while_offline_behind_queued_publishes_returns_at_once() {
    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let opts = ClientOptions {
        reconnect: Some(Reconnect {
            min: Duration::from_secs(60),
            max: Duration::from_secs(60),
        }),
        ..ClientOptions::new("offline")
    };
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move { Client::connect(&ctx, &uri, opts).await.unwrap() }
    });
    let peer = Peer::accept(&l).await;
    let c = connecting.await.unwrap();
    drop(peer);
    // The session sees the connection go and waits a minute to reconnect.
    tokio::time::sleep(Duration::from_millis(200)).await;
    c.publish("t", "queued", QoS::AtMostOnce).await.unwrap();
    // The session has seen the publish by the time DISCONNECT comes.
    tokio::time::sleep(Duration::from_millis(100)).await;
    timeout(WAIT, c.disconnect()).await.unwrap();
}

#[tokio::test]
async fn a_publish_waiting_for_its_ack_fails_once_the_connection_is_lost() {
    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move {
            Client::connect(&ctx, &uri, ClientOptions::new("lost"))
                .await
                .unwrap()
        }
    });
    let mut peer = Peer::accept(&l).await;
    let c = std::sync::Arc::new(connecting.await.unwrap());
    let publish = tokio::spawn({
        let c = c.clone();
        async move { c.publish("t", "x", QoS::AtLeastOnce).await }
    });
    assert_eq!(peer.packet().await.0 >> 4, 3, "PUBLISH");
    drop(peer);
    let res = timeout(WAIT, publish).await.unwrap().unwrap();
    assert!(matches!(res, Err(zzq_client::Error::Closed)), "{res:?}");
    assert!(matches!(
        c.publish("t", "x", QoS::AtLeastOnce).await,
        Err(zzq_client::Error::Closed)
    ));
}

#[tokio::test]
async fn a_dropped_ack_wait_keeps_its_publish() {
    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move {
            Client::connect(&ctx, &uri, ClientOptions::new("dropped"))
                .await
                .unwrap()
        }
    });
    let mut peer = Peer::accept(&l).await;
    let h = connecting.await.unwrap().handle();
    let pkid = |packet: &[u8]| match decode(packet, Version::V5, usize::MAX) {
        Ok(Some((Packet::Publish(p), _))) => p.pkid,
        other => panic!("expected PUBLISH, got {other:?}"),
    };
    drop(h.enqueue("t", "x", QoS::AtLeastOnce, false).await.unwrap());
    let (_, first) = peer.packet().await;
    peer.ack(PacketType::PubAck, pkid(&first)).await;
    let next = tokio::spawn({
        let h = h.clone();
        async move { h.publish("t", "y", QoS::AtLeastOnce).await }
    });
    let (_, second) = peer.packet().await;
    peer.ack(PacketType::PubAck, pkid(&second)).await;
    timeout(WAIT, next).await.unwrap().unwrap().unwrap();
    let acked = h.enqueue("t", "z", QoS::AtMostOnce, false).await.unwrap();
    acked.await.unwrap();
}

#[tokio::test]
async fn publishes_past_the_receive_maximum_hold_back_the_caller() {
    use zzq_proto::Property;
    use zzq_proto::property::{self, RECEIVE_MAXIMUM};

    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move {
            Client::connect(&ctx, &uri, ClientOptions::new("rm"))
                .await
                .unwrap()
        }
    });
    // A broker that allows one QoS 1 publish in flight and never acks.
    let s = timeout(WAIT, l.accept()).await.unwrap().unwrap().0;
    let mut peer = Peer { s, buf: Vec::new() };
    assert_eq!(peer.packet().await.0 >> 4, 1, "CONNECT");
    let mut props = Vec::new();
    property::encode_all(&[Property::U16(RECEIVE_MAXIMUM, 1)], &mut props);
    let ack = ConnAckFields {
        properties: &props,
        ..ConnAckFields::default()
    };
    let mut out = Vec::new();
    encode::connack(&ack, Version::V5, &mut out);
    peer.s.write_all(&out).await.unwrap();
    let client = connecting.await.unwrap();
    let handle = client.handle();
    // The acks are never awaited: only room in the session may stop this.
    let queued = timeout(Duration::from_secs(1), async {
        for _ in 0..10_000 {
            let acked = handle
                .enqueue("t", vec![0u8; 1024], QoS::AtLeastOnce, false)
                .await
                .unwrap();
            drop(acked);
        }
    })
    .await;
    assert!(
        queued.is_err(),
        "10 MB waited in the session for the broker"
    );
    drop(peer);
}

#[tokio::test]
async fn a_message_from_a_lost_session_is_not_acked_on_the_next() {
    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let opts = ClientOptions {
        session_expiry: 60,
        reconnect: Some(Reconnect {
            min: Duration::from_millis(10),
            max: Duration::from_millis(50),
        }),
        ..ClientOptions::new("stale")
    };
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move { Client::connect(&ctx, &uri, opts).await.unwrap() }
    });
    let mut first = Peer::accept(&l).await;
    let mut c = connecting.await.unwrap();
    let h = c.handle();
    let sub = tokio::spawn(async move { h.subscribe("t", QoS::AtLeastOnce).await.unwrap() });
    let (_, packet) = first.packet().await;
    first.suback(&packet).await;
    sub.await.unwrap();
    let h = PublishHeader {
        version: Version::V5,
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "t",
        pkid: 7,
        properties: &[],
    };
    let mut out = Vec::new();
    encode::publish(&h, b"old", &mut out);
    first.s.write_all(&out).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(first);
    // The broker lost the session: its packet ID 7 may mean another
    // message now.
    let mut second = Peer::accept(&l).await;
    let (_, packet) = second.packet().await;
    second.suback(&packet).await;
    let m = timeout(WAIT, c.recv()).await.unwrap().unwrap();
    assert_eq!(m.payload, "old");
    let mut chunk = [0u8; 64];
    let got = timeout(Duration::from_millis(300), second.s.read(&mut chunk)).await;
    assert!(got.is_err(), "acked on the new connection: {got:?}");
}

#[tokio::test]
async fn a_released_qos2_publish_is_done_when_the_broker_lost_the_session() {
    let (l, uri) = listen().await;
    let ctx = zzq_client::Context::new().unwrap();
    let opts = ClientOptions {
        session_expiry: 60,
        reconnect: Some(Reconnect {
            min: Duration::from_millis(10),
            max: Duration::from_millis(50),
        }),
        ..ClientOptions::new("rel")
    };
    let connecting = tokio::spawn({
        let ctx = ctx.clone();
        async move { Client::connect(&ctx, &uri, opts).await.unwrap() }
    });
    let mut first = Peer::accept(&l).await;
    let c = connecting.await.unwrap();
    let h = c.handle();
    let publish = tokio::spawn(async move { h.publish("t", "x", QoS::ExactlyOnce).await });
    let (byte1, packet) = first.packet().await;
    assert_eq!(byte1 >> 4, 3, "PUBLISH");
    let Ok(Some((Packet::Publish(p), _))) = decode(&packet, Version::V5, usize::MAX) else {
        panic!("expected PUBLISH");
    };
    first.ack(PacketType::PubRec, p.pkid).await;
    assert_eq!(first.packet().await.0 >> 4, 6, "PUBREL");
    drop(first);
    // PUBREC said the broker had it; the new connection has no flow to
    // finish.
    let mut second = Peer::accept(&l).await;
    let done = timeout(WAIT, publish).await.unwrap().unwrap();
    assert!(done.is_ok(), "{done:?}");
    let mut chunk = [0u8; 64];
    let got = timeout(Duration::from_millis(300), second.s.read(&mut chunk)).await;
    assert!(got.is_err(), "resent on the new connection: {got:?}");
}

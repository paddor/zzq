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

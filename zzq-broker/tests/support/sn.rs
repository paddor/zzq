//! A raw MQTT-SN client over UDP.

use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::time::timeout;
use zzq_proto::sn::{self, Filter, Flags, Packet, SnQoS};

use super::WAIT;

/// A UDP socket connected to a gateway, sending and reading datagrams.
#[derive(Debug)]
pub(crate) struct SnClient(UdpSocket);

impl SnClient {
    /// A socket for the gateway at `addr` (`host:port`) that sent nothing
    /// yet.
    pub(crate) async fn open(addr: &str) -> SnClient {
        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        s.connect(addr.trim_start_matches("udp://")).await.unwrap();
        SnClient(s)
    }

    /// Connects as `id` with a clean session and keep alive `duration`,
    /// and checks the CONNACK.
    pub(crate) async fn connect(addr: &str, id: &str, duration: u16) -> SnClient {
        let c = SnClient::open(addr).await;
        let flags = Flags {
            clean_session: true,
            ..Flags::default()
        };
        c.send(&Packet::Connect {
            flags,
            duration,
            client_id: id,
        })
        .await;
        let ack = c.recv().await;
        assert_eq!(
            sn::decode(&ack).unwrap().0,
            Packet::ConnAck { code: sn::ACCEPTED }
        );
        c
    }

    pub(crate) async fn send(&self, p: &Packet<'_>) {
        let mut out = Vec::new();
        sn::encode(p, &mut out);
        self.0.send(&out).await.unwrap();
    }

    /// Sends a datagram as it is.
    pub(crate) async fn send_raw(&self, datagram: &[u8]) {
        self.0.send(datagram).await.unwrap();
    }

    /// The next datagram. Fails after [`WAIT`].
    pub(crate) async fn recv(&self) -> Vec<u8> {
        self.recv_within(WAIT).await.expect("no datagram")
    }

    /// The next datagram, or `None` if none comes within `wait`.
    pub(crate) async fn recv_within(&self, wait: Duration) -> Option<Vec<u8>> {
        let mut buf = vec![0; 2048];
        let n = timeout(wait, self.0.recv(&mut buf)).await.ok()?.unwrap();
        buf.truncate(n);
        Some(buf)
    }

    /// Fails if a datagram comes within 300 ms.
    pub(crate) async fn quiet(&self) {
        let got = self.recv_within(Duration::from_millis(300)).await;
        assert!(got.is_none(), "unexpected datagram {got:?}");
    }

    /// Subscribes to `topic` at QoS 1 and returns its topic ID.
    pub(crate) async fn subscribe(&self, msg_id: u16, topic: &str) -> u16 {
        self.send(&Packet::Subscribe {
            flags: Flags {
                qos: SnQoS::AtLeastOnce,
                ..Flags::default()
            },
            msg_id,
            topic: Filter::Name(topic),
        })
        .await;
        let ack = self.recv().await;
        match sn::decode(&ack).unwrap().0 {
            Packet::SubAck {
                topic_id,
                msg_id: id,
                code: sn::ACCEPTED,
                ..
            } if id == msg_id => topic_id,
            p => panic!("expected SUBACK, got {p:?}"),
        }
    }

    /// Registers `topic` and returns the topic ID and the return code.
    pub(crate) async fn register(&self, msg_id: u16, topic: &str) -> (u16, u8) {
        self.send(&Packet::Register {
            topic_id: 0,
            msg_id,
            topic,
        })
        .await;
        let ack = self.recv().await;
        match sn::decode(&ack).unwrap().0 {
            Packet::RegAck { topic_id, code, .. } => (topic_id, code),
            p => panic!("expected REGACK, got {p:?}"),
        }
    }
}

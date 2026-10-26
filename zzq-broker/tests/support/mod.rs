//! Brokers, clients and raw MQTT connections shared by the test files.

#![allow(
    dead_code,
    clippy::panic,
    clippy::too_many_arguments,
    reason = "each test file uses a part; a raw connection fails loudly; raw packets take every field"
)]

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use zzq_broker::{Broker, BrokerOptions, Config, Context, Stats};
use zzq_client::{Client, ClientOptions, Message};
use zzq_proto::encode::{self, AckFields, ConnectFields, PublishHeader};
use zzq_proto::{
    Packet, PacketType, Properties, Property, QoS, SubscriptionOptions, Version, decode, property,
};

pub(crate) const WAIT: Duration = Duration::from_secs(5);

pub(crate) async fn setup(lanes: usize, opts: BrokerOptions) -> (Context, Broker, String) {
    let ctx = Context::with_config(&Config {
        io_threads: lanes,
        ..Config::default()
    })
    .unwrap();
    let broker = Broker::new(&ctx, opts).await.unwrap();
    let uri = broker.bind("tcp://127.0.0.1:0").await.unwrap().to_string();
    (ctx, broker, uri)
}

pub(crate) async fn client(ctx: &Context, uri: &str, id: &str, version: Version) -> Client {
    let opts = ClientOptions {
        version,
        ..ClientOptions::new(id)
    };
    Client::connect(ctx, uri, opts).await.unwrap()
}

pub(crate) async fn next(c: &mut Client) -> Message {
    timeout(WAIT, c.recv())
        .await
        .expect("timed out")
        .expect("closed")
}

pub(crate) async fn nothing(c: &mut Client) {
    assert!(
        timeout(Duration::from_millis(300), c.recv()).await.is_err(),
        "unexpected message"
    );
}

/// Polls the broker's stats until `done` holds, and returns them. Fails
/// with the last stats after [`WAIT`].
pub(crate) async fn stats_until(broker: &Broker, done: impl Fn(&Stats) -> bool) -> Stats {
    let started = Instant::now();
    loop {
        let stats = broker.stats().await.unwrap();
        if done(&stats) {
            return stats;
        }
        assert!(started.elapsed() < WAIT, "stats never got there: {stats:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Raw v5 property bytes, without the length prefix.
pub(crate) fn props(p: &[Property<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    property::encode_all(p, &mut out);
    out
}

/// A PUBLISH as raw bytes.
pub(crate) fn publish_bytes(
    version: Version,
    topic: &str,
    payload: &[u8],
    qos: QoS,
    pkid: u16,
    properties: &[u8],
) -> Vec<u8> {
    let h = PublishHeader {
        version,
        dup: false,
        qos,
        retain: false,
        topic,
        pkid,
        properties,
    };
    let mut out = Vec::new();
    encode::publish(&h, payload, &mut out);
    out
}

/// A CONNECT as raw bytes, with clean start.
pub(crate) fn connect_bytes(version: Version, id: &str, properties: &[Property<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    encode::connect(
        &ConnectFields {
            version,
            clean_start: true,
            keep_alive: 0,
            properties,
            client_id: id,
            will: None,
            username: None,
            password: None,
        },
        &mut out,
    );
    out
}

/// A received PUBLISH.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Pub {
    pub(crate) dup: bool,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
    pub(crate) topic: String,
    pub(crate) pkid: u16,
    /// Raw v5 property bytes.
    pub(crate) props: Vec<u8>,
    pub(crate) payload: Vec<u8>,
}

impl Pub {
    pub(crate) fn properties(&self) -> Properties<'_> {
        Properties::try_from(&self.props[..]).unwrap()
    }
}

/// A received packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Got {
    ConnAck {
        session_present: bool,
        code: u8,
        props: Vec<u8>,
    },
    Publish(Pub),
    /// PUBACK, PUBREC, PUBREL or PUBCOMP.
    Ack {
        kind: PacketType,
        pkid: u16,
        reason: u8,
    },
    SubAck {
        pkid: u16,
        codes: Vec<u8>,
    },
    UnsubAck {
        pkid: u16,
        codes: Vec<u8>,
    },
    PingResp,
    Disconnect {
        reason: u8,
    },
    /// Anything a broker should not send.
    Other(u8),
}

impl Got {
    fn from_packet(p: &Packet<'_>, byte1: u8) -> Got {
        let ack = |kind, a: &zzq_proto::packet::Ack<'_>| Got::Ack {
            kind,
            pkid: a.pkid,
            reason: a.reason,
        };
        match p {
            Packet::ConnAck(c) => Got::ConnAck {
                session_present: c.session_present,
                code: c.code,
                props: c.properties.raw().to_vec(),
            },
            Packet::Publish(p) => Got::Publish(Pub {
                dup: p.dup,
                qos: p.qos,
                retain: p.retain,
                topic: p.topic.to_owned(),
                pkid: p.pkid,
                props: p.properties.raw().to_vec(),
                payload: p.payload.to_vec(),
            }),
            Packet::PubAck(a) => ack(PacketType::PubAck, a),
            Packet::PubRec(a) => ack(PacketType::PubRec, a),
            Packet::PubRel(a) => ack(PacketType::PubRel, a),
            Packet::PubComp(a) => ack(PacketType::PubComp, a),
            Packet::SubAck(s) => Got::SubAck {
                pkid: s.pkid,
                codes: s.codes.to_vec(),
            },
            Packet::UnsubAck(s) => Got::UnsubAck {
                pkid: s.pkid,
                codes: s.codes.to_vec(),
            },
            Packet::PingResp => Got::PingResp,
            Packet::Disconnect(d) => Got::Disconnect { reason: d.reason },
            _ => Got::Other(byte1),
        }
    }
}

/// A raw MQTT connection, for packets the client never sends.
pub(crate) struct Raw {
    s: TcpStream,
    buf: Vec<u8>,
    /// Packets that came before the one a call waited for.
    early: VecDeque<Got>,
    pub(crate) version: Version,
}

impl Raw {
    /// A TCP connection that has sent nothing yet. Packets decode as v5
    /// until [`Raw::connect_with`] or [`Raw::version`] says otherwise.
    pub(crate) async fn open(uri: &str) -> Raw {
        let s = TcpStream::connect(uri.trim_start_matches("tcp://"))
            .await
            .unwrap();
        s.set_nodelay(true).unwrap();
        Raw {
            s,
            buf: Vec::new(),
            early: VecDeque::new(),
            version: Version::V5,
        }
    }

    /// Connects with clean start and no properties.
    pub(crate) async fn connect(uri: &str, id: &str, version: Version, keep_alive: u16) -> Raw {
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
        let (raw, code) = Raw::connect_with(uri, &c).await;
        assert_eq!(code, 0, "CONNACK code");
        raw
    }

    /// Sends `c` and returns the connection with the CONNACK code, which
    /// must come first.
    pub(crate) async fn connect_with(uri: &str, c: &ConnectFields<'_>) -> (Raw, u8) {
        let (raw, got) = Raw::connack_with(uri, c).await;
        let Got::ConnAck { code, .. } = got else {
            panic!("expected CONNACK, got {got:?}");
        };
        (raw, code)
    }

    /// Sends `c` and returns the connection with the whole CONNACK.
    pub(crate) async fn connack_with(uri: &str, c: &ConnectFields<'_>) -> (Raw, Got) {
        let mut raw = Raw::open(uri).await;
        raw.version = c.version;
        let mut out = Vec::new();
        encode::connect(c, &mut out);
        raw.send(&out).await;
        let got = raw.next().await;
        (raw, got)
    }

    pub(crate) async fn send(&mut self, bytes: &[u8]) {
        self.s.write_all(bytes).await.unwrap();
    }

    /// Drops the connection with a TCP reset instead of a close.
    pub(crate) fn reset(self) {
        #[allow(deprecated, reason = "a zero linger never blocks")]
        let _ = self.s.set_linger(Some(Duration::ZERO));
    }

    /// Sends, ignoring a connection the broker already closed.
    pub(crate) async fn send_lossy(&mut self, bytes: &[u8]) {
        let _ = self.s.write_all(bytes).await;
    }

    /// The next packet, or `None` once the broker closed the connection.
    /// Fails after [`WAIT`].
    pub(crate) async fn try_next(&mut self) -> Option<Got> {
        self.try_next_within(WAIT)
            .await
            .unwrap_or_else(|| panic!("no packet or close within {WAIT:?}"))
    }

    /// Like [`Raw::try_next`], but `None` if nothing happens within `wait`.
    pub(crate) async fn try_next_within(&mut self, wait: Duration) -> Option<Option<Got>> {
        if let Some(got) = self.early.pop_front() {
            return Some(Some(got));
        }
        self.read_within(wait).await
    }

    /// The next packet from the socket, skipping the early ones.
    async fn read_within(&mut self, wait: Duration) -> Option<Option<Got>> {
        let deadline = Instant::now() + wait;
        loop {
            match decode(&self.buf, self.version, usize::MAX) {
                Ok(Some((p, n))) => {
                    let got = Got::from_packet(&p, self.buf[0]);
                    self.buf.drain(..n);
                    return Some(Some(got));
                }
                Ok(None) => {}
                Err(e) => panic!("broker sent a bad packet: {e:?} {:?}", self.buf),
            }
            self.buf.reserve(65_536);
            let left = deadline.saturating_duration_since(Instant::now());
            match timeout(left, self.s.read_buf(&mut self.buf)).await {
                Err(_) => return None,
                Ok(Ok(0) | Err(_)) => return Some(None),
                Ok(Ok(_)) => {}
            }
        }
    }

    /// The next packet. Fails on a close.
    pub(crate) async fn next(&mut self) -> Got {
        self.try_next().await.expect("connection closed")
    }

    /// The next packet, which must be a PUBLISH.
    pub(crate) async fn publish(&mut self) -> Pub {
        match self.next().await {
            Got::Publish(p) => p,
            got => panic!("expected PUBLISH, got {got:?}"),
        }
    }

    /// Fails if a packet or a close arrives within `ms` milliseconds.
    pub(crate) async fn quiet(&mut self, ms: u64) {
        if let Some(got) = self.try_next_within(Duration::from_millis(ms)).await {
            panic!("expected silence, got {got:?}");
        }
    }

    /// Fails unless the broker closes the connection without sending more.
    pub(crate) async fn closed(&mut self) {
        if let Some(got) = self.try_next().await {
            panic!("expected a close, got {got:?}");
        }
    }

    /// Fails unless the broker refuses: a v5 DISCONNECT with one of
    /// `reasons` (a 3.1.1 connection just closes), then the close.
    pub(crate) async fn disconnected(&mut self, reasons: &[u8]) {
        if self.version.is_v5() {
            match self.try_next().await {
                Some(Got::Disconnect { reason }) => {
                    assert!(
                        reasons.contains(&reason),
                        "DISCONNECT reason {reason:#04x}, want one of {reasons:#04x?}"
                    );
                }
                got => panic!("expected DISCONNECT {reasons:#04x?}, got {got:?}"),
            }
        }
        self.closed().await;
    }

    /// Fails unless the broker refuses a CONNECT: a CONNACK with one of
    /// `codes`, or no CONNACK if `codes` is empty, then the close.
    pub(crate) async fn refused(&mut self, codes: &[u8]) {
        if !codes.is_empty() {
            match self.try_next().await {
                Some(Got::ConnAck { code, .. }) => assert!(
                    codes.contains(&code),
                    "CONNACK code {code:#04x}, want one of {codes:#04x?}"
                ),
                got => panic!("expected CONNACK {codes:#04x?}, got {got:?}"),
            }
        }
        self.closed().await;
    }

    pub(crate) async fn subscribe(&mut self, filter: &str, opts: SubscriptionOptions) -> Vec<u8> {
        self.subscribe_with(1, &[], &[(filter, opts)]).await
    }

    /// Subscribes and returns the SUBACK codes.
    pub(crate) async fn subscribe_with(
        &mut self,
        pkid: u16,
        properties: &[Property<'_>],
        filters: &[(&str, SubscriptionOptions)],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        encode::subscribe(self.version, pkid, properties, filters, &mut out);
        self.send(&out).await;
        // Retained messages may come first (3.8.4).
        match self
            .wait_for(|g| matches!(g, Got::SubAck { pkid: id, .. } if *id == pkid))
            .await
        {
            Got::SubAck { codes, .. } => codes,
            got => unreachable!("{got:?}"),
        }
    }

    /// Reads until a packet matches `want`, keeping PUBLISH packets before
    /// it for later calls. Fails on anything else.
    pub(crate) async fn wait_for(&mut self, want: impl Fn(&Got) -> bool) -> Got {
        if let Some(i) = self.early.iter().position(&want) {
            return self.early.remove(i).unwrap();
        }
        loop {
            match self.read_within(WAIT).await {
                Some(Some(got)) if want(&got) => return got,
                Some(Some(got @ Got::Publish(_))) => self.early.push_back(got),
                got => panic!("waited for a packet, got {got:?}"),
            }
        }
    }

    /// Acks a delivery, completing the QoS 2 exchange.
    pub(crate) async fn complete(&mut self, p: &Pub) {
        match p.qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => self.ack(PacketType::PubAck, p.pkid).await,
            QoS::ExactlyOnce => {
                self.ack(PacketType::PubRec, p.pkid).await;
                let pkid = p.pkid;
                self.wait_for(|g| matches!(g, Got::Ack { kind: PacketType::PubRel, pkid: id, .. } if *id == pkid))
                    .await;
                self.ack(PacketType::PubComp, p.pkid).await;
            }
        }
    }

    /// Sends a PUBLISH.
    pub(crate) async fn pub_(&mut self, topic: &str, payload: &[u8], qos: QoS, pkid: u16) {
        self.pub_props(topic, payload, qos, pkid, &[]).await;
    }

    /// Sends a PUBLISH with raw v5 property bytes.
    pub(crate) async fn pub_props(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        pkid: u16,
        properties: &[u8],
    ) {
        let out = publish_bytes(self.version, topic, payload, qos, pkid, properties);
        self.send(&out).await;
    }

    /// Sends PUBACK, PUBREC, PUBREL or PUBCOMP.
    pub(crate) async fn ack(&mut self, kind: PacketType, pkid: u16) {
        let mut out = Vec::new();
        encode::ack(&AckFields::new(kind, pkid), self.version, &mut out);
        self.send(&out).await;
    }

    /// Sends DISCONNECT with a v5 reason and properties.
    pub(crate) async fn disconnect(&mut self, reason: u8, properties: &[Property<'_>]) {
        let mut out = Vec::new();
        encode::reason_only(
            PacketType::Disconnect,
            self.version,
            reason,
            &props(properties),
            &mut out,
        );
        self.send(&out).await;
    }
}

//! MQTT-SN 1.2 gateway.
//!
//! Each MQTT-SN client gets an in-memory MQTT 3.1.1 connection to the
//! broker, which takes the normal accept path. The gateway translates
//! between the two and keeps the client's topic IDs. Sessions, QoS flows and
//! keep alive belong to the broker.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use zzq_proto::encode::{self, AckFields, ConnectFields, PublishHeader, WillFields};
use zzq_proto::sn::{self, Flags, SnQoS, Topic};
use zzq_proto::{Packet, PacketType, QoS, SubscriptionOptions, Version, decode};
use zzq_tokio::transport::Stream;
use zzq_tokio::transport::udp::MAX_DATAGRAM;

use crate::broker::Shared;
use crate::lane::Control;
use crate::msg::Msg;
use crate::options::{AuthDecision, ClientInfo, ConnectInfo};

/// Datagrams queued per client before the gateway drops new ones.
const CLIENT_QUEUE: usize = 1024;
/// Buffer of the in-memory MQTT connection, each way.
const PIPE_BYTES: usize = 256 << 10;
/// How often retransmissions and sleep deadlines are checked.
const TICK: Duration = Duration::from_secs(1);
/// Time a client has to answer WILLTOPICREQ and WILLMSGREQ.
const WILL_TIMEOUT: Duration = Duration::from_secs(10);

/// Datagram queue of each client, with an ID that tells reconnects apart.
type Clients = Rc<RefCell<FxHashMap<SocketAddr, (u64, mpsc::Sender<Vec<u8>>)>>>;

/// Serves MQTT-SN on `socket` until the broker shuts down.
pub(crate) async fn gateway(shared: Arc<Shared>, socket: UdpSocket) {
    let socket = Rc::new(socket);
    let clients: Clients = Rc::default();
    let mut buf = vec![0u8; MAX_DATAGRAM];
    let mut next_id = 0u64;
    loop {
        let Ok((n, addr)) = socket.recv_from(&mut buf).await else {
            continue;
        };
        let datagram = &buf[..n];
        let Ok((packet, _)) = sn::decode(datagram) else {
            continue;
        };
        match packet {
            sn::Packet::Connect { .. } => {
                let (tx, rx) = mpsc::channel(CLIENT_QUEUE);
                next_id += 1;
                clients.borrow_mut().insert(addr, (next_id, tx));
                let task = Client::new(&shared, socket.clone(), addr);
                tokio::task::spawn_local(task.run(next_id, rx, datagram.to_vec(), clients.clone()));
            }
            sn::Packet::SearchGw { .. } => {
                let reply = sn::Packet::GwInfo {
                    gw_id: shared.opts.sn_gateway_id,
                    address: &[],
                };
                send(&socket, addr, &reply).await;
            }
            sn::Packet::Publish {
                flags, topic, data, ..
            } if flags.qos == SnQoS::NoConnection => {
                publish_without_connection(&shared, topic, flags.retain, data);
            }
            _ => {
                let tx = clients.borrow().get(&addr).map(|(_, tx)| tx.clone());
                if let Some(tx) = tx {
                    let _ = tx.try_send(datagram.to_vec());
                }
            }
        }
    }
}

async fn send(socket: &UdpSocket, addr: SocketAddr, p: &sn::Packet<'_>) {
    let mut out = Vec::with_capacity(sn::encoded_len(p));
    sn::encode(p, &mut out);
    let _ = socket.send_to(&out, addr).await;
}

fn predefined(shared: &Shared, id: u16) -> Option<&str> {
    shared
        .opts
        .sn_predefined
        .iter()
        .find(|(i, _)| *i == id)
        .map(|(_, t)| t.as_str())
}

/// QoS -1: a publish from a client without a connection. The hooks see a
/// client with an empty ID and no credentials.
fn publish_without_connection(shared: &Shared, topic: Topic<'_>, retain: bool, data: &[u8]) {
    let name = match topic {
        Topic::Predefined(id) => predefined(shared, id).map(str::to_owned),
        Topic::Short(s) => std::str::from_utf8(&s).ok().map(str::to_owned),
        Topic::Id(_) | Topic::Name(_) => None,
    };
    let Some(name) = name.filter(|t| zzq_proto::topic::validate_name(t).is_ok()) else {
        return;
    };
    let o = &shared.opts;
    if name.starts_with("$SYS/") || crate::msg::too_deep(&name, o.max_topic_levels) {
        return;
    }
    let info = ConnectInfo {
        client_id: "",
        username: None,
        password: None,
        version: Version::V311,
        peer_cert: None,
    };
    if let Some(auth) = &o.authenticator
        && auth.authenticate(&info) != AuthDecision::Allow
    {
        return;
    }
    let client = ClientInfo {
        client_id: "",
        username: None,
    };
    if let Some(acl) = &o.authorizer
        && !acl.may_publish(&client, &name)
    {
        return;
    }
    let msg = Msg::new(&name, data, &[], QoS::AtMostOnce, retain);
    let lane = shared.ctx.next_lane().index();
    let _ = shared.lanes[lane].control.send(Control::Publish(msg));
}

/// One MQTT-SN client and its MQTT connection.
struct Client {
    shared: Arc<Shared>,
    socket: Rc<UdpSocket>,
    addr: SocketAddr,
    /// Topic ID to name, both registered and assigned by the gateway.
    topics: FxHashMap<u16, String>,
    ids: FxHashMap<String, u16>,
    next_topic_id: u16,
    /// Topic IDs sent in REGISTER and not yet acked, by REGISTER message ID.
    registering: FxHashMap<u16, u16>,
    /// Datagrams waiting for a REGACK, by topic ID.
    held: FxHashMap<u16, Vec<Vec<u8>>>,
    next_msg_id: u16,
    /// Topic ID of each inbound QoS 1 publish, for the PUBACK.
    puback_topic: FxHashMap<u16, u16>,
    /// Filter of each SUBSCRIBE, for the topic ID in SUBACK.
    sub_filter: FxHashMap<u16, String>,
    /// Sleep deadline and duration while the client sleeps.
    asleep: Option<(Instant, Duration)>,
    /// Datagrams held while the client sleeps.
    buffered: VecDeque<Vec<u8>>,
    /// Datagrams waiting for an ack, by message type and ID.
    unacked: FxHashMap<(u8, u16), Retry>,
    /// MQTT packets the gateway itself sends to the broker.
    to_broker: Vec<u8>,
    /// Packet IDs of QoS 2 messages dropped for lack of a topic ID, waiting
    /// for PUBREL.
    dropped: FxHashSet<u16>,
}

impl Client {
    fn new(shared: &Arc<Shared>, socket: Rc<UdpSocket>, addr: SocketAddr) -> Client {
        Client {
            shared: shared.clone(),
            socket,
            addr,
            to_broker: Vec::new(),
            dropped: FxHashSet::default(),
            topics: FxHashMap::default(),
            ids: FxHashMap::default(),
            next_topic_id: 0,
            registering: FxHashMap::default(),
            held: FxHashMap::default(),
            next_msg_id: 0,
            puback_topic: FxHashMap::default(),
            sub_filter: FxHashMap::default(),
            asleep: None,
            buffered: VecDeque::new(),
            unacked: FxHashMap::default(),
        }
    }

    async fn run(
        mut self,
        id: u64,
        mut rx: mpsc::Receiver<Vec<u8>>,
        connect: Vec<u8>,
        clients: Clients,
    ) {
        if let Some(mut pipe) = self.connect(&mut rx, &connect).await {
            self.serve(&mut rx, &mut pipe).await;
        }
        let mut map = clients.borrow_mut();
        if map.get(&self.addr).is_some_and(|(i, _)| *i == id) {
            map.remove(&self.addr);
        }
    }

    async fn send(&self, p: &sn::Packet<'_>) {
        send(&self.socket, self.addr, p).await;
    }

    /// Runs the will exchange, then opens the MQTT connection with CONNECT.
    async fn connect(
        &mut self,
        rx: &mut mpsc::Receiver<Vec<u8>>,
        datagram: &[u8],
    ) -> Option<DuplexStream> {
        let Ok((
            sn::Packet::Connect {
                flags,
                duration,
                client_id,
            },
            _,
        )) = sn::decode(datagram)
        else {
            return None;
        };
        let will = if flags.will {
            Some(self.will_exchange(rx).await?)
        } else {
            None
        };
        let fields = ConnectFields {
            version: Version::V311,
            clean_start: flags.clean_session,
            keep_alive: duration,
            properties: &[],
            client_id,
            will: will.as_ref().map(|(wf, topic, payload)| WillFields {
                qos: wf.qos.mqtt(),
                retain: wf.retain,
                properties: &[],
                topic,
                payload,
            }),
            username: None,
            password: None,
        };
        let mut out = Vec::new();
        encode::connect(&fields, &mut out);
        let (mut gateway_end, broker_end) = tokio::io::duplex(PIPE_BYTES);
        gateway_end.write_all(&out).await.ok()?;
        tokio::task::spawn_local(crate::broker::admit(
            self.shared.clone(),
            Stream::Inproc(broker_end),
        ));
        Some(gateway_end)
    }

    /// WILLTOPICREQ, WILLTOPIC, WILLMSGREQ, WILLMSG.
    async fn will_exchange(
        &self,
        rx: &mut mpsc::Receiver<Vec<u8>>,
    ) -> Option<(Flags, String, Vec<u8>)> {
        self.send(&sn::Packet::WillTopicReq).await;
        let topic = tokio::time::timeout(WILL_TIMEOUT, rx.recv())
            .await
            .ok()
            .flatten()?;
        let Ok((
            sn::Packet::WillTopic {
                flags: Some(flags),
                topic,
            },
            _,
        )) = sn::decode(&topic)
        else {
            return None;
        };
        let topic = topic.to_owned();
        self.send(&sn::Packet::WillMsgReq).await;
        let msg = tokio::time::timeout(WILL_TIMEOUT, rx.recv())
            .await
            .ok()
            .flatten()?;
        let Ok((sn::Packet::WillMsg { message }, _)) = sn::decode(&msg) else {
            return None;
        };
        Some((flags, topic, message.to_vec()))
    }

    async fn serve(&mut self, rx: &mut mpsc::Receiver<Vec<u8>>, pipe: &mut DuplexStream) {
        let mut buf = BytesMut::with_capacity(64 << 10);
        let mut out = Vec::new();
        let mut tick = tokio::time::interval(TICK.min(self.shared.opts.sn_retry));
        loop {
            tokio::select! {
                datagram = rx.recv() => {
                    let Some(datagram) = datagram else { return };
                    let Ok((packet, _)) = sn::decode(&datagram) else { continue };
                    self.acked(&packet);
                    if self.asleep.is_some() && !self.stays_asleep(&packet).await {
                        self.wake().await;
                    }
                    match self.inbound(&packet, &mut out).await {
                        Inbound::Continue => {}
                        Inbound::Sleep(seconds) => {
                            self.send(&sn::Packet::Disconnect { duration: None }).await;
                            self.sleep(seconds);
                        }
                        Inbound::Disconnect => {
                            let _ = pipe.write_all(&out).await;
                            self.send(&sn::Packet::Disconnect { duration: None }).await;
                            return;
                        }
                    }
                    if !out.is_empty() {
                        if pipe.write_all(&out).await.is_err() {
                            return;
                        }
                        out.clear();
                    }
                }
                n = pipe.read_buf(&mut buf) => {
                    if !matches!(n, Ok(n) if n > 0) {
                        self.send(&sn::Packet::Disconnect { duration: None }).await;
                        return;
                    }
                    if !self.relay(&mut buf, pipe).await {
                        return;
                    }
                }
                _ = tick.tick() => {
                    if !self.on_tick(pipe).await {
                        return;
                    }
                }
            }
        }
    }

    /// Translates the MQTT packets in `buf` for the client, then writes the
    /// gateway's own packets to the broker. Returns false once the pipe is
    /// gone.
    async fn relay(&mut self, buf: &mut BytesMut, pipe: &mut DuplexStream) -> bool {
        while let Ok(Some((packet, n))) = decode(buf, Version::V311, usize::MAX) {
            let datagrams = if self.asleep.is_some() && matches!(packet, Packet::PingResp) {
                Vec::new()
            } else {
                self.outbound(&packet)
            };
            buf.advance(n);
            for d in datagrams {
                self.deliver(d).await;
            }
        }
        if self.to_broker.is_empty() {
            return true;
        }
        let packets = std::mem::take(&mut self.to_broker);
        pipe.write_all(&packets).await.is_ok()
    }

    /// Sends a datagram, or buffers it while the client sleeps. QoS 1/2
    /// publishes, PUBREL and REGISTER are kept for retransmission.
    async fn deliver(&mut self, datagram: Vec<u8>) {
        if let Some(key) = retry_key(&datagram) {
            let next = Instant::now() + self.shared.opts.sn_retry;
            self.unacked.insert(
                key,
                Retry {
                    datagram: datagram.clone(),
                    next,
                    tries: 0,
                },
            );
        }
        if self.asleep.is_some() {
            if self.buffered.len() >= self.shared.opts.sn_sleep_buffer {
                self.buffered.pop_front();
            }
            self.buffered.push_back(datagram);
        } else {
            let _ = self.socket.send_to(&datagram, self.addr).await;
        }
    }

    /// Forgets retransmissions the client acknowledged.
    fn acked(&mut self, packet: &sn::Packet<'_>) {
        let key = match *packet {
            sn::Packet::PubAck { msg_id, .. } | sn::Packet::PubRec { msg_id } => {
                (KIND_PUBLISH, msg_id)
            }
            sn::Packet::RegAck { msg_id, .. } => (KIND_REGISTER, msg_id),
            sn::Packet::PubComp { msg_id } => (KIND_PUBREL, msg_id),
            _ => return,
        };
        self.unacked.remove(&key);
    }

    fn sleep(&mut self, seconds: u16) {
        let duration = Duration::from_secs(u64::from(seconds.max(1)));
        self.asleep = Some((Instant::now() + duration.mul_f32(1.5), duration));
    }

    /// Handles a datagram from a sleeping client. Returns false if the
    /// client woke up for good.
    async fn stays_asleep(&mut self, packet: &sn::Packet<'_>) -> bool {
        let sn::Packet::PingReq { client_id: Some(_) } = packet else {
            return false;
        };
        for d in std::mem::take(&mut self.buffered) {
            let _ = self.socket.send_to(&d, self.addr).await;
        }
        self.send(&sn::Packet::PingResp).await;
        if let Some((deadline, duration)) = &mut self.asleep {
            *deadline = Instant::now() + duration.mul_f32(1.5);
        }
        true
    }

    async fn wake(&mut self) {
        self.asleep = None;
        for d in std::mem::take(&mut self.buffered) {
            let _ = self.socket.send_to(&d, self.addr).await;
        }
    }

    /// Retransmissions, and keep alive on behalf of a sleeping client.
    /// Returns false when the client is gone.
    async fn on_tick(&mut self, pipe: &mut DuplexStream) -> bool {
        let now = Instant::now();
        if let Some((deadline, _)) = self.asleep {
            if now > deadline {
                return false;
            }
            let mut ping = Vec::new();
            encode::pingreq(&mut ping);
            return pipe.write_all(&ping).await.is_ok();
        }
        let retry = self.shared.opts.sn_retry;
        let max = self.shared.opts.sn_retries;
        let mut give_up = false;
        for r in self.unacked.values_mut().filter(|r| r.next <= now) {
            if r.tries >= max {
                give_up = true;
                continue;
            }
            r.tries += 1;
            r.next = now + retry;
            set_dup(&mut r.datagram);
            let _ = self.socket.send_to(&r.datagram, self.addr).await;
        }
        !give_up
    }

    fn topic_name(&self, topic: Topic<'_>) -> Option<String> {
        match topic {
            Topic::Id(id) => self.topics.get(&id).cloned(),
            Topic::Predefined(id) => predefined(&self.shared, id).map(str::to_owned),
            Topic::Short(s) => std::str::from_utf8(&s).ok().map(str::to_owned),
            Topic::Name(n) => Some(n.to_owned()),
        }
    }

    /// The topic ID of `name`, assigning one if needed. `None` once the
    /// client has `sn_max_topics` of them, or no ID is free.
    fn topic_id_for(&mut self, name: &str) -> Option<u16> {
        if let Some(&id) = self.ids.get(name) {
            return Some(id);
        }
        if self.topics.len() >= self.shared.opts.sn_max_topics {
            return None;
        }
        for _ in 0..u16::MAX {
            self.next_topic_id = self.next_topic_id.wrapping_add(1).max(1);
            let id = self.next_topic_id;
            if !self.topics.contains_key(&id) && predefined(&self.shared, id).is_none() {
                self.topics.insert(id, name.to_owned());
                self.ids.insert(name.to_owned(), id);
                return Some(id);
            }
        }
        None
    }

    fn msg_id(&mut self) -> u16 {
        self.next_msg_id = self.next_msg_id.wrapping_add(1).max(1);
        self.next_msg_id
    }

    /// Translates a datagram from the client into MQTT bytes in `out`.
    async fn inbound(&mut self, packet: &sn::Packet<'_>, out: &mut Vec<u8>) -> Inbound {
        let v = Version::V311;
        match *packet {
            sn::Packet::Register { msg_id, topic, .. } => self.register(msg_id, topic).await,
            sn::Packet::RegAck { msg_id, .. } => {
                if let Some(topic_id) = self.registering.remove(&msg_id) {
                    for d in self.held.remove(&topic_id).unwrap_or_default() {
                        self.deliver(d).await;
                    }
                }
            }
            sn::Packet::Publish { .. } => self.publish_in(packet, out).await,
            sn::Packet::PubAck { msg_id, .. } => ack(out, PacketType::PubAck, msg_id),
            sn::Packet::PubRec { msg_id } => ack(out, PacketType::PubRec, msg_id),
            sn::Packet::PubRel { msg_id } => ack(out, PacketType::PubRel, msg_id),
            sn::Packet::PubComp { msg_id } => ack(out, PacketType::PubComp, msg_id),
            sn::Packet::Subscribe {
                flags,
                msg_id,
                topic,
            } => self.subscribe_in(flags, msg_id, topic, out).await,
            sn::Packet::Unsubscribe { msg_id, topic, .. } => {
                if let Some(filter) = self.topic_name(topic) {
                    encode::unsubscribe(v, msg_id.max(1), &[], &[&filter], out);
                }
            }
            sn::Packet::PingReq { .. } => encode::pingreq(out),
            sn::Packet::Disconnect {
                duration: Some(seconds),
            } => return Inbound::Sleep(seconds),
            sn::Packet::Disconnect { duration: None } => {
                encode::reason_only(PacketType::Disconnect, v, 0, &[], out);
                return Inbound::Disconnect;
            }
            sn::Packet::WillTopicUpd { .. } => {
                self.send(&sn::Packet::WillTopicResp {
                    code: sn::REJECTED_NOT_SUPPORTED,
                })
                .await;
            }
            sn::Packet::WillMsgUpd { .. } => {
                self.send(&sn::Packet::WillMsgResp {
                    code: sn::REJECTED_NOT_SUPPORTED,
                })
                .await;
            }
            _ => {}
        }
        Inbound::Continue
    }

    /// REGISTER from the client: assigns a topic ID.
    async fn register(&mut self, msg_id: u16, topic: &str) {
        let (topic_id, code) = if zzq_proto::topic::validate_name(topic).is_err() {
            (0, sn::REJECTED_INVALID_TOPIC_ID)
        } else if let Some(id) = self.topic_id_for(topic) {
            (id, sn::ACCEPTED)
        } else {
            (0, sn::REJECTED_CONGESTION)
        };
        self.send(&sn::Packet::RegAck {
            topic_id,
            msg_id,
            code,
        })
        .await;
    }

    /// PUBLISH from the client.
    async fn publish_in(&mut self, packet: &sn::Packet<'_>, out: &mut Vec<u8>) {
        let sn::Packet::Publish {
            flags,
            topic,
            msg_id,
            data,
        } = *packet
        else {
            return;
        };
        let Some(name) = self.topic_name(topic) else {
            let topic_id = match topic {
                Topic::Id(id) | Topic::Predefined(id) => id,
                _ => 0,
            };
            let code = sn::REJECTED_INVALID_TOPIC_ID;
            self.send(&sn::Packet::PubAck {
                topic_id,
                msg_id,
                code,
            })
            .await;
            return;
        };
        let qos = flags.qos.mqtt();
        if qos == QoS::AtLeastOnce
            && let Topic::Id(id) | Topic::Predefined(id) = topic
        {
            self.puback_topic.insert(msg_id, id);
        }
        let h = PublishHeader {
            version: Version::V311,
            dup: flags.dup && qos != QoS::AtMostOnce,
            qos,
            retain: flags.retain,
            topic: &name,
            pkid: if qos == QoS::AtMostOnce {
                0
            } else {
                msg_id.max(1)
            },
            properties: &[],
        };
        encode::publish(&h, data, out);
    }

    /// SUBSCRIBE from the client.
    async fn subscribe_in(
        &mut self,
        flags: Flags,
        msg_id: u16,
        topic: Topic<'_>,
        out: &mut Vec<u8>,
    ) {
        let filter = self.topic_name(topic);
        // A plain filter gets its topic ID now, so SUBACK has one.
        let code = match &filter {
            None => sn::REJECTED_INVALID_TOPIC_ID,
            Some(f) if plain(f) && self.topic_id_for(f).is_none() => sn::REJECTED_CONGESTION,
            Some(_) => sn::ACCEPTED,
        };
        let Some(filter) = filter.filter(|_| code == sn::ACCEPTED) else {
            self.send(&sn::Packet::SubAck {
                flags,
                topic_id: 0,
                msg_id,
                code,
            })
            .await;
            return;
        };
        let opts = SubscriptionOptions::qos(flags.qos.mqtt());
        encode::subscribe(Version::V311, msg_id.max(1), &[], &[(&filter, opts)], out);
        self.sub_filter.insert(msg_id.max(1), filter);
    }

    /// SUBACK fields for the client: granted QoS, the topic ID of a plain
    /// filter, and the return code.
    fn suback(&mut self, pkid: u16, mqtt_code: u8) -> (Flags, u16, u8) {
        let granted = mqtt_code < 0x80;
        let filter = self.sub_filter.remove(&pkid).unwrap_or_default();
        let topic_id = if granted && plain(&filter) {
            self.ids.get(&filter).copied().unwrap_or(0)
        } else {
            0
        };
        let qos = match mqtt_code {
            1 => SnQoS::AtLeastOnce,
            2 => SnQoS::ExactlyOnce,
            _ => SnQoS::AtMostOnce,
        };
        let code = if granted {
            sn::ACCEPTED
        } else {
            sn::REJECTED_NOT_SUPPORTED
        };
        (
            Flags {
                qos,
                ..Flags::default()
            },
            topic_id,
            code,
        )
    }

    /// Translates one MQTT packet from the broker into datagrams.
    fn outbound(&mut self, packet: &Packet<'_>) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut push = |p: &sn::Packet<'_>| {
            let mut d = Vec::with_capacity(sn::encoded_len(p));
            sn::encode(p, &mut d);
            out.push(d);
        };
        match *packet {
            Packet::ConnAck(c) => {
                let code = if c.code == 0 {
                    sn::ACCEPTED
                } else {
                    sn::REJECTED_NOT_SUPPORTED
                };
                push(&sn::Packet::ConnAck { code });
            }
            Packet::Publish(p) => return self.publish_out(&p),
            Packet::PubAck(a) => {
                let topic_id = self.puback_topic.remove(&a.pkid).unwrap_or(0);
                push(&sn::Packet::PubAck {
                    topic_id,
                    msg_id: a.pkid,
                    code: sn::ACCEPTED,
                });
            }
            Packet::PubRec(a) => push(&sn::Packet::PubRec { msg_id: a.pkid }),
            Packet::PubRel(a) if self.dropped.remove(&a.pkid) => {
                ack(&mut self.to_broker, PacketType::PubComp, a.pkid);
            }
            Packet::PubRel(a) => push(&sn::Packet::PubRel { msg_id: a.pkid }),
            Packet::PubComp(a) => push(&sn::Packet::PubComp { msg_id: a.pkid }),
            Packet::SubAck(s) => {
                let (flags, topic_id, code) =
                    self.suback(s.pkid, s.codes.first().copied().unwrap_or(0x80));
                push(&sn::Packet::SubAck {
                    flags,
                    topic_id,
                    msg_id: s.pkid,
                    code,
                });
            }
            Packet::UnsubAck(s) => push(&sn::Packet::UnsubAck { msg_id: s.pkid }),
            Packet::PingResp => push(&sn::Packet::PingResp),
            _ => {}
        }
        out
    }

    /// A broker PUBLISH for the client. Registers the topic first if the
    /// client does not know it yet.
    fn publish_out(&mut self, p: &zzq_proto::packet::Publish<'_>) -> Vec<Vec<u8>> {
        let qos = match p.qos {
            QoS::AtMostOnce => SnQoS::AtMostOnce,
            QoS::AtLeastOnce => SnQoS::AtLeastOnce,
            QoS::ExactlyOnce => SnQoS::ExactlyOnce,
        };
        let flags = Flags {
            dup: p.dup,
            qos,
            retain: p.retain,
            ..Flags::default()
        };
        let predefined_id = self
            .shared
            .opts
            .sn_predefined
            .iter()
            .find(|(_, t)| t == p.topic)
            .map(|(i, _)| *i);
        let mut out = Vec::new();
        let (topic, pending) = if let Some(id) = predefined_id {
            (Topic::Predefined(id), None)
        } else if p.topic.len() == 2 {
            let b = p.topic.as_bytes();
            (Topic::Short([b[0], b[1]]), None)
        } else if let Some(&id) = self.ids.get(p.topic) {
            let pending = self.registering.values().any(|&t| t == id).then_some(id);
            (Topic::Id(id), pending)
        } else if let Some(id) = self.topic_id_for(p.topic) {
            let msg_id = self.msg_id();
            self.registering.insert(msg_id, id);
            let reg = sn::Packet::Register {
                topic_id: id,
                msg_id,
                topic: p.topic,
            };
            let mut d = Vec::with_capacity(sn::encoded_len(&reg));
            sn::encode(&reg, &mut d);
            out.push(d);
            (Topic::Id(id), Some(id))
        } else {
            self.drop_publish(p);
            return out;
        };
        let publish = sn::Packet::Publish {
            flags,
            topic,
            msg_id: p.pkid,
            data: p.payload,
        };
        let mut d = Vec::with_capacity(sn::encoded_len(&publish));
        sn::encode(&publish, &mut d);
        match pending {
            Some(id) => self.held.entry(id).or_default().push(d),
            None => out.push(d),
        }
        out
    }

    /// Drops a broker PUBLISH the client has no topic ID for, and acks it
    /// so the session's window moves on.
    fn drop_publish(&mut self, p: &zzq_proto::packet::Publish<'_>) {
        match p.qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => ack(&mut self.to_broker, PacketType::PubAck, p.pkid),
            QoS::ExactlyOnce => {
                self.dropped.insert(p.pkid);
                ack(&mut self.to_broker, PacketType::PubRec, p.pkid);
            }
        }
    }
}

/// Whether a SUBSCRIBE filter names one normal topic, which gets a topic
/// ID in SUBACK.
fn plain(filter: &str) -> bool {
    !filter.contains(['+', '#']) && filter.len() != 2 && !filter.is_empty()
}

enum Inbound {
    Continue,
    /// DISCONNECT with a sleep duration in seconds.
    Sleep(u16),
    Disconnect,
}

const KIND_REGISTER: u8 = 0x0a;
const KIND_PUBLISH: u8 = 0x0c;
const KIND_PUBREL: u8 = 0x10;

/// A datagram waiting for an ack from the client.
struct Retry {
    datagram: Vec<u8>,
    next: Instant,
    tries: u8,
}

/// Message type and ID of a datagram the client must ack.
fn retry_key(datagram: &[u8]) -> Option<(u8, u16)> {
    match sn::decode(datagram).ok()?.0 {
        sn::Packet::Publish { flags, msg_id, .. }
            if matches!(flags.qos, SnQoS::AtLeastOnce | SnQoS::ExactlyOnce) =>
        {
            Some((KIND_PUBLISH, msg_id))
        }
        sn::Packet::Register { msg_id, .. } => Some((KIND_REGISTER, msg_id)),
        sn::Packet::PubRel { msg_id } => Some((KIND_PUBREL, msg_id)),
        _ => None,
    }
}

/// Sets DUP on a PUBLISH datagram.
fn set_dup(datagram: &mut [u8]) {
    let at = if datagram.first() == Some(&0x01) {
        3
    } else {
        1
    };
    if datagram.get(at) == Some(&KIND_PUBLISH)
        && let Some(flags) = datagram.get_mut(at + 1)
    {
        *flags |= 0x80;
    }
}

fn ack(out: &mut Vec<u8>, kind: PacketType, msg_id: u16) {
    encode::ack(&AckFields::new(kind, msg_id), Version::V311, out);
}

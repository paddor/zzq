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
use zzq_proto::encode::{self, ConnectFields, WillFields};
use zzq_proto::sn::{self, Flags, SnQoS, Topic};
use zzq_proto::{Packet, QoS, Version, decode};
use zzq_tokio::transport::Stream;
use zzq_tokio::transport::udp::MAX_DATAGRAM;

use crate::broker::Shared;
use crate::lane::Control;
use crate::msg::Msg;
use crate::options::{AuthDecision, ClientInfo, ConnectInfo};

mod delivery;
mod topics;
mod translate;

use delivery::Retry;

/// Datagrams queued per client before the gateway drops new ones.
const CLIENT_QUEUE: usize = 1024;

/// Buffer of the in-memory MQTT connection, each way.
const PIPE_BYTES: usize = 256 << 10;

/// How often retransmissions and sleep deadlines are checked.
const TICK: Duration = Duration::from_secs(1);

/// Largest UDP payload over IPv4, and so the largest datagram the gateway
/// sends.
const MAX_SEND: usize = 65_507;

/// Controls queued for a lane beyond which QoS -1 publishes are dropped.
const CONNECTIONLESS_QUEUE: usize = 4096;
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
                // A live client handles its CONNECT: a resend, the end of
                // its sleep, or a new start.
                let live = clients.borrow().get(&addr).map(|(_, tx)| tx.clone());
                if let Some(tx) = live.filter(|tx| !tx.is_closed()) {
                    let _ = tx.try_send(datagram.to_vec());
                    continue;
                }
                let (tx, rx) = mpsc::channel(CLIENT_QUEUE);
                next_id += 1;
                clients.borrow_mut().insert(addr, (next_id, tx));
                let task = Client::new(&shared, socket.clone(), addr);
                tokio::task::spawn_local(task.run(next_id, rx, datagram.to_vec(), clients.clone()));
            }
            sn::Packet::SearchGw { .. } => {
                let reply = sn::Packet::GwInfo {
                    gw_id: shared.opts.sn.gateway_id,
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
        .sn
        .predefined
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
        // Topic IDs need a connection; other forms are not supported.
        _ => None,
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
    // Anyone can send these, so they are dropped while the lane is behind.
    let _ = shared.lanes[lane]
        .control
        .push_below(Control::Publish(msg), CONNECTIONLESS_QUEUE);
}

/// One MQTT-SN client and its MQTT connection.
struct Client {
    shared: Arc<Shared>,
    socket: Rc<UdpSocket>,
    addr: SocketAddr,
    /// Client ID from CONNECT.
    id: String,
    /// Nothing but CONNECT came since the connection started, so another
    /// CONNECT is a resend.
    fresh: bool,
    /// Return code of the CONNACK sent, to answer a resent CONNECT.
    connack: Option<u8>,
    /// MQTT packets the gateway itself sends to the broker.
    to_broker: Vec<u8>,
    topics: Topics,
    /// Sleep deadline and duration while the client sleeps.
    asleep: Option<(Instant, Duration)>,
    /// Datagrams held while the client sleeps.
    buffered: VecDeque<Vec<u8>>,
    retry: Retries,
    replies: Replies,
}

/// The client's topic IDs.
#[derive(Default)]
struct Topics {
    /// Topic ID to name, both registered and assigned by the gateway.
    by_id: FxHashMap<u16, String>,
    ids: FxHashMap<String, u16>,
    next_id: u16,
    /// Topic IDs sent in REGISTER and not yet acked, by REGISTER message ID.
    registering: FxHashMap<u16, u16>,
    /// Datagrams waiting for a REGACK, by topic ID.
    held: FxHashMap<u16, Vec<Vec<u8>>>,
}

/// Datagrams the client must ack.
#[derive(Default)]
struct Retries {
    /// Waiting for an ack, by message type and ID.
    unacked: FxHashMap<(u8, u16), Retry>,
    next_msg_id: u16,
}

/// What translating an ack between the client and the broker needs.
#[derive(Default)]
struct Replies {
    /// Topic ID of each inbound QoS 1 publish, for the PUBACK.
    puback_topic: FxHashMap<u16, u16>,
    /// Filter of each SUBSCRIBE, for the topic ID in SUBACK.
    sub_filter: FxHashMap<u16, String>,
    /// Packet IDs of QoS 2 messages dropped for lack of a topic ID, waiting
    /// for PUBREL.
    dropped: FxHashSet<u16>,
}

enum Inbound {
    Continue,
    /// DISCONNECT with a sleep duration in seconds.
    Sleep(u16),
    Disconnect,
}

impl Client {
    fn new(shared: &Arc<Shared>, socket: Rc<UdpSocket>, addr: SocketAddr) -> Client {
        Client {
            shared: shared.clone(),
            socket,
            addr,
            id: String::new(),
            fresh: true,
            connack: None,
            to_broker: Vec::new(),
            topics: Topics::default(),
            asleep: None,
            buffered: VecDeque::new(),
            retry: Retries::default(),
            replies: Replies::default(),
        }
    }

    async fn run(
        mut self,
        id: u64,
        mut rx: mpsc::Receiver<Vec<u8>>,
        mut connect: Vec<u8>,
        clients: Clients,
    ) {
        while let Some(mut pipe) = self.connect(&mut rx, &connect).await {
            let Some(again) = self.serve(&mut rx, &mut pipe).await else {
                break;
            };
            // A new CONNECT starts over, like a new client. The old MQTT
            // connection closes as lost.
            connect = again;
            self = Client::new(&self.shared, self.socket.clone(), self.addr);
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
        client_id.clone_into(&mut self.id);
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
        let topic = self.ask(rx, &sn::Packet::WillTopicReq).await?;
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
        let msg = self.ask(rx, &sn::Packet::WillMsgReq).await?;
        let Ok((sn::Packet::WillMsg { message }, _)) = sn::decode(&msg) else {
            return None;
        };
        Some((flags, topic, message.to_vec()))
    }

    /// Sends `request` and waits for the answer. A resent CONNECT means the
    /// request got lost: it goes out again.
    async fn ask(
        &self,
        rx: &mut mpsc::Receiver<Vec<u8>>,
        request: &sn::Packet<'_>,
    ) -> Option<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + WILL_TIMEOUT;
        loop {
            self.send(request).await;
            let answer = tokio::time::timeout_at(deadline, rx.recv())
                .await
                .ok()
                .flatten()?;
            if !matches!(sn::decode(&answer), Ok((sn::Packet::Connect { .. }, _))) {
                return Some(answer);
            }
        }
    }

    /// Serves the client until it leaves. Returns a CONNECT that starts the
    /// client over.
    async fn serve(
        &mut self,
        rx: &mut mpsc::Receiver<Vec<u8>>,
        pipe: &mut DuplexStream,
    ) -> Option<Vec<u8>> {
        let mut buf = BytesMut::with_capacity(64 << 10);
        let mut out = Vec::new();
        let mut tick = tokio::time::interval(TICK.min(self.shared.opts.sn.retry));
        loop {
            tokio::select! {
                datagram = rx.recv() => {
                    let datagram = datagram?;
                    let Ok((packet, _)) = sn::decode(&datagram) else { continue };
                    if let sn::Packet::Connect { flags, client_id, .. } = packet {
                        if !self.reconnects(flags, client_id).await {
                            continue;
                        }
                        return Some(datagram);
                    }
                    self.fresh = false;
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
                            return None;
                        }
                    }
                    if !out.is_empty() {
                        if pipe.write_all(&out).await.is_err() {
                            return None;
                        }
                        out.clear();
                    }
                }
                n = pipe.read_buf(&mut buf) => {
                    if !matches!(n, Ok(n) if n > 0) {
                        self.send(&sn::Packet::Disconnect { duration: None }).await;
                        return None;
                    }
                    if !self.relay(&mut buf, pipe).await {
                        return None;
                    }
                }
                _ = tick.tick() => {
                    if !self.on_tick(pipe).await {
                        return None;
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

    /// Handles a CONNECT on a live connection. A sleeping client wakes up,
    /// and a resend gets the CONNACK again. Returns true for a new start.
    async fn reconnects(&mut self, flags: Flags, client_id: &str) -> bool {
        if self.asleep.is_some() && !flags.clean_session {
            self.send(&sn::Packet::ConnAck { code: sn::ACCEPTED }).await;
            self.wake().await;
            return false;
        }
        if !self.fresh || client_id != self.id {
            return true;
        }
        // The broker's CONNACK may still be on its way.
        if let Some(code) = self.connack {
            self.send(&sn::Packet::ConnAck { code }).await;
        }
        false
    }
}

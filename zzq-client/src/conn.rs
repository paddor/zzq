//! The client session task: connects, runs one connection at a time and
//! reconnects with backoff.
//!
//! [`Session`] outlives connections. It holds the requests still waiting
//! for an ack, as encoded packets, and the subscriptions, so a new
//! connection can resend the first and restore the second.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::{Buf, BytesMut};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};
use zzq_proto::encode::{self, AckFields, ConnectFields, PublishHeader, WillFields};
use zzq_proto::packet::{ConnAck, Publish, decode};
use zzq_proto::property::{self, Property};
use zzq_proto::{Packet, PacketType, QoS, ReasonCode, SubscriptionOptions, Version, reason};
use zzq_tokio::transport::{Driver, TransportOptions};
use zzq_tokio::{Context, Endpoint};

use crate::{ClientOptions, Cmd, Connected, Error, Message, Result};

/// Largest packet accepted from the broker.
const MAX_PACKET: usize = 256 << 20;

/// Connects, then keeps the session going until the client disconnects,
/// the handle is dropped, or a connection fails without `reconnect`.
pub(crate) async fn run_session(
    ctx: Context,
    endpoint: Endpoint,
    transport: TransportOptions,
    mut s: Session,
) {
    let (min, max) = s
        .opts
        .reconnect
        .map_or((Duration::ZERO, Duration::ZERO), |r| (r.min, r.max));
    let mut delay = min;
    let limit = s.opts.connect_timeout;
    loop {
        let connect = zzq_tokio::transport::connect(&ctx, &endpoint, &transport);
        match tokio::time::timeout(limit, connect).await {
            Ok(Ok(stream)) => match stream.drive(ConnDriver(s)).await {
                Ok(back) => s = back,
                Err(_) => return,
            },
            failed => {
                if let Some(tx) = s.connack.take() {
                    let e = match failed {
                        Ok(Err(e)) => e.into(),
                        _ => Error::Closed,
                    };
                    let _ = tx.send(Err(e));
                    return;
                }
            }
        }
        if s.ended || s.opts.reconnect.is_none() || s.cmd.is_closed() {
            break;
        }
        if s.established {
            s.established = false;
            delay = min;
        }
        if !s.wait_offline(delay).await {
            break;
        }
        delay = (delay * 2).min(max);
    }
    s.fail_pending();
}

#[derive(Debug)]
enum Pending {
    Publish(Option<oneshot::Sender<Result<()>>>),
    Sub(oneshot::Sender<Result<Vec<ReasonCode>>>, Change),
    /// A resubscribe after reconnecting, nobody waits for it.
    Restore,
}

/// What a SUBACK or UNSUBACK changes in the subscriptions to restore.
#[derive(Debug)]
enum Change {
    Add(Vec<(String, SubscriptionOptions)>, u32),
    Remove(Vec<String>),
}

/// State that outlives a connection.
#[derive(Debug)]
pub(crate) struct Session {
    pub(crate) opts: ClientOptions,
    pub(crate) cmd: mpsc::UnboundedReceiver<Cmd>,
    pub(crate) inbox: mpsc::Sender<Message>,
    pub(crate) connack: Option<oneshot::Sender<Result<Connected>>>,
    pending: FxHashMap<u16, Pending>,
    /// Encoded requests not yet acked, by packet ID, with their send order.
    unacked: FxHashMap<u16, (u64, Vec<u8>)>,
    sent: u64,
    awaiting_rel: FxHashSet<u16>,
    /// QoS 2 messages in the inbox, PUBREC not sent yet.
    unconsumed: FxHashSet<u16>,
    next_pkid: u16,
    /// Granted subscriptions: filter, options and Subscription Identifier.
    subs: Vec<(String, SubscriptionOptions, u32)>,
    /// Commands that came while offline.
    backlog: VecDeque<Cmd>,
    /// QoS 1/2 publishes beyond the broker's Receive Maximum.
    waiting: VecDeque<Cmd>,
    /// QoS 1/2 publishes sent and not yet acked.
    inflight: usize,
    /// The broker's Receive Maximum.
    quota: usize,
    /// A connection got its CONNACK since the last backoff.
    established: bool,
    /// Do not reconnect.
    ended: bool,
}

impl Session {
    pub(crate) fn new(
        opts: ClientOptions,
        cmd: mpsc::UnboundedReceiver<Cmd>,
        inbox: mpsc::Sender<Message>,
        connack: oneshot::Sender<Result<Connected>>,
    ) -> Session {
        Session {
            opts,
            cmd,
            inbox,
            connack: Some(connack),
            pending: FxHashMap::default(),
            unacked: FxHashMap::default(),
            sent: 0,
            awaiting_rel: FxHashSet::default(),
            unconsumed: FxHashSet::default(),
            next_pkid: 0,
            subs: Vec::new(),
            backlog: VecDeque::new(),
            waiting: VecDeque::new(),
            inflight: 0,
            quota: usize::from(u16::MAX),
            established: false,
            ended: false,
        }
    }
}

struct ConnDriver(Session);

impl Driver for ConnDriver {
    type Output = Session;

    fn run<R, W>(self, reader: R, writer: W) -> impl Future<Output = Session>
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
        run(self.0, reader, writer)
    }
}

/// Output shared with the writer task.
#[derive(Debug, Default)]
struct Out {
    buf: RefCell<Vec<u8>>,
    wake: Notify,
    closed: std::cell::Cell<bool>,
    /// Answered once DISCONNECT is written.
    bye: RefCell<Option<oneshot::Sender<()>>>,
}

impl Out {
    fn push(&self, f: impl FnOnce(&mut Vec<u8>)) {
        f(&mut self.buf.borrow_mut());
        self.wake.notify_one();
    }

    fn close(&self) {
        self.closed.set(true);
        self.wake.notify_one();
    }
}

/// One connection of a session.
struct Conn<'a> {
    s: &'a mut Session,
    out: Rc<Out>,
}

async fn run<R, W>(mut s: Session, mut r: R, mut w: W) -> Session
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    let mut buf = BytesMut::with_capacity(64 << 10);
    let resume = s.connack.is_none();
    let handshake = handshake(&s.opts, resume, &mut r, &mut w, &mut buf);
    let connected = tokio::time::timeout(s.opts.connect_timeout, handshake)
        .await
        .unwrap_or(Err(Error::Closed));
    let session_present = match connected {
        Ok((c, quota)) => {
            s.established = true;
            s.quota = usize::from(quota.max(1));
            let present = c.session_present;
            if let Some(tx) = s.connack.take() {
                let _ = tx.send(Ok(c));
            }
            present
        }
        Err(e) => {
            // A refused first connect ends the client; a lost one retries.
            if let Some(tx) = s.connack.take() {
                let _ = tx.send(Err(e));
                s.ended = true;
            } else if let Error::Refused(code) = e {
                s.ended = !retryable(code);
            }
            return s;
        }
    };
    let out = Rc::new(Out::default());
    tokio::task::spawn_local(writer(out.clone(), w));
    let mut c = Conn { s: &mut s, out };
    if resume {
        c.restore(session_present);
    }
    let _ = c.serve(&mut r, &mut buf).await;
    c.out.close();
    s
}

impl Conn<'_> {
    /// Reads, runs commands and pings until the connection ends.
    async fn serve<R: AsyncRead + Unpin>(&mut self, r: &mut R, buf: &mut BytesMut) -> Result<()> {
        self.input(buf).await?;
        for cmd in std::mem::take(&mut self.s.backlog) {
            self.command(cmd);
        }
        let ka = u64::from(self.s.opts.keep_alive);
        let mut ping = tokio::time::interval(Duration::from_secs(ka.max(1)));
        ping.tick().await;
        let mut last_rx = Instant::now();
        loop {
            tokio::select! {
                cmd = self.s.cmd.recv() => match cmd {
                    Some(Cmd::Disconnect { reason, done }) => {
                        let v = self.s.opts.version;
                        self.out.push(|b| encode::reason_only(PacketType::Disconnect, v, reason.into(), &[], b));
                        self.s.ended = true;
                        *self.out.bye.borrow_mut() = Some(done);
                        return Ok(());
                    }
                    Some(c) => self.command(c),
                    None => {
                        self.s.ended = true;
                        return Ok(());
                    }
                },
                n = r.read_buf(buf) => {
                    if !matches!(n, Ok(n) if n > 0) {
                        return Err(Error::Closed);
                    }
                    last_rx = Instant::now();
                    self.input(buf).await?;
                }
                _ = ping.tick(), if ka > 0 => {
                    if last_rx.elapsed() > Duration::from_millis(ka * 1500) {
                        return Err(Error::Closed);
                    }
                    self.out.push(encode::pingreq);
                }
            }
        }
    }

    /// After a reconnect: resends unacked requests and, if the broker lost
    /// the session, the subscriptions.
    fn restore(&mut self, session_present: bool) {
        // Restores from an earlier connection would only add duplicates.
        let stale: Vec<u16> = self
            .s
            .pending
            .iter()
            .filter(|(_, p)| matches!(p, Pending::Restore))
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            self.s.pending.remove(&id);
            self.s.unacked.remove(&id);
        }
        let mut resend: Vec<(u64, Vec<u8>)> = self.s.unacked.values().cloned().collect();
        resend.sort_by_key(|(seq, _)| *seq);
        if !session_present {
            self.s.awaiting_rel.clear();
            self.s.unconsumed.clear();
            let mut by_id: Vec<(u32, Vec<(String, SubscriptionOptions)>)> = Vec::new();
            for (f, o, id) in &self.s.subs {
                match by_id.iter_mut().find(|(i, _)| i == id) {
                    Some((_, list)) => list.push((f.clone(), *o)),
                    None => by_id.push((*id, vec![(f.clone(), *o)])),
                }
            }
            for (id, filters) in by_id {
                let Some(pkid) = self.pkid() else {
                    // Every packet ID is taken: send it once one is free.
                    self.s.waiting.push_front(Cmd::Subscribe {
                        filters,
                        sub_id: id,
                        done: oneshot::channel().0,
                    });
                    continue;
                };
                let packet = self.subscribe_packet(pkid, &filters, id);
                self.send_tracked(pkid, packet, Pending::Restore);
            }
        }
        for (_, mut packet) in resend {
            if packet.first().is_some_and(|b| b >> 4 == 3) {
                packet[0] |= 0x08;
            }
            self.out.push(|b| b.extend_from_slice(&packet));
        }
    }

    /// A free packet ID, if there is one.
    fn pkid(&mut self) -> Option<u16> {
        if !self.ids_left() {
            return None;
        }
        for _ in 0..u16::MAX {
            self.s.next_pkid = self.s.next_pkid.wrapping_add(1).max(1);
            if !self.s.pending.contains_key(&self.s.next_pkid) {
                return Some(self.s.next_pkid);
            }
        }
        None
    }

    fn ids_left(&self) -> bool {
        self.s.pending.len() < usize::from(u16::MAX)
    }

    /// Whether `cmd` has to wait: for the broker's Receive Maximum, or for
    /// a free packet ID.
    fn blocked(&self, cmd: &Cmd) -> bool {
        match cmd {
            Cmd::Publish { qos, .. } if *qos != QoS::AtMostOnce => {
                self.s.inflight >= self.s.quota || !self.ids_left()
            }
            Cmd::Subscribe { .. } | Cmd::Unsubscribe { .. } => !self.ids_left(),
            _ => false,
        }
    }

    /// Sends waiting commands that may go now, in order.
    fn send_waiting(&mut self) {
        while let Some(cmd) = self.s.waiting.front()
            && !self.blocked(cmd)
        {
            if let Some(cmd) = self.s.waiting.pop_front() {
                self.command(cmd);
            }
        }
    }

    /// Sends `packet` and keeps it for resending until acked.
    fn send_tracked(&mut self, pkid: u16, packet: Vec<u8>, pending: Pending) {
        self.out.push(|b| b.extend_from_slice(&packet));
        self.s.sent += 1;
        self.s.unacked.insert(pkid, (self.s.sent, packet));
        self.s.pending.insert(pkid, pending);
    }

    fn subscribe_packet(
        &self,
        pkid: u16,
        filters: &[(String, SubscriptionOptions)],
        sub_id: u32,
    ) -> Vec<u8> {
        let list: Vec<(&str, _)> = filters.iter().map(|(f, o)| (f.as_str(), *o)).collect();
        let props: Vec<Property<'_>> = if sub_id > 0 {
            vec![Property::VarInt(property::SUBSCRIPTION_IDENTIFIER, sub_id)]
        } else {
            Vec::new()
        };
        let mut b = Vec::new();
        encode::subscribe(self.s.opts.version, pkid, &props, &list, &mut b);
        b
    }

    fn command(&mut self, cmd: Cmd) {
        if self.blocked(&cmd) {
            self.s.waiting.push_back(cmd);
            return;
        }
        let v = self.s.opts.version;
        // Not blocked, so a packet ID is free.
        match cmd {
            Cmd::Publish {
                topic,
                payload,
                qos,
                retain,
                done,
            } => {
                let pkid = if qos == QoS::AtMostOnce {
                    0
                } else {
                    let Some(pkid) = self.pkid() else { return };
                    self.s.inflight += 1;
                    pkid
                };
                self.send_publish(&topic, &payload, (qos, retain, pkid), done);
            }
            Cmd::Subscribe {
                filters,
                sub_id,
                done,
            } => {
                let Some(pkid) = self.pkid() else { return };
                let packet = self.subscribe_packet(pkid, &filters, sub_id);
                self.send_tracked(
                    pkid,
                    packet,
                    Pending::Sub(done, Change::Add(filters, sub_id)),
                );
            }
            Cmd::Unsubscribe { filters, done } => {
                let Some(pkid) = self.pkid() else { return };
                let list: Vec<&str> = filters.iter().map(String::as_str).collect();
                let mut b = Vec::new();
                encode::unsubscribe(v, pkid, &[], &list, &mut b);
                self.send_tracked(pkid, b, Pending::Sub(done, Change::Remove(filters)));
            }
            Cmd::Ack { pkid, qos } => self.ack_taken(pkid, qos),
            Cmd::Disconnect { .. } => {}
        }
    }

    fn send_publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        (qos, retain, pkid): (QoS, bool, u16),
        done: Option<oneshot::Sender<Result<()>>>,
    ) {
        let h = PublishHeader {
            version: self.s.opts.version,
            dup: false,
            qos,
            retain,
            topic,
            pkid,
            properties: &[],
        };
        if qos == QoS::AtMostOnce {
            self.out.push(|b| encode::publish(&h, payload, b));
        } else {
            let mut b = Vec::new();
            encode::publish(&h, payload, &mut b);
            self.send_tracked(pkid, b, Pending::Publish(done));
        }
    }

    /// Acks a message the application took.
    fn ack_taken(&mut self, pkid: u16, qos: QoS) {
        let kind = match qos {
            QoS::AtMostOnce => return,
            QoS::AtLeastOnce => PacketType::PubAck,
            QoS::ExactlyOnce => {
                self.s.unconsumed.remove(&pkid);
                PacketType::PubRec
            }
        };
        let v = self.s.opts.version;
        self.out
            .push(|b| encode::ack(&AckFields::new(kind, pkid), v, b));
    }

    async fn input(&mut self, buf: &mut BytesMut) -> Result<()> {
        loop {
            let (msg, n) = match decode(buf, self.s.opts.version, MAX_PACKET) {
                Ok(Some((packet, n))) => (self.packet(packet)?, n),
                Ok(None) => return Ok(()),
                Err(_) => return Err(Error::Protocol("malformed packet")),
            };
            buf.advance(n);
            if let Some(m) = msg
                && self.s.inbox.send(m).await.is_err()
            {
                self.s.ended = true;
                return Err(Error::Closed);
            }
        }
    }

    fn packet(&mut self, packet: Packet<'_>) -> Result<Option<Message>> {
        let v = self.s.opts.version;
        match packet {
            Packet::Publish(p) => return Ok(self.publish(&p)),
            Packet::PubAck(a) | Packet::PubComp(a) => self.done(a.pkid, a.reason),
            Packet::PubRec(a) => {
                if a.reason >= 0x80 {
                    self.done(a.pkid, a.reason);
                } else if let Some((_, packet)) = self.s.unacked.get_mut(&a.pkid) {
                    // Resend PUBREL, not the PUBLISH, from now on.
                    packet.clear();
                    encode::ack(&AckFields::new(PacketType::PubRel, a.pkid), v, packet);
                    let rel = packet.clone();
                    self.out.push(|b| b.extend_from_slice(&rel));
                }
            }
            Packet::PubRel(a) => {
                self.s.awaiting_rel.remove(&a.pkid);
                self.out
                    .push(|b| encode::ack(&AckFields::new(PacketType::PubComp, a.pkid), v, b));
            }
            Packet::SubAck(ack) | Packet::UnsubAck(ack) => {
                self.s.unacked.remove(&ack.pkid);
                match self.s.pending.remove(&ack.pkid) {
                    Some(Pending::Sub(tx, change)) => {
                        self.apply(change, ack.codes);
                        let _ = tx.send(Ok(ack.codes.iter().map(|&c| c.into()).collect()));
                    }
                    Some(Pending::Restore | Pending::Publish(_)) | None => {}
                }
                self.send_waiting();
            }
            Packet::PingResp => {}
            Packet::Disconnect(_) => return Err(Error::Closed),
            _ => return Err(Error::Protocol("unexpected packet")),
        }
        Ok(None)
    }

    /// Records granted subscriptions and removed ones for later restores.
    fn apply(&mut self, change: Change, codes: &[u8]) {
        let subs = &mut self.s.subs;
        match change {
            Change::Add(filters, sub_id) => {
                for (i, (f, o)) in filters.into_iter().enumerate() {
                    if codes.get(i).is_some_and(|&c| c < 0x80) {
                        subs.retain(|(g, _, _)| *g != f);
                        subs.push((f, o, sub_id));
                    }
                }
            }
            Change::Remove(filters) => subs.retain(|(g, _, _)| !filters.contains(g)),
        }
    }

    fn done(&mut self, pkid: u16, code: u8) {
        self.s.unacked.remove(&pkid);
        let Some(Pending::Publish(tx)) = self.s.pending.remove(&pkid) else {
            return;
        };
        if let Some(tx) = tx {
            let _ = tx.send(if code >= 0x80 {
                Err(Error::Rejected(code.into()))
            } else {
                Ok(())
            });
        }
        self.s.inflight = self.s.inflight.saturating_sub(1);
        self.send_waiting();
    }

    fn publish(&mut self, p: &Publish<'_>) -> Option<Message> {
        // Acks wait until the application takes the message.
        let ack = match p.qos {
            QoS::AtMostOnce => None,
            QoS::AtLeastOnce => Some(p.pkid),
            QoS::ExactlyOnce => {
                if !self.s.awaiting_rel.insert(p.pkid) {
                    // A resend. Answer it if the application has the first.
                    if !self.s.unconsumed.contains(&p.pkid) {
                        let v = self.s.opts.version;
                        self.out.push(|b| {
                            encode::ack(&AckFields::new(PacketType::PubRec, p.pkid), v, b);
                        });
                    }
                    return None;
                }
                self.s.unconsumed.insert(p.pkid);
                Some(p.pkid)
            }
        };
        let subscription_ids = p
            .properties
            .iter()
            .filter_map(|x| match x {
                Property::VarInt(property::SUBSCRIPTION_IDENTIFIER, id) => Some(id),
                _ => None,
            })
            .collect();
        Some(Message {
            topic: p.topic.to_owned(),
            payload: bytes::Bytes::copy_from_slice(p.payload),
            qos: p.qos,
            retain: p.retain,
            subscription_ids,
            ack,
        })
    }
}

impl Session {
    /// Waits `delay` before the next connect. Takes commands meanwhile, so
    /// `disconnect` works while offline. Returns false to stop.
    async fn wait_offline(&mut self, delay: Duration) -> bool {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => return true,
                cmd = self.cmd.recv() => match cmd {
                    Some(Cmd::Disconnect { done, .. }) => {
                        let _ = done.send(());
                        self.ended = true;
                        return false;
                    }
                    Some(c) => self.backlog.push_back(c),
                    None => {
                        self.ended = true;
                        return false;
                    }
                },
            }
        }
    }

    /// Fails every request still waiting for an ack.
    fn fail_pending(&mut self) {
        for cmd in self.backlog.drain(..).chain(self.waiting.drain(..)) {
            match cmd {
                Cmd::Publish { done: Some(tx), .. } => {
                    let _ = tx.send(Err(Error::Closed));
                }
                Cmd::Subscribe { done, .. } | Cmd::Unsubscribe { done, .. } => {
                    let _ = done.send(Err(Error::Closed));
                }
                Cmd::Disconnect { done, .. } => {
                    let _ = done.send(());
                }
                Cmd::Publish { done: None, .. } | Cmd::Ack { .. } => {}
            }
        }
        for (_, p) in self.pending.drain() {
            match p {
                Pending::Publish(Some(tx)) => {
                    let _ = tx.send(Err(Error::Closed));
                }
                Pending::Sub(tx, _) => {
                    let _ = tx.send(Err(Error::Closed));
                }
                Pending::Publish(None) | Pending::Restore => {}
            }
        }
    }
}

/// Sends CONNECT and waits for CONNACK. A resumed session does not start
/// clean if it outlives the connection.
async fn handshake<R, W>(
    opts: &ClientOptions,
    resume: bool,
    r: &mut R,
    w: &mut W,
    buf: &mut BytesMut,
) -> Result<(Connected, u16)>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut props = Vec::new();
    if opts.session_expiry > 0 {
        props.push(Property::U32(
            property::SESSION_EXPIRY_INTERVAL,
            opts.session_expiry,
        ));
    }
    // Unacked QoS 1/2 messages fit in the inbox, so it never blocks them.
    let window = u16::try_from(opts.inbox.max(1)).unwrap_or(u16::MAX);
    if window < u16::MAX {
        props.push(Property::U16(property::RECEIVE_MAXIMUM, window));
    }
    let keeps_session = if opts.version.is_v5() {
        opts.session_expiry > 0
    } else {
        !opts.clean_start
    };
    let will_props = opts
        .will
        .as_ref()
        .filter(|w| w.delay > 0)
        .map(|w| [Property::U32(property::WILL_DELAY_INTERVAL, w.delay)]);
    let fields = ConnectFields {
        version: opts.version,
        clean_start: opts.clean_start && !(resume && keeps_session),
        keep_alive: opts.keep_alive,
        properties: &props,
        client_id: &opts.client_id,
        will: opts.will.as_ref().map(|w| WillFields {
            qos: w.qos,
            retain: w.retain,
            properties: will_props.as_ref().map_or(&[][..], |p| &p[..]),
            topic: &w.topic,
            payload: &w.payload,
        }),
        username: opts.username.as_deref(),
        password: opts.password.as_deref(),
    };
    let mut out = Vec::new();
    encode::connect(&fields, &mut out);
    w.write_all(&out)
        .await
        .map_err(|e| Error::Engine(e.into()))?;
    loop {
        match decode(buf, opts.version, MAX_PACKET) {
            Ok(Some((Packet::ConnAck(c), n))) => {
                let res = connack(&c, opts.version);
                buf.advance(n);
                return res;
            }
            Ok(Some(_)) | Err(_) => return Err(Error::Protocol("expected CONNACK")),
            Ok(None) => {}
        }
        match r.read_buf(buf).await {
            Ok(0) | Err(_) => return Err(Error::Closed),
            Ok(_) => {}
        }
    }
}

/// CONNACK details and the broker's Receive Maximum.
fn connack(c: &ConnAck<'_>, version: Version) -> Result<(Connected, u16)> {
    if c.code != 0 {
        // v3.1.1 return codes become the v5 codes with the same meaning.
        let code = if version.is_v5() {
            c.code
        } else {
            reason::v311::to_v5(c.code)
        };
        return Err(Error::Refused(code.into()));
    }
    let connected = Connected {
        session_present: c.session_present,
        assigned_client_id: c
            .properties
            .get_str(property::ASSIGNED_CLIENT_IDENTIFIER)
            .map(str::to_owned),
    };
    let quota = c
        .properties
        .get_int(property::RECEIVE_MAXIMUM)
        .map_or(u16::MAX, |v| u16::try_from(v).unwrap_or(u16::MAX));
    Ok((connected, quota))
}

/// Refusals worth another attempt: the broker is busy or full, not
/// rejecting the client.
const fn retryable(code: ReasonCode) -> bool {
    matches!(
        code,
        ReasonCode::SERVER_UNAVAILABLE
            | ReasonCode::SERVER_BUSY
            | ReasonCode::QUOTA_EXCEEDED
            | ReasonCode::CONNECTION_RATE_EXCEEDED
    )
}

async fn writer<W: AsyncWrite + Unpin>(out: Rc<Out>, mut w: W) {
    let mut spare = Vec::new();
    loop {
        let buf = std::mem::replace(&mut *out.buf.borrow_mut(), std::mem::take(&mut spare));
        if buf.is_empty() {
            if out.closed.get() {
                let _ = w.shutdown().await;
                if let Some(done) = out.bye.borrow_mut().take() {
                    let _ = done.send(());
                }
                return;
            }
            spare = buf;
            // TLS may only have buffered the last bytes.
            if w.flush().await.is_err() {
                return;
            }
            out.wake.notified().await;
            continue;
        }
        if w.write_all(&buf).await.is_err() {
            return;
        }
        spare = buf;
        spare.clear();
    }
}

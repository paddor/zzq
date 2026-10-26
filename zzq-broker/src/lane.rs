//! State of one broker lane: its sessions, a copy of the subscription
//! index, and the packet handling for its connections.

use std::cell::RefCell;
use std::io;
use std::ops::Range;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use bytestring::ByteString;
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::sync::{Notify, oneshot};
use zzq_proto::encode::{self, SubAckFields};
use zzq_proto::packet::{Ack, Publish, Subscribe, Unsubscribe, decode};
use zzq_proto::property::{self, Property};
use zzq_proto::topic::{SharedFilter, is_shared};
use zzq_proto::{Packet, PacketType, QoS, SubscriptionOptions, Version, reason};

use crate::broker::Shared;
use crate::budget::{self, Charge, LaneTotal};
use crate::conn::{ConnShared, OwnedConnect, ReadBlocks};
use crate::dump::Part;
use crate::index::{Entry, Matches, SubIds, SubIndex, SubRef};
use crate::msg::{Arena, Block, Msg, Publication};
use crate::options::{Authorizer, ClientInfo};
use crate::session::{DelayedWill, Kept, Outcome, QueueLimit, Queued, Session, Waiter};
use crate::slab::{Key, Slab};
use crate::state::SessionState;

pub(crate) type LaneRef = Rc<RefCell<Lane>>;

/// Calls of [`Lane::sweep`] that cover all sessions once.
pub(crate) const SWEEP_PARTS: u32 = 20;

/// Lane counters, summed by `Broker::stats`.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct LaneStats {
    pub(crate) connections: u64,
    pub(crate) sessions: u64,
    pub(crate) subscriptions: u64,
    pub(crate) publishes: u64,
    pub(crate) deliveries: u64,
    pub(crate) dropped_buffer_full: u64,
    pub(crate) dropped_offline: u64,
    pub(crate) dropped_queue_full: u64,
    pub(crate) dropped_too_large: u64,
    pub(crate) dropped_expired: u64,
    pub(crate) dropped_session_ended: u64,
    pub(crate) dropped_state_full: u64,
    /// Bytes the lane's sessions count against `max_state_bytes`.
    pub(crate) state_bytes: u64,
}

/// A delivery for a session on another lane.
#[derive(Debug)]
pub(crate) enum Remote {
    /// PUBLISH packets for the target lane to route to its own sessions,
    /// as offset and length into one shared buffer.
    Forward {
        chunk: Bytes,
        first: (u32, u32),
        more: Vec<(u32, u32)>,
        origin: Origin,
    },
    /// A delivery to one session, chosen for a shared subscription.
    Deliver { key: Key, item: Queued },
    /// The last batch the sending lane ships before a state dump.
    Drained(Arc<SyncAck>),
}

/// How forwarded packets were sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Origin {
    version: Version,
    /// The publisher, for No Local.
    from: Option<SubRef>,
    /// The chunk is the publisher's read buffer, which long-held messages
    /// must not pin, not a copy made for these packets.
    pins: bool,
}

/// Counts the lanes that still have to apply a subscription change.
#[derive(Debug, Default)]
pub(crate) struct SyncAck {
    remaining: AtomicUsize,
    notify: Notify,
}

impl SyncAck {
    pub(crate) fn expect(&self, n: usize) {
        self.remaining.fetch_add(n, Ordering::AcqRel);
    }

    fn done(&self) {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.notify.notify_one();
        }
    }

    /// Waits until every lane applied the change.
    pub(crate) async fn wait(&self) {
        while self.remaining.load(Ordering::Acquire) > 0 {
            self.notify.notified().await;
        }
    }
}

/// An ack packet held until other lanes applied a subscription change.
#[derive(Debug)]
pub(crate) struct Held {
    pub(crate) key: Key,
    pub(crate) sync: Arc<SyncAck>,
    pub(crate) packet: Vec<u8>,
    /// New subscriptions that get retained messages after the ack.
    pub(crate) retained: Vec<Wanted>,
}

/// A subscription that gets the retained messages it matches: filter,
/// granted QoS and Subscription Identifier.
type Wanted = (Box<str>, QoS, u32);

/// Work sent to a lane by other lanes or the broker handle.
#[derive(Debug)]
pub(crate) enum Control {
    /// Add a subscription, then count down the ack.
    SubAdd(Box<str>, Entry, Option<Arc<SyncAck>>),
    /// Remove a subscription, then count down the ack.
    SubRemove(Box<str>, SubRef, Option<Arc<SyncAck>>),
    Publish(Msg),
    /// A session on another lane crossed its queue watermark.
    Congested(SubRef, bool),
    Stats(oneshot::Sender<LaneStats>),
    /// Close every connection and stop, then answer.
    Shutdown(Option<oneshot::Sender<()>>),
    /// Close every connection as lost and refuse new ones, then answer.
    Quiesce(oneshot::Sender<()>),
    /// Ship what is waiting for other lanes, then a marker to each.
    Drain(Arc<SyncAck>),
    /// Write the sessions that outlive their connection, then answer.
    Dump(Box<Dump>),
    /// Add sessions from a state file, then answer. Their subscriptions
    /// count down the ack on the other lanes.
    Restore(Vec<SessionState>, Arc<SyncAck>, oneshot::Sender<()>),
}

/// A lane's share of the state file being written.
#[derive(Debug)]
pub(crate) struct Dump {
    pub(crate) part: Part,
    pub(crate) reply: oneshot::Sender<io::Result<()>>,
}

/// A congested session a QoS 1/2 publish was routed to.
#[derive(Debug, Clone, Copy)]
enum Hit {
    Local(Key),
    Remote(SubRef),
}

/// The message of one publish once a session holds it, and the charge for
/// its bytes once more than one session of the lane may hold it.
#[derive(Debug, Default)]
struct Owned {
    msg: Option<Msg>,
    share: Option<Rc<Charge>>,
}

impl Owned {
    const fn of(msg: Msg) -> Owned {
        Owned {
            msg: Some(msg),
            share: None,
        }
    }
}

/// QoS, retain flag and Subscription Identifier of one delivery.
#[derive(Debug, Clone, Copy)]
struct Grant {
    qos: QoS,
    retain: bool,
    sub_id: SubIds,
    /// The last delivery of this publish: it may take the owned message
    /// instead of cloning it.
    last: bool,
    /// A retained message sent on subscribe. `max_retained` bounds those,
    /// so `max_queued` does not drop them.
    on_subscribe: bool,
}

/// Why a connection's read loop ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Close {
    /// DISCONNECT from the client. `will` asks for the will anyway.
    Graceful { will: bool },
    /// Socket closed or failed.
    Lost,
    /// Closed by the broker; a DISCONNECT may already be queued.
    Kicked,
    /// Protocol violation with a v5 reason code.
    Protocol(u8),
}

/// Result of attaching a CONNECT to a session.
#[derive(Debug)]
pub(crate) struct Attached {
    pub(crate) key: Key,
}

#[derive(Debug)]
pub(crate) struct Lane {
    pub(crate) id: u16,
    pub(crate) shared: Arc<Shared>,
    pub(crate) sessions: Slab<Session>,
    by_client: FxHashMap<Box<str>, Key>,
    index: SubIndex,
    dirty: Vec<Key>,
    remote: Box<[Vec<Remote>]>,
    /// One task ships at a time, in arrival order, so batches for a lane
    /// leave in the order they were collected, also while it is full.
    pub(crate) shipping: Rc<tokio::sync::Mutex<()>>,
    held: Vec<Held>,
    /// First congested target of the publish being routed.
    hit: Option<Hit>,
    /// Congested sessions on other lanes.
    remote_congested: FxHashSet<SubRef>,
    /// Copies of QoS 1/2 publishes for congested sessions on other lanes.
    fwd: Arena,
    /// Acks of local publishers held for congested remote sessions.
    remote_waiters: FxHashMap<SubRef, Vec<Waiter>>,
    pub(crate) stats: LaneStats,
    /// Bytes the sessions charge against `max_state_bytes`.
    state: LaneTotal,
    /// The state budget was reached at the last check.
    pub(crate) state_full: bool,
    /// What connections without a buffer of their own read into.
    pub(crate) read_blocks: ReadBlocks,
    /// Calls of [`Lane::sweep`] so far.
    sweeps: u32,
    next_conn_id: u64,
    next_client_no: u64,
    next_will_id: u64,
    pub(crate) shut_down: bool,
    /// Closing for a state dump: no new connections, no wills on timers.
    pub(crate) closing: bool,
}

impl Lane {
    pub(crate) fn new(id: u16, shared: Arc<Shared>) -> Lane {
        let lanes = shared.lanes.len();
        Lane {
            id,
            shared,
            sessions: Slab::default(),
            by_client: FxHashMap::default(),
            index: SubIndex::default(),
            dirty: Vec::new(),
            remote: (0..lanes).map(|_| Vec::new()).collect(),
            shipping: Rc::default(),
            held: Vec::new(),
            hit: None,
            remote_congested: FxHashSet::default(),
            fwd: Arena::default(),
            remote_waiters: FxHashMap::default(),
            stats: LaneStats::default(),
            state: LaneTotal::default(),
            state_full: false,
            read_blocks: ReadBlocks::default(),
            sweeps: 0,
            next_conn_id: 0,
            next_client_no: 0,
            next_will_id: 0,
            shut_down: false,
            closing: false,
        }
    }

    pub(crate) const fn next_conn_id(&mut self) -> u64 {
        self.next_conn_id += 1;
        self.next_conn_id
    }

    /// A client ID for a client that sent none.
    /// A fresh client ID that `lane_for` maps to this lane, so the client
    /// finds its session here when it reconnects.
    pub(crate) fn assign_client_id(&mut self) -> String {
        let lanes = self.shared.lanes.len();
        loop {
            self.next_client_no += 1;
            let id = format!("zzq-{}-{}-{}", self.shared.id, self.id, self.next_client_no);
            if crate::broker::lane_for(&id, lanes) == usize::from(self.id) {
                return id;
            }
        }
    }

    const fn sub_ref(&self, key: Key) -> SubRef {
        SubRef { lane: self.id, key }
    }

    #[inline]
    fn mark_in(dirty: &mut Vec<Key>, s: &mut Session, key: Key) {
        if !s.dirty {
            s.dirty = true;
            dirty.push(key);
        }
    }

    #[inline]
    fn mark(&mut self, key: Key) {
        if let Some(s) = self.sessions.get_mut(key)
            && !s.dirty
        {
            s.dirty = true;
            self.dirty.push(key);
        }
    }

    /// Wakes the writers of sessions with new output.
    #[inline]
    pub(crate) fn flush(&mut self) {
        for key in self.dirty.drain(..) {
            if let Some(s) = self.sessions.get_mut(key) {
                s.dirty = false;
                if let Some(c) = &s.conn {
                    c.wake.notify_one();
                }
            }
        }
    }

    /// Whether batches for other lanes are waiting.
    pub(crate) fn has_remote(&self) -> bool {
        self.remote.iter().any(|b| !b.is_empty())
    }

    /// The next batch for another lane, and its lane. Its place gets a
    /// vector of the same size, so the next batch does not grow from empty.
    pub(crate) fn next_remote(&mut self) -> Option<(usize, Vec<Remote>)> {
        let (lane, batch) = self
            .remote
            .iter_mut()
            .enumerate()
            .find(|(_, b)| !b.is_empty())?;
        let len = batch.len();
        Some((lane, std::mem::replace(batch, Vec::with_capacity(len))))
    }

    /// Puts back a batch [`Lane::next_remote`] took, which could not leave
    /// yet. Nothing was collected for its lane since.
    pub(crate) fn unship(&mut self, lane: usize, batch: Vec<Remote>) {
        self.remote[lane] = batch;
    }

    fn broadcast(&self, make: &impl Fn() -> Control) {
        for (i, lane) in self.shared.lanes.iter().enumerate() {
            if i != usize::from(self.id) {
                let _ = lane.control.push(make());
            }
        }
    }

    fn subscribe_entry(&mut self, filter: &str, entry: &Entry, sync: Option<&Arc<SyncAck>>) {
        self.index.add(filter, entry.clone());
        let filter: Box<str> = filter.into();
        if let Some(s) = sync {
            s.expect(self.shared.lanes.len() - 1);
        }
        self.broadcast(&|| Control::SubAdd(filter.clone(), entry.clone(), sync.cloned()));
    }

    fn unsubscribe_entry(
        &mut self,
        filter: &str,
        sub: SubRef,
        sync: Option<&Arc<SyncAck>>,
    ) -> bool {
        let removed = self.index.remove(filter, sub);
        let filter: Box<str> = filter.into();
        if let Some(s) = sync {
            s.expect(self.shared.lanes.len() - 1);
        }
        self.broadcast(&|| Control::SubRemove(filter.clone(), sub, sync.cloned()));
        removed
    }

    fn entry_for(&self, key: Key, filter: &str, opts: SubscriptionOptions, sub_id: u32) -> Entry {
        Entry {
            sub: self.sub_ref(key),
            opts,
            sub_id: SubIds::one(sub_id),
            share: is_shared(filter).then(|| Arc::from(filter)),
            online: is_shared(filter)
                .then(|| self.sessions.get(key).map(|s| s.online.clone()))
                .flatten(),
        }
    }

    /// Ends a session for good. A will still waiting for its delay goes
    /// out now.
    fn remove_session(&mut self, key: Key) {
        if let Some(s) = self.sessions.get_mut(key) {
            s.unbind();
        }
        self.release_waiters(key);
        let Some(mut s) = self.sessions.remove(key) else {
            return;
        };
        self.stats.dropped_expired += s.expired;
        self.stats.dropped_session_ended += s.held();
        let sub = self.sub_ref(key);
        for filter in s.subs.keys() {
            self.unsubscribe_entry(filter, sub, None);
        }
        self.stats.subscriptions = self.index.len() as u64;
        if self.by_client.get(&s.client_id) == Some(&key) {
            self.by_client.remove(&s.client_id);
        }
        if let Some(will) = s.delayed_will.take() {
            self.publish_will(will.msg);
        }
    }

    fn kick(conn: &ConnShared, version: Version, code: u8) {
        let mut bye = Vec::new();
        if version.is_v5() {
            encode::reason_only(PacketType::Disconnect, version, code, &[], &mut bye);
        }
        conn.close(bye);
    }

    /// Whether `client_id` has a session on this lane.
    pub(crate) fn has_session(&self, client_id: &str) -> bool {
        self.by_client.contains_key(client_id)
    }

    /// Whether `client_id` has a live connection on this lane.
    pub(crate) fn is_connected(&self, client_id: &str) -> bool {
        self.by_client
            .get(client_id)
            .and_then(|k| self.sessions.get(*k))
            .is_some_and(|s| s.conn.is_some())
    }

    /// Binds a connection to its session, creating or resuming it, and
    /// queues CONNACK.
    pub(crate) fn attach(&mut self, c: &OwnedConnect, conn: Rc<ConnShared>) -> Attached {
        let max_expiry = self.shared.opts.max_session_expiry;
        let mut present = false;
        let mut key = self.by_client.get(c.client_id.as_str()).copied();
        if let Some(k) = key
            && let Some(s) = self.sessions.get_mut(k)
        {
            // The old session ends if the new connection starts clean or the
            // old one kept no session.
            let ends = c.clean_start || s.expiry == 0;
            // The old connection ends without DISCONNECT, so its will goes
            // out now, unless it has a delay and the session goes on: the
            // new connection came first and cancels it. So does a will of
            // an earlier connection that waits for its delay.
            let old = s.conn.take();
            let delayed = s.delayed_will.take().map(|w| w.msg);
            let will = match s.will.take() {
                Some(w) if old.is_some() && (w.delay == 0 || ends) => Some(w.msg),
                _ => delayed.filter(|_| ends),
            };
            // Packets queued for the old connection must not reach the new
            // one; the inflight window is resent below.
            s.out.clear();
            if let Some(old) = old {
                // Before its CONNACK, a connection just closes: CONNACK comes
                // before any other packet [MQTT-3.2.0-1].
                if old.connacked.get() {
                    Self::kick(&old, s.version, reason::SESSION_TAKEN_OVER);
                } else {
                    old.close(Vec::new());
                }
                self.stats.connections -= 1;
            }
            if let Some(will) = will {
                self.publish_will(will);
            }
            if ends {
                self.remove_session(k);
                key = None;
            } else {
                present = true;
            }
        }
        let key = key.unwrap_or_else(|| {
            let session = Session::new(c.client_id.as_str().into(), c.version, self.state.clone());
            let k = self.sessions.insert(session);
            self.by_client.insert(c.client_id.as_str().into(), k);
            k
        });
        self.stats.connections += 1;
        let props = self.connack_props(c);
        let Some(s) = self.sessions.get_mut(key) else {
            return Attached { key };
        };
        s.version = c.version;
        s.username = c.username.as_deref().map(Box::from);
        s.last_allowed = None;
        s.bind(conn);
        s.expiry = c.session_expiry.min(max_expiry);
        s.expires_at = None;
        s.will.clone_from(&c.will);
        s.window = c.receive_maximum.min(self.shared.opts.max_inflight).max(1);
        s.max_packet = c.max_packet_size;
        s.aliases.clear();
        connack_ok(c.version, present, &props, &mut s.out);
        if present {
            s.resend();
        }
        self.mark(key);
        Attached { key }
    }

    fn connack_props(&self, c: &OwnedConnect) -> Vec<u8> {
        let mut props = Vec::new();
        if !c.version.is_v5() {
            return props;
        }
        let o = &self.shared.opts;
        let mut list = vec![
            Property::U16(property::TOPIC_ALIAS_MAXIMUM, o.topic_alias_maximum),
            Property::U32(
                property::MAXIMUM_PACKET_SIZE,
                o.max_packet_size.min(u32::MAX as usize) as u32,
            ),
        ];
        if o.receive_maximum < u16::MAX {
            list.push(Property::U16(property::RECEIVE_MAXIMUM, o.receive_maximum));
        }
        if c.server_keep_alive {
            list.push(Property::U16(property::SERVER_KEEP_ALIVE, c.keep_alive));
        }
        if c.assigned {
            list.push(Property::Str(
                property::ASSIGNED_CLIENT_IDENTIFIER,
                &c.client_id,
            ));
        }
        if c.session_expiry > o.max_session_expiry {
            list.push(Property::U32(
                property::SESSION_EXPIRY_INTERVAL,
                o.max_session_expiry,
            ));
        }
        property::encode_all(&list, &mut props);
        props
    }

    /// Unbinds a closed connection from its session.
    pub(crate) fn detach(&mut self, key: Key, conn_id: u64, close: Close) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if !s.has_conn(conn_id) {
            return;
        }
        s.unbind();
        s.out.clear();
        s.aliases.clear();
        self.stats.connections -= 1;
        self.shared.connections.fetch_sub(1, Ordering::AcqRel);
        let send_will = !matches!(close, Close::Graceful { will: false });
        let will = s.will.take().filter(|_| send_will);
        let expiry = s.expiry;
        if expiry != 0 && expiry != u32::MAX {
            s.expires_at = Some(Instant::now() + Duration::from_secs(u64::from(expiry)));
        }
        self.release_waiters(key);
        if expiry == 0 {
            self.remove_session(key);
        }
        if let Some(w) = will {
            if w.delay == 0 || expiry == 0 {
                self.publish_will(w.msg);
            } else {
                let delay = Duration::from_secs(u64::from(w.delay.min(expiry)));
                self.schedule_will(key, w.msg, delay);
            }
        }
    }

    /// Publishes `msg` after `delay`, unless a new connection or the end of
    /// the session takes it first.
    fn schedule_will(&mut self, key: Key, msg: Msg, delay: Duration) {
        let Some(lane) = crate::broker::lane_ref(self.shared.id) else {
            return;
        };
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        self.next_will_id += 1;
        let id = self.next_will_id;
        s.delayed_will = Some(DelayedWill {
            timer: id,
            due: Instant::now() + delay,
            msg,
        });
        tokio::task::spawn_local(async move {
            tokio::time::sleep(delay).await;
            {
                let mut l = lane.borrow_mut();
                // A state dump keeps the will with its due time.
                if l.closing {
                    return;
                }
                let Some(will) = l
                    .sessions
                    .get_mut(key)
                    .and_then(|s| s.delayed_will.take_if(|w| w.timer == id))
                else {
                    return;
                };
                l.publish_will(will.msg);
                l.flush();
            }
            crate::broker::ship(&lane).await;
        });
    }

    /// Publishes a will. Its Message Expiry Interval starts now.
    fn publish_will(&mut self, mut msg: Msg) {
        msg.expires_at = crate::msg::expiry(msg.props());
        self.publish_owned(&msg);
    }

    /// Routes a message that is not from a client connection.
    pub(crate) fn publish_owned(&mut self, msg: &Msg) {
        if msg.retain {
            self.shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .set(msg, self.state_full);
        }
        let p = Publication::of(msg);
        let mut owned = Owned::of(msg.clone());
        self.route(None, &p, &mut owned);
    }

    /// Handles every packet in `chunk`, which holds only whole packets.
    pub(crate) fn handle_chunk(&mut self, key: Key, chunk: &Bytes) -> Result<(), Close> {
        let Some(s) = self.sessions.get(key) else {
            return Err(Close::Kicked);
        };
        let version = s.version;
        let max = self.shared.opts.max_packet_size;
        let mut buf: &[u8] = chunk;
        while !buf.is_empty() {
            let (packet, n) = match decode(buf, version, max) {
                Ok(Some(p)) => p,
                Ok(None) => return Err(Close::Protocol(reason::MALFORMED_PACKET)),
                Err(e) => return Err(Close::Protocol(e.reason_code())),
            };
            let raw = &buf[..n];
            buf = &buf[n..];
            self.handle_packet(key, packet, raw, chunk)?;
        }
        Ok(())
    }

    #[inline]
    fn handle_packet(
        &mut self,
        key: Key,
        packet: Packet<'_>,
        raw: &[u8],
        chunk: &Bytes,
    ) -> Result<(), Close> {
        match packet {
            Packet::Publish(p) => self.on_publish(key, &p, raw, chunk),
            Packet::PubAck(a) => self.on_ack(key, |s| s.on_puback(a.pkid)),
            Packet::PubRec(a) => self.on_ack(key, |s| s.on_pubrec(a.pkid, a.reason)),
            Packet::PubRel(a) => {
                self.on_pubrel(key, &a);
                Ok(())
            }
            Packet::PubComp(a) => self.on_ack(key, |s| s.on_pubcomp(a.pkid)),
            Packet::Subscribe(s) => {
                self.on_subscribe(key, &s);
                Ok(())
            }
            Packet::Unsubscribe(u) => {
                self.on_unsubscribe(key, &u);
                Ok(())
            }
            Packet::PingReq => {
                if let Some(s) = self.sessions.get_mut(key) {
                    encode::pingresp(&mut s.out);
                }
                self.mark(key);
                Ok(())
            }
            Packet::Disconnect(d) => {
                let s = self.sessions.get_mut(key).ok_or(Close::Kicked)?;
                if let Some(e) = d.properties.get_int(property::SESSION_EXPIRY_INTERVAL) {
                    if s.expiry == 0 && e != 0 {
                        return Err(Close::Protocol(reason::PROTOCOL_ERROR));
                    }
                    s.expiry = e.min(self.shared.opts.max_session_expiry);
                }
                Err(Close::Graceful {
                    will: d.reason == reason::DISCONNECT_WITH_WILL,
                })
            }
            _ => Err(Close::Protocol(reason::PROTOCOL_ERROR)),
        }
    }

    /// Applies an ack. `f` says whether the session has output, or the
    /// reason the ack breaks the protocol.
    fn on_ack(
        &mut self,
        key: Key,
        f: impl FnOnce(&mut Session) -> Result<bool, u8>,
    ) -> Result<(), Close> {
        let mut result = Ok(());
        if let Some(s) = self.sessions.get_mut(key) {
            match f(s) {
                Ok(true) => self.mark(key),
                Ok(false) => {}
                Err(code) => result = Err(Close::Protocol(code)),
            }
        }
        self.release_waiters(key);
        result
    }

    fn on_pubrel(&mut self, key: Key, a: &Ack<'_>) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let code = if s.awaiting_rel.remove(&a.pkid) {
            0
        } else {
            reason::PACKET_IDENTIFIER_NOT_FOUND
        };
        s.ack(PacketType::PubComp, a.pkid, code);
        self.mark(key);
    }

    #[inline]
    fn on_publish(
        &mut self,
        key: Key,
        p: &Publish<'_>,
        raw: &[u8],
        chunk: &Bytes,
    ) -> Result<(), Close> {
        let opts_alias_max = self.shared.opts.topic_alias_maximum;
        let receive_max = usize::from(self.shared.opts.receive_maximum);
        let s = self.sessions.get_mut(key).ok_or(Close::Kicked)?;
        let (aliased, filtered_props) = if s.version.is_v5() && !p.properties.is_empty() {
            // Subscription Identifiers are the broker's to set [MQTT-3.3.4-6].
            if p.properties
                .get_int(property::SUBSCRIPTION_IDENTIFIER)
                .is_some()
            {
                return Err(Close::Protocol(reason::PROTOCOL_ERROR));
            }
            resolve_alias(s, p, opts_alias_max)?
        } else {
            (None, None)
        };
        let topic = aliased.as_deref().unwrap_or(p.topic);
        if crate::msg::too_deep(topic, self.shared.opts.max_topic_levels) {
            return Err(Close::Protocol(reason::TOPIC_NAME_INVALID));
        }
        // `$SYS/` is the broker's own.
        let allowed = !topic.starts_with("$SYS/")
            && match self.shared.opts.authorizer.as_deref() {
                Some(acl) => may_publish(acl, s, topic),
                None => true,
            };
        // v3 has no reason codes: a denied publish gets a positive ack and is
        // dropped [MQTT-3.3.5-2].
        let code = if allowed || !s.version.is_v5() {
            0
        } else {
            reason::NOT_AUTHORIZED
        };
        if !admit_publish(s, p, allowed, code, receive_max)? {
            Self::mark_in(&mut self.dirty, s, key);
            return Ok(());
        }
        let packet = filtered_props.is_none().then_some((raw, s.version));
        let publication = Publication {
            topic,
            payload: p.payload,
            props: filtered_props.as_deref().unwrap_or(p.properties.raw()),
            qos: p.qos,
            retain: p.retain,
            raw: packet.filter(|_| p.qos == QoS::AtMostOnce),
            packet,
            chunk: Some(chunk),
            pins: true,
            expires_at: None,
        };
        let mut owned = Owned::default();
        if p.retain {
            let msg = publication.to_msg();
            self.shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .set(&msg, self.state_full);
            owned.msg = Some(msg);
        }
        let m = self.publisher_matches(key, topic);
        self.hit = None;
        self.route_matches(Some(key), &m, &publication, &mut owned);
        self.ack_or_hold(key, p);
        Ok(())
    }

    /// Acks a routed QoS 1/2 publish, or holds the ack while a subscriber it
    /// went to is congested.
    fn ack_or_hold(&mut self, key: Key, p: &Publish<'_>) {
        let kind = match p.qos {
            QoS::AtMostOnce => return,
            QoS::AtLeastOnce => PacketType::PubAck,
            QoS::ExactlyOnce => PacketType::PubRec,
        };
        let waiter = Waiter {
            publisher: key,
            kind,
            pkid: p.pkid,
        };
        match self.hit.take() {
            Some(Hit::Local(sub)) if sub != key && self.sessions.get(sub).is_some() => {
                if let Some(s) = self.sessions.get_mut(sub) {
                    s.waiters.push(waiter);
                }
            }
            Some(Hit::Remote(sub)) => self.remote_waiters.entry(sub).or_default().push(waiter),
            _ => {
                if let Some(s) = self.sessions.get_mut(key) {
                    s.ack(kind, p.pkid, 0);
                }
                self.mark(key);
            }
        }
    }

    /// Sends held acks once a congested session drained or went away.
    fn release_waiters(&mut self, key: Key) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let low = self.shared.opts.qos_queue_high / 2;
        let drained = s.conn.is_none() || s.queued() <= low;
        if !s.congested || !drained {
            return;
        }
        s.congested = false;
        let waiters = std::mem::take(&mut s.waiters);
        let sub = self.sub_ref(key);
        self.broadcast(&|| Control::Congested(sub, false));
        self.send_acks(&waiters);
    }

    fn send_acks(&mut self, waiters: &[Waiter]) {
        for w in waiters {
            if let Some(p) = self.sessions.get_mut(w.publisher) {
                p.ack(w.kind, w.pkid, 0);
                self.mark(w.publisher);
            }
        }
    }

    /// Matches for a publish from `key`, reusing the session's last lookup
    /// when the topic repeats and the index did not change.
    #[inline]
    fn publisher_matches(&mut self, key: Key, topic: &str) -> Rc<Matches> {
        let generation = self.index.generation();
        if let Some(session) = self.sessions.get(key)
            && let Some((last, at, matches)) = &session.last_topic
            && *at == generation
            && **last == *topic
        {
            return matches.clone();
        }
        let (matches, cached) = self.index.lookup(topic);
        if cached && let Some(session) = self.sessions.get_mut(key) {
            // Reuse the topic buffer: a publisher changing topics should not
            // allocate per message.
            let mut last = session
                .last_topic
                .take()
                .map(|(t, _, _)| t)
                .unwrap_or_default();
            last.clear();
            last.push_str(topic);
            session.last_topic = Some((last, generation, matches.clone()));
        }
        matches
    }

    /// Delivers a publication to every matching subscription.
    #[inline]
    fn route(&mut self, from: Option<Key>, p: &Publication<'_>, owned: &mut Owned) {
        let (m, _) = self.index.lookup(p.topic);
        self.route_matches(from, &m, p, owned);
    }

    /// Delivers a publication to `m`, the subscriptions matching its topic.
    #[inline]
    fn route_matches(
        &mut self,
        from: Option<Key>,
        m: &Matches,
        p: &Publication<'_>,
        owned: &mut Owned,
    ) {
        self.stats.publishes += 1;
        let mut remote_lanes = 0u64;
        let mut remote_hot = false;
        let n = m.direct.len();
        for (i, e) in m.direct.iter().enumerate() {
            if e.sub.lane == self.id {
                let last = i + 1 == n && m.groups.is_empty();
                if let Some(g) = Self::grant(from, e, p, last) {
                    self.deliver(e.sub.key, p, owned, g);
                }
            } else {
                remote_lanes |= 1 << (e.sub.lane & 63);
                if p.qos != QoS::AtMostOnce
                    && !self.remote_congested.is_empty()
                    && self.remote_congested.contains(&e.sub)
                {
                    self.hit.get_or_insert(Hit::Remote(e.sub));
                    remote_hot = true;
                }
            }
        }
        for (i, g) in m.groups.iter().enumerate() {
            let e = g.pick();
            if e.sub.lane == self.id {
                if let Some(g) = Self::grant(from, e, p, i + 1 == m.groups.len()) {
                    self.deliver(e.sub.key, p, owned, g);
                }
            } else {
                let msg = owned.msg.get_or_insert_with(|| p.to_msg()).clone();
                self.remote[usize::from(e.sub.lane)].push(Remote::Deliver {
                    key: e.sub.key,
                    item: Queued {
                        msg,
                        qos: p.qos.min(e.opts.qos),
                        retain: e.opts.retain_as_published && p.retain,
                        sub_id: e.sub_id,
                    },
                });
            }
        }
        if remote_lanes != 0 {
            let (chunk, at, version, pins) = forward_packet(p, remote_hot, &mut self.fwd);
            let origin = Origin {
                version,
                from: from.map(|k| self.sub_ref(k)),
                pins,
            };
            for (lane, batch) in self.remote.iter_mut().enumerate() {
                if remote_lanes & (1 << (lane & 63)) != 0 && lane != usize::from(self.id) {
                    push_forward(batch, &chunk, at, origin);
                }
            }
        }
    }

    /// Routes a publish forwarded by another lane to this lane's sessions.
    fn route_forwarded(&mut self, chunk: &Bytes, at: (u32, u32), origin: Origin) {
        let Origin {
            version,
            from,
            pins,
        } = origin;
        let Some(packet) = chunk.get(at.0 as usize..(at.0 + at.1) as usize) else {
            return;
        };
        let Ok(Some((Packet::Publish(p), n))) = decode(packet, version, usize::MAX) else {
            return;
        };
        let publication = Publication {
            topic: p.topic,
            payload: p.payload,
            props: p.properties.raw(),
            qos: p.qos,
            retain: p.retain,
            raw: (p.qos == QoS::AtMostOnce).then_some((&packet[..n], version)),
            packet: None,
            chunk: Some(chunk),
            pins,
            expires_at: None,
        };
        let from = from.filter(|f| f.lane == self.id).map(|f| f.key);
        let (m, _) = self.index.lookup(p.topic);
        let mut owned = Owned::default();
        let n = m.direct.len();
        for (i, e) in m.direct.iter().enumerate() {
            if e.sub.lane == self.id
                && let Some(g) = Self::grant(from, e, &publication, i + 1 == n)
            {
                self.deliver(e.sub.key, &publication, &mut owned, g);
            }
        }
    }

    /// What `e` gets of `p`, unless No Local skips its own publish.
    #[inline]
    fn grant(from: Option<Key>, e: &Entry, p: &Publication<'_>, last: bool) -> Option<Grant> {
        if e.opts.no_local && from == Some(e.sub.key) {
            return None;
        }
        Some(Grant {
            qos: p.qos.min(e.opts.qos),
            retain: e.opts.retain_as_published && p.retain,
            sub_id: e.sub_id,
            last,
            on_subscribe: false,
        })
    }

    #[inline]
    fn deliver(&mut self, key: Key, p: &Publication<'_>, owned: &mut Owned, g: Grant) {
        let Grant {
            qos,
            retain,
            sub_id,
            on_subscribe,
            ..
        } = g;
        let o = &self.shared.opts;
        let max_out = o.max_out_bytes;
        let limit = if on_subscribe {
            QueueLimit {
                count: usize::MAX,
                bytes: usize::MAX,
                state_full: self.state_full,
            }
        } else {
            QueueLimit {
                count: o.max_queued,
                bytes: o.max_queued_bytes,
                state_full: self.state_full,
            }
        };
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let outcome = if qos == QoS::AtMostOnce {
            if s.conn.is_none() {
                Outcome::Offline
            } else {
                s.deliver_qos0(p, retain, sub_id, max_out)
            }
        } else {
            let kept = keep(s, p, owned, &self.state, g);
            s.deliver_qos12(kept, limit)
        };
        if matches!(outcome, Outcome::Queued | Outcome::QueueFull(_))
            && s.conn.is_some()
            && s.queued() >= self.shared.opts.qos_queue_high
        {
            let newly = !s.congested;
            s.congested = true;
            if newly {
                let sub = self.sub_ref(key);
                self.broadcast(&|| Control::Congested(sub, true));
            }
            self.hit.get_or_insert(Hit::Local(key));
        }
        self.count(key, outcome);
    }

    #[inline]
    fn count(&mut self, key: Key, outcome: Outcome) {
        match outcome {
            Outcome::Sent => {
                self.stats.deliveries += 1;
                self.mark(key);
            }
            Outcome::Queued => {}
            Outcome::QueueFull(n) => self.stats.dropped_queue_full += u64::from(n),
            Outcome::StateFull => self.stats.dropped_state_full += 1,
            Outcome::BufferFull => self.stats.dropped_buffer_full += 1,
            Outcome::Offline => self.stats.dropped_offline += 1,
            Outcome::TooLarge => self.stats.dropped_too_large += 1,
        }
    }

    /// Delivers a message routed on another lane.
    pub(crate) fn deliver_remote(&mut self, r: &Remote) {
        match r {
            Remote::Forward {
                chunk,
                first,
                more,
                origin,
            } => {
                self.route_forwarded(chunk, *first, *origin);
                for at in more {
                    self.route_forwarded(chunk, *at, *origin);
                }
            }
            Remote::Drained(sync) => sync.done(),
            Remote::Deliver { key, item } => {
                let p = Publication::of(&item.msg);
                let mut owned = Owned::of(item.msg.clone());
                let g = Grant {
                    qos: item.qos,
                    retain: item.retain,
                    sub_id: item.sub_id,
                    last: true,
                    on_subscribe: false,
                };
                self.deliver(*key, &p, &mut owned, g);
            }
        }
    }

    fn on_subscribe(&mut self, key: Key, sub: &Subscribe<'_>) {
        let sub_id = sub
            .properties
            .get_int(property::SUBSCRIPTION_IDENTIFIER)
            .unwrap_or(0);
        let lanes = self.shared.lanes.len();
        let mut codes = Vec::new();
        let mut wanted = Vec::new();
        let sync = self.sync();
        for (filter, opts) in sub.filters() {
            let Some(s) = self.sessions.get_mut(key) else {
                return;
            };
            if let Some(code) = refused(&self.shared.opts, s, filter) {
                codes.push(code);
                continue;
            }
            let existed = s.subscribe(filter, opts, sub_id, lanes);
            let entry = self.entry_for(key, filter, opts, sub_id);
            self.subscribe_entry(filter, &entry, sync.as_ref());
            codes.push(opts.qos as u8);
            if retained_wanted(filter, opts, existed) {
                wanted.push((filter.into(), opts.qos, sub_id));
            }
        }
        self.stats.subscriptions = self.index.len() as u64;
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let a = SubAckFields {
            kind: PacketType::SubAck,
            pkid: sub.pkid,
            properties: &[],
            codes: &codes,
        };
        let version = s.version;
        self.ack_after_sync(key, sync, |out| encode::suback(&a, version, out), wanted);
    }

    /// Sends the retained messages `wanted` subscriptions match. Read only
    /// once every lane has the subscriptions: a publish on another lane then
    /// either updated the store already, or is routed to them live.
    fn send_retained(&mut self, key: Key, wanted: &[Wanted]) {
        if wanted.is_empty() {
            return;
        }
        let mut retained = Vec::new();
        {
            let store = self
                .shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for (filter, qos, sub_id) in wanted {
                retained.extend(
                    store
                        .matching(filter)
                        .into_iter()
                        .map(|m| (m, *qos, *sub_id)),
                );
            }
        }
        self.deliver_retained(key, retained);
    }

    /// Sends retained messages for new subscriptions, with RETAIN set.
    fn deliver_retained(&mut self, key: Key, retained: Vec<(Msg, QoS, u32)>) {
        for (msg, max_qos, sub_id) in retained {
            let p = Publication {
                retain: true,
                ..Publication::of(&msg)
            };
            let g = Grant {
                qos: msg.qos.min(max_qos),
                retain: true,
                sub_id: SubIds::one(sub_id),
                last: true,
                on_subscribe: true,
            };
            let mut retained = msg.clone();
            retained.retain = true;
            let mut owned = Owned::of(retained);
            self.deliver(key, &p, &mut owned, g);
        }
    }

    /// An ack to collect other lanes, if there are any.
    fn sync(&self) -> Option<Arc<SyncAck>> {
        (self.shared.lanes.len() > 1).then(Arc::default)
    }

    /// Queues an ack packet now, or holds it until `sync` completes.
    fn ack_after_sync(
        &mut self,
        key: Key,
        sync: Option<Arc<SyncAck>>,
        encode: impl FnOnce(&mut Vec<u8>),
        retained: Vec<Wanted>,
    ) {
        if let Some(sync) = sync {
            let mut packet = Vec::new();
            encode(&mut packet);
            self.held.push(Held {
                key,
                sync,
                packet,
                retained,
            });
        } else {
            if let Some(s) = self.sessions.get_mut(key) {
                encode(&mut s.out);
            }
            self.mark(key);
            self.send_retained(key, &retained);
        }
    }

    /// Ack packets waiting for other lanes.
    pub(crate) fn take_held(&mut self) -> Vec<Held> {
        std::mem::take(&mut self.held)
    }

    /// Queues an ack packet that was held for other lanes.
    pub(crate) fn release(&mut self, h: &Held) {
        if let Some(s) = self.sessions.get_mut(h.key) {
            s.out.extend_from_slice(&h.packet);
        }
        self.mark(h.key);
        self.send_retained(h.key, &h.retained);
        self.flush();
    }

    fn on_unsubscribe(&mut self, key: Key, u: &Unsubscribe<'_>) {
        let mut codes = Vec::new();
        let sub = self.sub_ref(key);
        let sync = self.sync();
        let lanes = self.shared.lanes.len();
        for filter in u.filters() {
            let existed = self
                .sessions
                .get_mut(key)
                .is_some_and(|s| s.unsubscribe(filter, lanes));
            if existed {
                self.unsubscribe_entry(filter, sub, sync.as_ref());
            }
            codes.push(if existed {
                0
            } else {
                reason::NO_SUBSCRIPTION_EXISTED
            });
        }
        self.stats.subscriptions = self.index.len() as u64;
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let a = SubAckFields {
            kind: PacketType::UnsubAck,
            pkid: u.pkid,
            properties: &[],
            codes: &codes,
        };
        let version = s.version;
        self.ack_after_sync(
            key,
            sync,
            |out| encode::suback(&a, version, out),
            Vec::new(),
        );
    }

    /// Handles a control message.
    pub(crate) fn control(&mut self, c: Control) {
        match c {
            Control::SubAdd(filter, entry, sync) => {
                self.index.add(&filter, entry);
                if let Some(s) = sync {
                    s.done();
                }
                self.stats.subscriptions = self.index.len() as u64;
            }
            Control::SubRemove(filter, sub, sync) => {
                self.index.remove(&filter, sub);
                if let Some(s) = sync {
                    s.done();
                }
                self.stats.subscriptions = self.index.len() as u64;
            }
            Control::Publish(msg) => self.publish_owned(&msg),
            Control::Congested(sub, true) => {
                self.remote_congested.insert(sub);
            }
            Control::Congested(sub, false) => {
                self.remote_congested.remove(&sub);
                if let Some(waiters) = self.remote_waiters.remove(&sub) {
                    self.send_acks(&waiters);
                }
            }
            Control::Stats(reply) => {
                let mut stats = self.stats;
                stats.sessions = self.sessions.len() as u64;
                stats.state_bytes = self.state.get() as u64;
                // Removed sessions added theirs to `self.stats`.
                stats.dropped_expired += self
                    .sessions
                    .keys()
                    .filter_map(|k| self.sessions.get(k))
                    .map(|s| s.expired)
                    .sum::<u64>();
                let _ = reply.send(stats);
            }
            Control::Quiesce(done) => {
                self.quiesce();
                let _ = done.send(());
            }
            Control::Drain(sync) => self.drain(&sync),
            Control::Dump(d) => {
                let Dump { mut part, reply } = *d;
                let written = self.dump(&mut part);
                // Let go of the file before answering, so it closes.
                drop(part);
                let _ = reply.send(written);
            }
            Control::Restore(states, sync, done) => {
                self.restore(states, &sync);
                let _ = done.send(());
            }
            Control::Shutdown(done) => {
                self.shutdown();
                if let Some(done) = done {
                    let _ = done.send(());
                }
            }
        }
        self.flush();
    }

    /// Keep alive timeouts and session expiry, for the next of
    /// [`SWEEP_PARTS`] parts of the sessions.
    pub(crate) fn sweep(&mut self, now: Instant) {
        self.check_state();
        let part = self.sweeps % SWEEP_PARTS;
        self.sweeps = self.sweeps.wrapping_add(1);
        // The retained store is shared; one lane, once a round, is enough.
        if self.id == 0 && part == 0 {
            self.shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .sweep(now);
        }
        for slot in sweep_part(part, self.sessions.slots()) {
            let Some(key) = self.sessions.key_at(slot) else {
                continue;
            };
            let Some(s) = self.sessions.get_mut(key) else {
                continue;
            };
            s.trim_buffers();
            if let Some(c) = &s.conn {
                let ka = c.keep_alive.get();
                if ka > 0
                    && now.duration_since(c.last_rx.get())
                        > Duration::from_millis(u64::from(ka) * 1500)
                {
                    Self::kick(c, s.version, reason::KEEP_ALIVE_TIMEOUT);
                }
            } else if s.expires_at.is_some_and(|t| t <= now) {
                self.remove_session(key);
            }
        }
    }

    /// Publishes this lane's state bytes, and checks `max_state_bytes`
    /// against those of all lanes and the retained store.
    fn check_state(&mut self) {
        let max = self.shared.opts.max_state_bytes;
        if max == usize::MAX {
            return;
        }
        self.shared.lanes[usize::from(self.id)]
            .state
            .store(self.state.get(), Ordering::Relaxed);
        let retained = self
            .shared
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .bytes();
        let lanes: usize = self
            .shared
            .lanes
            .iter()
            .map(|l| l.state.load(Ordering::Relaxed))
            .sum();
        self.state_full = lanes + retained >= max;
    }

    /// Closes every connection as lost, so wills and session expiry run
    /// as usual, and refuses new ones.
    fn quiesce(&mut self) {
        self.closing = true;
        let connected: Vec<(Key, Rc<ConnShared>)> = self
            .sessions
            .keys()
            .filter_map(|k| Some((k, self.sessions.get(k)?.conn.clone()?)))
            .collect();
        for (key, conn) in connected {
            if let Some(s) = self.sessions.get(key) {
                Self::kick(&conn, s.version, reason::SERVER_SHUTTING_DOWN);
            }
            self.detach(key, conn.id, Close::Kicked);
        }
    }

    /// Queues a marker for every other lane behind what waits for it.
    fn drain(&mut self, sync: &Arc<SyncAck>) {
        for (lane, batch) in self.remote.iter_mut().enumerate() {
            if lane != usize::from(self.id) {
                batch.push(Remote::Drained(sync.clone()));
            }
        }
    }

    /// Writes the sessions that outlive their connection.
    fn dump(&self, part: &mut Part) -> io::Result<()> {
        for key in self.sessions.keys() {
            if let Some(s) = self.sessions.get(key)
                && s.expiry != 0
            {
                part.session(s)?;
            }
        }
        part.flush()
    }

    /// Adds sessions from a state file, with their subscriptions on every
    /// lane and their delayed wills timed.
    fn restore(&mut self, states: Vec<SessionState>, sync: &Arc<SyncAck>) {
        let lanes = self.shared.lanes.len();
        let now = Instant::now();
        for st in states {
            if self.by_client.contains_key(&st.client_id) {
                continue;
            }
            let (session, will) = Session::restore(st, self.state.clone(), lanes);
            let subs: Vec<(Box<str>, SubscriptionOptions, u32)> = session
                .subs
                .iter()
                .map(|(f, (opts, id))| (f.clone(), *opts, *id))
                .collect();
            let client_id = session.client_id.clone();
            let key = self.sessions.insert(session);
            self.by_client.insert(client_id, key);
            for (filter, opts, sub_id) in subs {
                let entry = self.entry_for(key, &filter, opts, sub_id);
                self.subscribe_entry(&filter, &entry, Some(sync));
            }
            if let Some((due, msg)) = will {
                self.schedule_will(key, msg, due.saturating_duration_since(now));
            }
        }
        self.stats.subscriptions = self.index.len() as u64;
    }

    fn shutdown(&mut self) {
        self.shut_down = true;
        let keys: Vec<Key> = self.sessions.keys().collect();
        for key in keys {
            if let Some(s) = self.sessions.get_mut(key)
                && let Some(c) = s.conn.take()
            {
                Self::kick(&c, s.version, reason::SERVER_SHUTTING_DOWN);
            }
        }
    }
}

/// Queues a CONNACK accepting a connection.
fn connack_ok(version: Version, present: bool, props: &[u8], out: &mut Vec<u8>) {
    let ack = encode::ConnAckFields {
        session_present: present,
        code: reason::SUCCESS,
        properties: props,
    };
    encode::connack(&ack, version, out);
}

/// Applies a v5 Topic Alias. Returns the aliased topic, if the publish had
/// none, and the properties without the alias.
fn resolve_alias(
    s: &mut Session,
    p: &Publish<'_>,
    max: u16,
) -> Result<(Option<ByteString>, Option<Vec<u8>>), Close> {
    let Some(alias) = p.properties.get_int(property::TOPIC_ALIAS) else {
        return Ok((None, None));
    };
    let alias = alias as usize;
    if alias == 0 || alias > usize::from(max) {
        return Err(Close::Protocol(reason::TOPIC_ALIAS_INVALID));
    }
    let mut aliased = None;
    if p.topic.is_empty() {
        let topic = s.aliases.get(alias).cloned().flatten();
        aliased = Some(topic.ok_or(Close::Protocol(reason::PROTOCOL_ERROR))?);
    } else {
        if s.aliases.len() <= alias {
            s.aliases.resize(alias + 1, None);
        }
        s.aliases[alias] = Some(ByteString::from(p.topic));
    }
    let mut props = Vec::new();
    for prop in p
        .properties
        .iter()
        .filter(|x| x.id() != property::TOPIC_ALIAS)
    {
        prop.encode(&mut props);
    }
    Ok((aliased, Some(props)))
}

/// The publication as one PUBLISH packet another lane can decode, as a
/// buffer, the packet's offset and length in it, and whether the buffer is
/// the publisher's read buffer. That is the publisher's own bytes, unless a
/// congested subscriber there (`hot`) will hold the message long: then a
/// copy in `arena`, so the copy is made here and not on that busy lane. A
/// packet that must change becomes a v5 encoding in `arena`.
fn forward_packet(
    p: &Publication<'_>,
    hot: bool,
    arena: &mut Arena,
) -> (Bytes, (u32, u32), Version, bool) {
    if let (Some((raw, version)), Some(chunk)) = (p.packet, p.chunk)
        && crate::msg::within(chunk, raw)
    {
        if !hot {
            let at = raw.as_ptr() as usize - chunk.as_ptr() as usize;
            return (chunk.clone(), (at as u32, raw.len() as u32), version, true);
        }
        let copy = arena.put(raw.len(), |b| b.extend_from_slice(raw));
        return (copy, (0, raw.len() as u32), version, false);
    }
    let h = encode::PublishHeader {
        version: Version::V5,
        dup: false,
        qos: p.qos,
        retain: p.retain,
        topic: p.topic,
        pkid: u16::from(p.qos != QoS::AtMostOnce),
        properties: p.props,
    };
    let len = encode::packet_len(h.remaining(p.payload.len()));
    let out = arena.put(len, |b| encode::publish(&h, p.payload, &mut Block(b)));
    let at = (0, out.len() as u32);
    (out, at, Version::V5, false)
}

/// What session `s` keeps of `p` at QoS 1/2: the message, copied if it would
/// pin a read buffer long, and a share of its bytes when more sessions of
/// the lane may get it.
fn keep(
    s: &mut Session,
    p: &Publication<'_>,
    owned: &mut Owned,
    total: &LaneTotal,
    g: Grant,
) -> Kept {
    let msg = owned.msg.get_or_insert_with(|| p.to_msg());
    // A slow or offline session may hold a message long; do not let it pin
    // read buffers.
    if s.holds_long() && p.pins && p.chunk.is_some_and(|c| msg.pins(c)) {
        *msg = s.arena.copy(msg);
    }
    let share = if g.last && owned.share.is_none() {
        None
    } else {
        let share = owned
            .share
            .get_or_insert_with(|| Rc::new(Charge::new(total.clone(), budget::shared(msg))));
        Some(share.clone())
    };
    let msg = if g.last {
        owned.msg.take()
    } else {
        owned.msg.clone()
    };
    let item = Queued {
        msg: msg.unwrap_or_else(|| p.to_msg()),
        qos: g.qos,
        retain: g.retain,
        sub_id: g.sub_id,
    };
    Kept { item, share }
}

/// Appends a forwarded packet, sharing the last batch entry when it uses
/// the same buffer.
fn push_forward(batch: &mut Vec<Remote>, chunk: &Bytes, at: (u32, u32), origin: Origin) {
    if let Some(Remote::Forward {
        chunk: c,
        more,
        origin: o,
        ..
    }) = batch.last_mut()
        && c.as_ptr() == chunk.as_ptr()
        && c.len() == chunk.len()
        && *o == origin
    {
        more.push(at);
        return;
    }
    // Packets of one read are mostly alike, so size the rest from this one;
    // a copy made for one packet gets no vector.
    let more = (chunk.len() / (at.1 as usize).max(1)).saturating_sub(1);
    batch.push(Remote::Forward {
        chunk: chunk.clone(),
        first: at,
        more: Vec::with_capacity(more),
        origin,
    });
}

/// Asks the authorizer about a publish, reusing the answer for a repeated
/// topic.
#[inline]
fn may_publish(acl: &dyn Authorizer, s: &mut Session, topic: &str) -> bool {
    if let Some((last, allowed)) = &s.last_allowed
        && **last == *topic
    {
        return *allowed;
    }
    let client = ClientInfo {
        client_id: &s.client_id,
        username: s.username.as_deref(),
    };
    let allowed = acl.may_publish(&client, topic);
    s.last_allowed = Some((topic.into(), allowed));
    allowed
}

/// Checks an inbound PUBLISH before routing. Answers rejected publishes, with
/// `code`, and repeated QoS 2 packet IDs right away and returns false for
/// them. Routed publishes are acked after routing.
#[inline]
fn admit_publish(
    s: &mut Session,
    p: &Publish<'_>,
    allowed: bool,
    code: u8,
    receive_max: usize,
) -> Result<bool, Close> {
    match p.qos {
        QoS::AtMostOnce => Ok(allowed),
        QoS::AtLeastOnce if !allowed => {
            s.ack(PacketType::PubAck, p.pkid, code);
            Ok(false)
        }
        QoS::AtLeastOnce => Ok(true),
        QoS::ExactlyOnce if !allowed => {
            s.ack(PacketType::PubRec, p.pkid, code);
            Ok(false)
        }
        QoS::ExactlyOnce => {
            let fresh = s.awaiting_rel.insert(p.pkid);
            if fresh && s.awaiting_rel.len() > receive_max {
                return Err(Close::Protocol(reason::RECEIVE_MAXIMUM_EXCEEDED));
            }
            if !fresh {
                s.ack(PacketType::PubRec, p.pkid, 0);
            }
            Ok(fresh)
        }
    }
}

/// The SUBACK code refusing `filter` for `s`, if it is refused.
fn refused(o: &crate::options::BrokerOptions, s: &Session, filter: &str) -> Option<u8> {
    let code = if crate::msg::too_deep(filter, o.max_topic_levels) {
        reason::TOPIC_FILTER_INVALID
    } else if !may_subscribe(o.authorizer.as_deref(), s, filter) {
        reason::NOT_AUTHORIZED
    } else if s.subs.len() >= o.max_subscriptions && !s.subs.contains_key(filter) {
        reason::QUOTA_EXCEEDED
    } else {
        return None;
    };
    Some(if s.version.is_v5() {
        code
    } else {
        reason::UNSPECIFIED_ERROR
    })
}

fn may_subscribe(acl: Option<&dyn Authorizer>, s: &Session, filter: &str) -> bool {
    acl.is_none_or(|acl| {
        let client = ClientInfo {
            client_id: &s.client_id,
            username: s.username.as_deref(),
        };
        match SharedFilter::try_from(filter) {
            Ok(shared) => acl.may_subscribe_shared(&client, shared.group, shared.filter),
            Err(_) => acl.may_subscribe(&client, filter),
        }
    })
}

/// Whether a subscription gets the retained messages it matches, by
/// Retain Handling. Shared subscriptions never do.
fn retained_wanted(filter: &str, opts: SubscriptionOptions, existed: bool) -> bool {
    !is_shared(filter) && (opts.retain_handling == 0 || (opts.retain_handling == 1 && !existed))
}

/// The session slots that sweep `part` of a round covers: its share of
/// `slots`, so each slot is swept once a round, however few there are.
fn sweep_part(part: u32, slots: u32) -> Range<u32> {
    let step = slots.div_ceil(SWEEP_PARTS);
    let from = part.saturating_mul(step).min(slots);
    from..from.saturating_add(step).min(slots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_parts_cover_each_slot_once_a_round() {
        for slots in [0, 1, 2, 19, 20, 21, 41, 1000, 20_001] {
            let mut swept = vec![0u8; slots as usize];
            for part in 0..SWEEP_PARTS {
                for slot in sweep_part(part, slots) {
                    swept[slot as usize] += 1;
                }
            }
            assert!(swept.iter().all(|&n| n == 1), "{slots} slots");
        }
    }
}

//! Per-client session state and outgoing QoS flows.

use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Instant;

use bytestring::ByteString;
use rustc_hash::{FxHashMap, FxHashSet};
use zzq_proto::encode::{self, AckFields, PublishHeader};
use zzq_proto::property::{MESSAGE_EXPIRY_INTERVAL, Property, SUBSCRIPTION_IDENTIFIER};
use zzq_proto::{PacketType, QoS, SubscriptionOptions, Version};

use crate::conn::ConnShared;
use crate::index::{Matches, SubIds};
use crate::msg::{Arena, Msg, Publication, expired, same_family};
use crate::slab::Key;

/// Write buffers above this size are freed once their session goes idle.
const IDLE_BUFFER: usize = 64 << 10;
/// Unacked messages beyond which a session counts as slow: new messages
/// for it get their own copy instead of pinning read buffers.
const SLOW_INFLIGHT: usize = 256;

/// Will message of a connection.
#[derive(Debug, Clone)]
pub(crate) struct Will {
    pub(crate) msg: Msg,
    /// Will Delay Interval, seconds.
    pub(crate) delay: u32,
}

/// A message waiting for the inflight window.
#[derive(Debug, Clone)]
pub(crate) struct Queued {
    pub(crate) msg: Msg,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
    pub(crate) sub_id: SubIds,
}

#[derive(Debug)]
struct Inflight {
    pkid: u16,
    item: Queued,
    /// PUBREC received, PUBREL sent, waiting for PUBCOMP.
    released: bool,
}

/// A publisher ack held back while a subscriber is congested.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Waiter {
    pub(crate) publisher: Key,
    pub(crate) kind: PacketType,
    pub(crate) pkid: u16,
}

/// What happened to one delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Sent,
    Queued,
    /// The queue was full; the oldest queued message was dropped.
    QueueFull,
    /// The connection's write buffer was over its limit.
    BufferFull,
    /// The session is offline and the message is QoS 0.
    Offline,
    /// Larger than the client's Maximum Packet Size.
    TooLarge,
}

#[derive(Debug)]
pub(crate) struct Session {
    pub(crate) client_id: Box<str>,
    pub(crate) username: Option<Box<str>>,
    pub(crate) version: Version,
    pub(crate) conn: Option<Rc<ConnShared>>,
    /// Filter (with any `$share/` prefix) to options and Subscription
    /// Identifier.
    pub(crate) subs: FxHashMap<Box<str>, (SubscriptionOptions, u32)>,
    /// Session Expiry Interval, seconds. `u32::MAX` never expires.
    pub(crate) expiry: u32,
    pub(crate) expires_at: Option<Instant>,
    pub(crate) will: Option<Will>,
    /// Will of a lost connection waiting for its delay, and the ID of its
    /// timer.
    pub(crate) delayed_will: Option<(u64, Msg)>,
    /// Encoded packets waiting for the writer.
    pub(crate) out: Vec<u8>,
    /// The writer's second buffer, swapped with `out` on each write.
    pub(crate) spare: Vec<u8>,
    /// The writer took `out` since the last sweep.
    wrote: bool,
    pub(crate) dirty: bool,
    /// Inflight window towards the client.
    pub(crate) window: u16,
    /// Client's Maximum Packet Size.
    pub(crate) max_packet: usize,
    /// Inbound QoS 2 packet IDs waiting for PUBREL.
    pub(crate) awaiting_rel: FxHashSet<u16>,
    /// Inbound topic aliases of the current connection.
    pub(crate) aliases: Vec<Option<ByteString>>,
    /// Matches of the last topic this client published to, and the index
    /// generation they belong to.
    pub(crate) last_topic: Option<(String, u64, Rc<Matches>)>,
    /// Last topic the authorizer was asked about, and its answer.
    pub(crate) last_allowed: Option<(Box<str>, bool)>,
    inflight: VecDeque<Inflight>,
    queue: VecDeque<Queued>,
    next_pkid: u16,
    /// Queued messages dropped because they expired.
    pub(crate) expired: u64,
    /// Over the queue high watermark while connected.
    pub(crate) congested: bool,
    /// Publisher acks held until this session drains.
    pub(crate) waiters: Vec<Waiter>,
    /// Packet IDs in use. IDs go out in order, so the inflight window holds
    /// them in order and the next ID is free unless it wraps around to the
    /// oldest. Only then is this built, until the window empties.
    in_use: Option<Box<[u64; 1024]>>,
    /// Copies of messages held long.
    pub(crate) arena: Arena,
}

impl Session {
    pub(crate) fn new(client_id: Box<str>, version: Version) -> Session {
        Session {
            client_id,
            username: None,
            version,
            conn: None,
            subs: FxHashMap::default(),
            expiry: 0,
            expires_at: None,
            will: None,
            delayed_will: None,
            out: Vec::new(),
            spare: Vec::new(),
            wrote: false,
            dirty: false,
            window: u16::MAX,
            max_packet: usize::MAX,
            awaiting_rel: FxHashSet::default(),
            aliases: Vec::new(),
            last_topic: None,
            last_allowed: None,
            inflight: VecDeque::new(),
            queue: VecDeque::new(),
            next_pkid: 0,
            expired: 0,
            congested: false,
            waiters: Vec::new(),
            in_use: None,
            arena: Arena::default(),
        }
    }

    /// QoS 1/2 messages waiting for the inflight window.
    #[inline]
    pub(crate) fn queued(&self) -> usize {
        self.queue.len()
    }

    /// Hands the writer the encoded packets, in exchange for its spare
    /// buffer.
    pub(crate) fn take_out(&mut self) -> Vec<u8> {
        self.wrote = true;
        let spare = std::mem::take(&mut self.spare);
        std::mem::replace(&mut self.out, spare)
    }

    /// Frees large write buffers that are empty, unless the writer used them
    /// since the last call. The lane calls this once a second, so idle
    /// sessions do not keep a past burst's memory, and busy ones do not
    /// grow theirs again.
    pub(crate) fn trim_buffers(&mut self) {
        if !std::mem::take(&mut self.wrote) {
            if self.out.is_empty() && self.out.capacity() > IDLE_BUFFER {
                self.out = Vec::new();
            }
            if self.spare.capacity() > IDLE_BUFFER {
                self.spare = Vec::new();
            }
        }
        if self.inflight.is_empty() {
            self.in_use = None;
        }
    }

    /// Whether `conn_id` is the current connection.
    #[inline]
    pub(crate) fn has_conn(&self, conn_id: u64) -> bool {
        self.conn.as_ref().is_some_and(|c| c.id == conn_id)
    }

    /// Delivers a QoS 0 message to a connected session.
    #[inline]
    pub(crate) fn deliver_qos0(
        &mut self,
        p: &Publication<'_>,
        retain: bool,
        sub_id: SubIds,
        max_out: usize,
    ) -> Outcome {
        if self.out.len() >= max_out {
            return Outcome::BufferFull;
        }
        if let Some((raw, version)) = p.raw
            && sub_id.is_empty()
            && same_family(version, self.version)
            && raw.len() <= self.max_packet
        {
            self.out.push((raw[0] & !0x01) | u8::from(retain));
            self.out.extend_from_slice(&raw[1..]);
            return Outcome::Sent;
        }
        let item = Fields {
            topic: p.topic,
            payload: p.payload,
            props: p.props,
            qos: QoS::AtMostOnce,
            retain,
            dup: false,
            pkid: 0,
            sub_id,
            expires_at: p.expires_at,
        };
        if self.encode(&item) {
            Outcome::Sent
        } else {
            Outcome::TooLarge
        }
    }

    /// Delivers a QoS 1 or 2 message: sends it if the window allows, queues
    /// it otherwise.
    pub(crate) fn deliver_qos12(&mut self, item: Queued, max_queued: usize) -> Outcome {
        if !self.would_queue() {
            return self.send_inflight(item);
        }
        let mut outcome = Outcome::Queued;
        if self.queue.len() >= max_queued {
            self.queue.pop_front();
            outcome = Outcome::QueueFull;
        }
        self.queue.push_back(item);
        outcome
    }

    /// Whether a QoS 1/2 delivery now would wait in the queue.
    #[inline]
    pub(crate) fn would_queue(&self) -> bool {
        self.conn.is_none()
            || !self.queue.is_empty()
            || self.inflight.len() >= usize::from(self.window)
    }

    /// Whether a QoS 1/2 delivery now may stay a while: queued, or behind
    /// many unacked messages.
    #[inline]
    pub(crate) fn holds_long(&self) -> bool {
        self.would_queue() || self.inflight.len() >= SLOW_INFLIGHT
    }

    fn send_inflight(&mut self, item: Queued) -> Outcome {
        let pkid = self.alloc_pkid();
        let ok = self.encode(&Fields::of(&item, pkid, false));
        if !ok {
            self.free_pkid(pkid);
            return Outcome::TooLarge;
        }
        self.inflight.push_back(Inflight {
            pkid,
            item,
            released: false,
        });
        Outcome::Sent
    }

    fn alloc_pkid(&mut self) -> u16 {
        if self.in_use.is_none() {
            let id = self.next_pkid.wrapping_add(1).max(1);
            if self.inflight.front().is_none_or(|f| f.pkid != id) {
                self.next_pkid = id;
                return id;
            }
        }
        let inflight = &self.inflight;
        let bits = self.in_use.get_or_insert_with(|| {
            let mut bits = Box::new([0; 1024]);
            for f in inflight {
                bits[usize::from(f.pkid >> 6)] |= 1u64 << (f.pkid & 63);
            }
            bits
        });
        loop {
            self.next_pkid = self.next_pkid.wrapping_add(1).max(1);
            let id = self.next_pkid;
            let (word, bit) = (usize::from(id >> 6), 1u64 << (id & 63));
            if bits[word] & bit == 0 {
                bits[word] |= bit;
                return id;
            }
        }
    }

    fn free_pkid(&mut self, id: u16) {
        if let Some(bits) = &mut self.in_use {
            bits[usize::from(id >> 6)] &= !(1u64 << (id & 63));
        }
    }

    /// Moves queued messages into the window. Returns whether anything was
    /// written.
    pub(crate) fn pump(&mut self) -> bool {
        let mut wrote = false;
        let mut now = None;
        while self.conn.is_some() && self.inflight.len() < usize::from(self.window) {
            let Some(item) = self.queue.pop_front() else {
                break;
            };
            if item.msg.expires_at.is_some()
                && expired(item.msg.expires_at, *now.get_or_insert_with(Instant::now))
            {
                self.expired += 1;
                continue;
            }
            wrote |= self.send_inflight(item) == Outcome::Sent;
        }
        wrote
    }

    fn take_inflight(&mut self, pkid: u16, released: bool) -> Option<Inflight> {
        let i = self
            .inflight
            .iter()
            .position(|f| f.pkid == pkid && f.released == released)?;
        let f = self.inflight.remove(i)?;
        self.free_pkid(pkid);
        Some(f)
    }

    /// QoS of the delivery in flight under `pkid`, and whether PUBREC
    /// released it.
    fn inflight_state(&self, pkid: u16) -> Option<(QoS, bool)> {
        self.inflight
            .iter()
            .find(|f| f.pkid == pkid)
            .map(|f| (f.item.qos, f.released))
    }

    /// PUBACK from the client. `Err` with a reason code if it acks a QoS 2
    /// delivery.
    pub(crate) fn on_puback(&mut self, pkid: u16) -> Result<bool, u8> {
        match self.inflight_state(pkid) {
            None => Ok(false),
            Some((QoS::AtLeastOnce, _)) => {
                self.take_inflight(pkid, false);
                Ok(self.pump())
            }
            Some(_) => Err(zzq_proto::reason::PROTOCOL_ERROR),
        }
    }

    /// PUBREC from the client. Answers with PUBREL. `Err` with a reason
    /// code if it acks a QoS 1 delivery.
    pub(crate) fn on_pubrec(&mut self, pkid: u16, reason: u8) -> Result<bool, u8> {
        let Some(f) = self.inflight.iter_mut().find(|f| f.pkid == pkid) else {
            self.ack(
                PacketType::PubRel,
                pkid,
                zzq_proto::reason::PACKET_IDENTIFIER_NOT_FOUND,
            );
            return Ok(true);
        };
        if f.item.qos != QoS::ExactlyOnce {
            return Err(zzq_proto::reason::PROTOCOL_ERROR);
        }
        if reason >= 0x80 {
            self.take_inflight(pkid, false);
            return Ok(self.pump());
        }
        f.released = true;
        self.ack(PacketType::PubRel, pkid, 0);
        Ok(true)
    }

    /// PUBCOMP from the client. `Err` with a reason code if it comes before
    /// PUBREC, or for a QoS 1 delivery.
    pub(crate) fn on_pubcomp(&mut self, pkid: u16) -> Result<bool, u8> {
        match self.inflight_state(pkid) {
            None => Ok(false),
            Some((_, true)) => {
                self.take_inflight(pkid, true);
                Ok(self.pump())
            }
            Some(_) => Err(zzq_proto::reason::PROTOCOL_ERROR),
        }
    }

    /// Resends the inflight window after a reconnect.
    pub(crate) fn resend(&mut self) {
        let inflight = std::mem::take(&mut self.inflight);
        for f in &inflight {
            if f.released {
                self.ack(PacketType::PubRel, f.pkid, 0);
            } else {
                self.encode(&Fields::of(&f.item, f.pkid, true));
            }
        }
        self.inflight = inflight;
        self.pump();
    }

    /// Writes an ack.
    pub(crate) fn ack(&mut self, kind: PacketType, pkid: u16, reason: u8) {
        let a = AckFields {
            reason,
            ..AckFields::new(kind, pkid)
        };
        encode::ack(&a, self.version, &mut self.out);
    }

    fn encode(&mut self, f: &Fields<'_>) -> bool {
        let mut props_buf = Vec::new();
        let props: &[u8] =
            if self.version.is_v5() && (!f.sub_id.is_empty() || f.expires_at.is_some()) {
                if let Some(at) = f.expires_at {
                    // Forward the time left, not the time the publisher asked for.
                    let left = at
                        .saturating_duration_since(Instant::now())
                        .as_secs()
                        .max(1);
                    let parsed = zzq_proto::Properties::try_from(f.props).unwrap_or_default();
                    for p in parsed.iter().filter(|p| p.id() != MESSAGE_EXPIRY_INTERVAL) {
                        p.encode(&mut props_buf);
                    }
                    Property::U32(
                        MESSAGE_EXPIRY_INTERVAL,
                        u32::try_from(left).unwrap_or(u32::MAX),
                    )
                    .encode(&mut props_buf);
                } else {
                    props_buf.extend_from_slice(f.props);
                }
                for id in f.sub_id.iter() {
                    Property::VarInt(SUBSCRIPTION_IDENTIFIER, id).encode(&mut props_buf);
                }
                &props_buf
            } else {
                f.props
            };
        let h = PublishHeader {
            version: self.version,
            dup: f.dup,
            qos: f.qos,
            retain: f.retain,
            topic: f.topic,
            pkid: f.pkid,
            properties: props,
        };
        if encode::packet_len(h.remaining(f.payload.len())) > self.max_packet {
            return false;
        }
        encode::publish(&h, f.payload, &mut self.out);
        true
    }
}

struct Fields<'a> {
    topic: &'a str,
    payload: &'a [u8],
    props: &'a [u8],
    qos: QoS,
    retain: bool,
    dup: bool,
    pkid: u16,
    sub_id: SubIds,
    expires_at: Option<Instant>,
}

impl<'a> Fields<'a> {
    fn of(item: &'a Queued, pkid: u16, dup: bool) -> Fields<'a> {
        Fields {
            topic: item.msg.topic(),
            payload: item.msg.payload(),
            props: item.msg.props(),
            qos: item.qos,
            retain: item.retain,
            dup,
            pkid,
            sub_id: item.sub_id,
            expires_at: item.msg.expires_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item() -> Queued {
        Queued {
            msg: Msg::new("t", b"x", &[], QoS::AtLeastOnce, false),
            qos: QoS::AtLeastOnce,
            retain: false,
            sub_id: SubIds::default(),
        }
    }

    fn ids(s: &Session) -> Vec<u16> {
        s.inflight.iter().map(|f| f.pkid).collect()
    }

    /// One message stays unacked while others pass by more than all IDs:
    /// IDs stay unique, and in order again once the window empties.
    #[test]
    fn pkids_skip_unacked_after_wrapping() {
        let mut s = Session::new("c".into(), Version::V5);
        s.send_inflight(item());
        let stuck = s.inflight[0].pkid;
        for n in 0..140_000 {
            s.send_inflight(item());
            s.send_inflight(item());
            let mut v = ids(&s);
            v.sort_unstable();
            v.dedup();
            assert_eq!(v.len(), s.inflight.len(), "after {n}");
            // Ack the older of the two new ones, out of order.
            assert_eq!(s.on_puback(s.inflight[1].pkid), Ok(false));
            assert_eq!(s.on_puback(s.inflight[1].pkid), Ok(false));
        }
        assert!(s.in_use.is_some());
        assert_eq!(s.on_puback(stuck), Ok(false));
        s.trim_buffers();
        assert!(s.in_use.is_none());
        s.send_inflight(item());
        s.send_inflight(item());
        let v = ids(&s);
        assert_eq!(v[1], v[0].wrapping_add(1).max(1));
    }

    /// A large write buffer outlives a sweep after a write, and is freed by
    /// the next sweep without one.
    #[test]
    fn trim_frees_buffers_of_idle_sessions_only() {
        let mut s = Session::new("c".into(), Version::V5);
        s.out.reserve(2 * IDLE_BUFFER);
        let mut buf = s.take_out();
        buf.clear();
        s.spare = buf;
        s.trim_buffers();
        assert!(s.spare.capacity() > IDLE_BUFFER);
        s.trim_buffers();
        assert_eq!(s.spare.capacity(), 0);
    }
}

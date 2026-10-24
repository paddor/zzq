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
use crate::msg::{Msg, Publication, expired, same_family};
use crate::slab::Key;

/// Empty write buffers above this size are freed by the lane sweep.
const IDLE_BUFFER: usize = 64 << 10;

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
    /// Packet IDs in use, allocated on first QoS 1/2 delivery.
    in_use: Option<Box<[u64; 1024]>>,
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
        }
    }

    /// QoS 1/2 messages waiting for the inflight window.
    #[inline]
    pub(crate) fn queued(&self) -> usize {
        self.queue.len()
    }

    /// Frees large write buffers that are empty. The lane calls this once a
    /// second, so idle sessions do not keep a past burst's memory.
    pub(crate) fn trim_buffers(&mut self) {
        if self.out.is_empty() && self.out.capacity() > IDLE_BUFFER {
            self.out = Vec::new();
        }
        if self.spare.capacity() > IDLE_BUFFER {
            self.spare = Vec::new();
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
        if self.conn.is_some()
            && self.queue.is_empty()
            && self.inflight.len() < usize::from(self.window)
        {
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
        let bits = self.in_use.get_or_insert_with(|| Box::new([0; 1024]));
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

    /// PUBACK from the client.
    pub(crate) fn on_puback(&mut self, pkid: u16) -> bool {
        if self
            .take_inflight(pkid, false)
            .is_some_and(|f| f.item.qos == QoS::AtLeastOnce)
        {
            return self.pump();
        }
        false
    }

    /// PUBREC from the client. Answers with PUBREL.
    pub(crate) fn on_pubrec(&mut self, pkid: u16, reason: u8) -> bool {
        let Some(f) = self.inflight.iter_mut().find(|f| f.pkid == pkid) else {
            self.ack(
                PacketType::PubRel,
                pkid,
                zzq_proto::reason::PACKET_IDENTIFIER_NOT_FOUND,
            );
            return true;
        };
        if reason >= 0x80 {
            self.take_inflight(pkid, false);
            return self.pump();
        }
        f.released = true;
        self.ack(PacketType::PubRel, pkid, 0);
        true
    }

    /// PUBCOMP from the client.
    pub(crate) fn on_pubcomp(&mut self, pkid: u16) -> bool {
        self.take_inflight(pkid, true).is_some() && self.pump()
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
                    let parsed = zzq_proto::Properties::parse(f.props).unwrap_or_default();
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

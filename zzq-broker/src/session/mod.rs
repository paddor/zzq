//! Per-client session state.

use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use bytestring::ByteString;
use rustc_hash::{FxHashMap, FxHashSet};
use zzq_proto::{PacketType, QoS, SubscriptionOptions, Version};

use crate::budget::{self, Charge, LaneTotal};
use crate::conn::ConnShared;
use crate::index::{Matches, SubIds};
use crate::msg::{Arena, Msg};
use crate::slab::Key;
use crate::state::SessionState;

mod outbound;
mod output;

/// Write buffers above this size are freed once their session goes idle.
const IDLE_BUFFER: usize = 64 << 10;

/// Will message of a connection.
#[derive(Debug, Clone)]
pub(crate) struct Will {
    pub(crate) msg: Msg,
    /// Will Delay Interval, seconds.
    pub(crate) delay: u32,
}

/// The will of a lost connection, waiting for its delay.
#[derive(Debug)]
pub(crate) struct DelayedWill {
    /// ID of the timer that publishes it.
    pub(crate) timer: u64,
    pub(crate) due: Instant,
    pub(crate) msg: Msg,
}

/// A message waiting for the inflight window.
#[derive(Debug, Clone)]
pub(crate) struct Queued {
    pub(crate) msg: Msg,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
    pub(crate) sub_id: SubIds,
}

/// A QoS 1/2 message a session keeps. With `share`, other sessions of the
/// lane hold the same message, and the share counts its bytes once for all.
#[derive(Debug)]
pub(crate) struct Kept {
    pub(crate) item: Queued,
    pub(crate) share: Option<Rc<Charge>>,
}

impl Kept {
    /// A message no other session shares.
    pub(crate) const fn own(item: Queued) -> Kept {
        Kept { item, share: None }
    }
}

#[derive(Debug)]
struct Inflight {
    pkid: u16,
    held: Kept,
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
    /// The queue was full: this many messages were dropped, the oldest
    /// queued ones, or the new one if it alone is over the byte limit.
    QueueFull(u32),
    /// The state budget was full, so the message was dropped instead of
    /// queued.
    StateFull,
    /// The connection's write buffer was over its limit.
    BufferFull,
    /// The session is offline and the message is QoS 0.
    Offline,
    /// Larger than the client's Maximum Packet Size.
    TooLarge,
}

/// What a session may queue beyond its inflight window.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QueueLimit {
    /// Messages.
    pub(crate) count: usize,
    /// Bytes of topics, properties and payloads.
    pub(crate) bytes: usize,
    /// The state budget is used up: nothing more may be queued.
    pub(crate) state_full: bool,
}

#[derive(Debug)]
pub(crate) struct Session {
    pub(crate) client_id: Box<str>,
    pub(crate) username: Option<Box<str>>,
    pub(crate) version: Version,
    pub(crate) conn: Option<Rc<ConnShared>>,
    /// A connection is attached. Shared subscription entries on every lane
    /// read it to prefer connected members.
    pub(crate) online: Arc<AtomicBool>,
    /// Filter (with any `$share/` prefix) to options and Subscription
    /// Identifier.
    pub(crate) subs: FxHashMap<Box<str>, (SubscriptionOptions, u32)>,
    /// Session Expiry Interval, seconds. `u32::MAX` never expires.
    pub(crate) expiry: u32,
    pub(crate) expires_at: Option<Instant>,
    pub(crate) will: Option<Will>,
    pub(crate) delayed_will: Option<DelayedWill>,
    /// Client's Maximum Packet Size.
    pub(crate) max_packet: usize,
    pub(crate) out: Output,
    pub(crate) inbound: Inbound,
    pub(crate) flow: Outbound,
    pub(crate) backpressure: Backpressure,
    /// What the session counts against `max_state_bytes`.
    charge: Charge,
}

/// Encoded packets for the connection's writer.
#[derive(Debug, Default)]
pub(crate) struct Output {
    /// Waiting for the writer.
    pub(crate) buf: Vec<u8>,
    /// The writer's second buffer, swapped with `buf` on each write.
    pub(crate) spare: Vec<u8>,
    /// The writer took `buf` since the last sweep.
    wrote: bool,
    /// The lane wakes the writer once it finishes the current work.
    pub(crate) dirty: bool,
}

/// What the session keeps about the messages its client publishes.
#[derive(Debug, Default)]
pub(crate) struct Inbound {
    /// QoS 2 packet IDs waiting for PUBREL.
    pub(crate) awaiting_rel: FxHashSet<u16>,
    /// Topic aliases of the current connection.
    pub(crate) aliases: Vec<Option<ByteString>>,
    /// Matches of the last topic this client published to, and the index
    /// generation they belong to.
    pub(crate) last_topic: Option<(String, u64, Rc<Matches>)>,
    /// Last topic the authorizer was asked about, and its answer.
    pub(crate) last_allowed: Option<(Box<str>, bool)>,
}

/// QoS 1/2 messages towards the client.
#[derive(Debug)]
pub(crate) struct Outbound {
    /// Inflight window towards the client.
    pub(crate) window: u16,
    inflight: VecDeque<Inflight>,
    /// Entries at the back of `inflight` not resent on this connection yet:
    /// a resumed client may allow fewer in flight than before.
    unsent: usize,
    queue: VecDeque<Kept>,
    /// Bytes of the messages in `queue`, for `max_queued_bytes`.
    queued_bytes: usize,
    next_pkid: u16,
    /// Packet IDs in use. IDs go out in order, so the inflight window holds
    /// them in order and the next ID is free unless it wraps around to the
    /// oldest. Only then is this built, until the window empties.
    in_use: Option<Box<[u64; 1024]>>,
    /// Queued messages dropped because they expired.
    pub(crate) expired: u64,
    /// Copies of messages held long.
    arena: Arena,
    /// Bytes charged for the slots `queue` and `inflight` have room for, and
    /// the room left in the arena block.
    room: usize,
}

impl Default for Outbound {
    fn default() -> Self {
        Outbound {
            window: u16::MAX,
            inflight: VecDeque::new(),
            unsent: 0,
            queue: VecDeque::new(),
            queued_bytes: 0,
            next_pkid: 0,
            in_use: None,
            expired: 0,
            arena: Arena::default(),
            room: 0,
        }
    }
}

/// Publisher acks held while this session is congested.
#[derive(Debug, Default)]
pub(crate) struct Backpressure {
    /// Over the queue high watermark while connected.
    pub(crate) congested: bool,
    /// Publisher acks held until this session drains.
    pub(crate) waiters: Vec<Waiter>,
}

impl Session {
    pub(crate) fn new(client_id: Box<str>, version: Version, total: LaneTotal) -> Session {
        let charge = Charge::new(total, budget::session(&client_id));
        Session {
            client_id,
            username: None,
            version,
            conn: None,
            online: Arc::default(),
            subs: FxHashMap::default(),
            expiry: 0,
            expires_at: None,
            will: None,
            delayed_will: None,
            max_packet: usize::MAX,
            out: Output::default(),
            inbound: Inbound::default(),
            flow: Outbound::default(),
            backpressure: Backpressure::default(),
            charge,
        }
    }

    /// Adds or replaces a subscription. Returns whether it existed.
    pub(crate) fn subscribe(
        &mut self,
        filter: &str,
        opts: SubscriptionOptions,
        sub_id: u32,
        lanes: usize,
    ) -> bool {
        let existed = self.subs.insert(filter.into(), (opts, sub_id)).is_some();
        if !existed {
            self.charge.add(budget::subscription(filter, lanes));
        }
        existed
    }

    /// Removes a subscription. Returns whether it existed.
    pub(crate) fn unsubscribe(&mut self, filter: &str, lanes: usize) -> bool {
        let existed = self.subs.remove(filter).is_some();
        if existed {
            self.charge.sub(budget::subscription(filter, lanes));
        }
        existed
    }

    /// Attaches a connection.
    pub(crate) fn bind(&mut self, conn: Rc<ConnShared>) {
        self.conn = Some(conn);
        self.online.store(true, Ordering::Relaxed);
    }

    /// Detaches the connection.
    pub(crate) fn unbind(&mut self) {
        self.conn = None;
        self.online.store(false, Ordering::Relaxed);
    }

    /// Hands the writer the encoded packets, in exchange for its spare
    /// buffer.
    pub(crate) fn take_out(&mut self) -> Vec<u8> {
        self.out.wrote = true;
        let spare = std::mem::take(&mut self.out.spare);
        std::mem::replace(&mut self.out.buf, spare)
    }

    /// Frees large write buffers that are empty, unless the writer used them
    /// since the last call. The lane calls this once a second, so idle
    /// sessions do not keep a past burst's memory, and busy ones do not
    /// grow theirs again.
    pub(crate) fn trim_buffers(&mut self) {
        if !std::mem::take(&mut self.out.wrote) {
            if self.out.buf.is_empty() && self.out.buf.capacity() > IDLE_BUFFER {
                self.out.buf = Vec::new();
            }
            if self.out.spare.capacity() > IDLE_BUFFER {
                self.out.spare = Vec::new();
            }
        }
        if self.flow.inflight.is_empty() {
            self.flow.in_use = None;
        }
    }

    /// Whether `conn_id` is the current connection.
    #[inline]
    pub(crate) fn has_conn(&self, conn_id: u64) -> bool {
        self.conn.as_ref().is_some_and(|c| c.id == conn_id)
    }
}

impl Session {
    /// Messages sent and not acked: packet ID, message, and whether PUBREC
    /// released it.
    pub(crate) fn inflight(&self) -> impl ExactSizeIterator<Item = (u16, &Queued, bool)> {
        self.flow
            .inflight
            .iter()
            .map(|f| (f.pkid, &f.held.item, f.released))
    }

    /// Messages waiting for the inflight window, oldest first.
    pub(crate) fn queue(&self) -> impl ExactSizeIterator<Item = &Queued> {
        self.flow.queue.iter().map(|h| &h.item)
    }

    /// The last packet ID sent.
    pub(crate) const fn next_pkid(&self) -> u16 {
        self.flow.next_pkid
    }

    /// The offline session `st` describes, charging `total`. A delayed will
    /// comes back apart, for the lane to time it.
    pub(crate) fn restore(
        st: SessionState,
        total: LaneTotal,
        lanes: usize,
    ) -> (Session, Option<(Instant, Msg)>) {
        let mut s = Session::new(st.client_id, st.version, total);
        s.username = st.username;
        s.expiry = st.expiry;
        s.expires_at = st.expires_at;
        s.will = st.will;
        s.flow.next_pkid = st.next_pkid;
        for (filter, opts, sub_id) in &st.subs {
            s.subscribe(filter, *opts, *sub_id, lanes);
        }
        s.inbound.awaiting_rel.extend(st.awaiting_rel);
        for (pkid, item, released) in st.inflight {
            let held = Kept::own(item);
            s.charge.add(budget::held(&held));
            s.flow.inflight.push_back(Inflight {
                pkid,
                held,
                released,
            });
        }
        if !s.flow.inflight.is_empty() {
            // The window may hold IDs out of order since a wrap; the bitmap
            // finds free ones in any order.
            let mut bits = Box::new([0; 1024]);
            for f in &s.flow.inflight {
                bits[usize::from(f.pkid >> 6)] |= 1u64 << (f.pkid & 63);
            }
            s.flow.in_use = Some(bits);
        }
        for item in st.queue {
            let h = Kept::own(item);
            s.charge.add(budget::held(&h));
            s.flow.queued_bytes += h.item.msg.size();
            s.flow.queue.push_back(h);
        }
        s.charge_room();
        (s, st.delayed_will)
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

    fn held() -> Kept {
        Kept::own(item())
    }

    const fn limit(count: usize, bytes: usize) -> QueueLimit {
        QueueLimit {
            count,
            bytes,
            state_full: false,
        }
    }

    /// The queue makes room by bytes, oldest first, and the lane total
    /// follows every charge back to zero.
    #[test]
    fn queue_bytes_and_charges() {
        let total = LaneTotal::default();
        let mut s = Session::new("c".into(), Version::V5, total.clone());
        let base = total.get();
        assert!(base > 0);
        let opts = SubscriptionOptions::default();
        assert!(!s.subscribe("a/#", opts, 0, 2));
        assert!(s.subscribe("a/#", opts, 0, 2));
        let subscribed = total.get();
        assert!(subscribed > base);
        let size = item().msg.size();
        let room = limit(usize::MAX, 3 * size);
        for _ in 0..3 {
            assert_eq!(s.deliver_qos12(held(), room), Outcome::Queued);
        }
        assert_eq!(s.deliver_qos12(held(), room), Outcome::QueueFull(1));
        assert_eq!(s.queued(), 3);
        // Larger than the limit alone, or no room left in the budget.
        let small = limit(usize::MAX, size - 1);
        assert_eq!(s.deliver_qos12(held(), small), Outcome::QueueFull(1));
        let full = QueueLimit {
            state_full: true,
            ..room
        };
        assert_eq!(s.deliver_qos12(held(), full), Outcome::StateFull);
        assert!(s.flow.room >= 3 * size_of::<Kept>());
        assert_eq!(
            total.get(),
            subscribed + 3 * budget::held(&held()) + s.flow.room
        );
        assert_eq!(s.charge.get(), total.get());
        assert!(s.unsubscribe("a/#", 2));
        drop(s);
        assert_eq!(total.get(), 0);
    }

    /// Sessions that share a message count its bytes once, until the last
    /// of them lets go.
    #[test]
    fn a_shared_message_counts_once() {
        let total = LaneTotal::default();
        let mut sessions = ["a", "b"].map(|id| Session::new(id.into(), Version::V5, total.clone()));
        // Both sessions alike.
        let base = total.get();
        let bytes = budget::shared(&item().msg);
        let share = Rc::new(Charge::new(total.clone(), bytes));
        let shared = || Kept {
            item: item(),
            share: Some(share.clone()),
        };
        assert_eq!(budget::held(&shared()), 0);
        for s in &mut sessions {
            assert_eq!(s.deliver_qos12(shared(), limit(10, 10)), Outcome::Queued);
        }
        drop(share);
        // Each queue has room for a few slots.
        let room = sessions[0].flow.room;
        assert!(room >= size_of::<Kept>());
        assert_eq!(total.get(), base + bytes + 2 * room);
        let [a, b] = sessions;
        drop(a);
        assert_eq!(total.get(), base / 2 + bytes + room);
        drop(b);
        assert_eq!(total.get(), 0);
    }

    /// A session pays for the slots its queue grew to and the room left in
    /// its arena block, which outlast the messages.
    #[test]
    fn slots_and_arena_room_stay_charged() {
        let total = LaneTotal::default();
        let mut s = Session::new("c".into(), Version::V5, total.clone());
        let base = total.get();
        let msg = s.copy(&item().msg);
        let room = s.flow.room;
        assert!(room > 0 && room == s.flow.arena.room());
        let copied = Kept::own(Queued { msg, ..item() });
        let bytes = budget::held(&copied);
        assert_eq!(
            s.deliver_qos12(copied, limit(10, usize::MAX)),
            Outcome::Queued
        );
        assert_eq!(total.get(), base + bytes + s.flow.room);
        assert_eq!(
            s.flow.room,
            room + s.flow.queue.capacity() * size_of::<Kept>()
        );
        let h = s.pop_queued().unwrap();
        s.charge.sub(budget::held(&h));
        drop(h);
        // The queue keeps its slots and the arena its block.
        assert_eq!(total.get(), base + s.flow.room);
        drop(s);
        assert_eq!(total.get(), 0);
    }

    /// A restored window may hold IDs out of order since a wrap: new IDs
    /// skip them. The charges go with the session.
    #[test]
    fn restored_session_skips_ids_in_use() {
        let total = LaneTotal::default();
        let st = SessionState {
            client_id: "c".into(),
            username: None,
            version: Version::V5,
            expiry: 60,
            expires_at: None,
            will: None,
            delayed_will: None,
            next_pkid: 7,
            subs: vec![("a/#".into(), SubscriptionOptions::default(), 0)],
            awaiting_rel: Vec::new(),
            inflight: vec![
                (65535, item(), false),
                (8, item(), false),
                (7, item(), false),
            ],
            queue: vec![item()],
        };
        let (mut s, _) = Session::restore(st, total.clone(), 1);
        assert_eq!((s.queued(), s.flow.inflight.len()), (1, 3));
        assert_eq!(s.alloc_pkid(), 9);
        drop(s);
        assert_eq!(total.get(), 0);
    }

    /// Sends a message the session is charged for.
    fn send(s: &mut Session) -> Outcome {
        let h = held();
        s.charge.add(budget::held(&h));
        s.send_inflight(h)
    }

    fn ids(s: &Session) -> Vec<u16> {
        s.flow.inflight.iter().map(|f| f.pkid).collect()
    }

    /// One message stays unacked while others pass by more than all IDs:
    /// IDs stay unique, and in order again once the window empties.
    #[test]
    fn pkids_skip_unacked_after_wrapping() {
        let mut s = Session::new("c".into(), Version::V5, LaneTotal::default());
        send(&mut s);
        let stuck = s.flow.inflight[0].pkid;
        for n in 0..140_000 {
            send(&mut s);
            send(&mut s);
            let mut v = ids(&s);
            v.sort_unstable();
            v.dedup();
            assert_eq!(v.len(), s.flow.inflight.len(), "after {n}");
            // Ack the older of the two new ones, out of order.
            assert_eq!(s.on_puback(s.flow.inflight[1].pkid), Ok(false));
            assert_eq!(s.on_puback(s.flow.inflight[1].pkid), Ok(false));
        }
        assert!(s.flow.in_use.is_some());
        assert_eq!(s.on_puback(stuck), Ok(false));
        s.trim_buffers();
        assert!(s.flow.in_use.is_none());
        send(&mut s);
        send(&mut s);
        let v = ids(&s);
        assert_eq!(v[1], v[0].wrapping_add(1).max(1));
    }

    /// A large write buffer outlives a sweep after a write, and is freed by
    /// the next sweep without one.
    #[test]
    fn trim_frees_buffers_of_idle_sessions_only() {
        let mut s = Session::new("c".into(), Version::V5, LaneTotal::default());
        s.out.buf.reserve(2 * IDLE_BUFFER);
        let mut buf = s.take_out();
        buf.clear();
        s.out.spare = buf;
        s.trim_buffers();
        assert!(s.out.spare.capacity() > IDLE_BUFFER);
        s.trim_buffers();
        assert_eq!(s.out.spare.capacity(), 0);
    }
}

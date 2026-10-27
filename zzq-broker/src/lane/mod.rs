//! State of one broker lane: its sessions, a copy of the subscription
//! index, and the packet handling for its connections.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::broker::{Dropped, Shared};
use crate::budget::LaneTotal;
use crate::conn::ReadBlocks;
use crate::index::{SubIndex, SubRef};
use crate::msg::Arena;
use crate::session::{Session, Waiter};
use crate::slab::{Key, Slab};

mod backpressure;
mod connect;
mod control;
mod maintenance;
mod packet;
mod route;
mod subscribe;

use backpressure::Hit;
pub(crate) use control::{Control, Dump, SyncAck, Ticket};
pub(crate) use route::Remote;
pub(crate) use subscribe::Held;

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
    pub(crate) dropped: Dropped,
    /// Bytes the lane's sessions count against `max_state_bytes`.
    pub(crate) state_bytes: u64,
}

impl LaneStats {
    /// Adds what session `s` counts itself: messages it sent from its
    /// queue, and those it dropped.
    pub(crate) const fn add_session(&mut self, s: &Session) {
        self.deliveries += s.flow.sent;
        self.dropped.expired += s.flow.expired;
        self.dropped.too_large += s.flow.too_large;
    }
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
    pub(crate) out: Outbox,
    held: Vec<Held>,
    congestion: Congestion,
    pub(crate) stats: LaneStats,
    pub(crate) budget: StateBudget,
    /// What connections without a buffer of their own read into.
    pub(crate) read_blocks: ReadBlocks,
    /// Calls of [`Lane::sweep`] so far.
    sweeps: u32,
    ids: Ids,
    /// Sessions with a delayed will, by its due time and timer ID.
    wills: BTreeMap<(Instant, u64), Key>,
    pub(crate) phase: Phase,
}

/// Whether a lane serves connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Phase {
    Open,
    /// Closing for a state dump: no new connections, and sessions and
    /// delayed wills wait with their due times.
    Closing,
    /// Every connection closed and the lane's loops stopped.
    ShutDown,
}

/// What waits to go to other lanes.
#[derive(Debug)]
pub(crate) struct Outbox {
    /// Batches by target lane.
    remote: Box<[Vec<Remote>]>,
    /// One task ships at a time, in arrival order, so batches for a lane
    /// leave in the order they were collected, also while it is full.
    pub(crate) shipping: Rc<tokio::sync::Mutex<()>>,
    /// Copies of QoS 1/2 publishes for congested sessions on other lanes.
    fwd: Arena,
}

/// Congested sessions and the publisher acks held for them.
#[derive(Debug, Default)]
struct Congestion {
    /// First congested target of the publish being routed.
    hit: Option<Hit>,
    /// Congested sessions on other lanes.
    remote: FxHashSet<SubRef>,
    /// Acks of local publishers held for congested remote sessions.
    waiters: FxHashMap<SubRef, Vec<Waiter>>,
}

/// The lane's part of `max_state_bytes`.
#[derive(Debug)]
pub(crate) struct StateBudget {
    /// Bytes the sessions charge.
    total: LaneTotal,
    /// The budget was reached at the last check.
    pub(crate) full: bool,
}

/// The last IDs the lane handed out.
#[derive(Debug, Default)]
struct Ids {
    conn: u64,
    client_no: u64,
    will: u64,
}

impl Lane {
    pub(crate) fn new(id: u16, shared: Arc<Shared>) -> Lane {
        let lanes = shared.lanes.len();
        let total = LaneTotal::default();
        Lane {
            id,
            shared,
            sessions: Slab::default(),
            by_client: FxHashMap::default(),
            index: SubIndex::default(),
            dirty: Vec::new(),
            out: Outbox {
                remote: (0..lanes).map(|_| Vec::new()).collect(),
                shipping: Rc::default(),
                fwd: Arena::default(),
            },
            held: Vec::new(),
            congestion: Congestion::default(),
            stats: LaneStats::default(),
            budget: StateBudget { total, full: false },
            read_blocks: ReadBlocks::default(),
            sweeps: 0,
            ids: Ids::default(),
            wills: BTreeMap::new(),
            phase: Phase::Open,
        }
    }

    pub(crate) const fn next_conn_id(&mut self) -> u64 {
        self.ids.conn += 1;
        self.ids.conn
    }

    const fn sub_ref(&self, key: Key) -> SubRef {
        SubRef { lane: self.id, key }
    }

    #[inline]
    fn mark_in(dirty: &mut Vec<Key>, s: &mut Session, key: Key) {
        if !s.out.dirty {
            s.out.dirty = true;
            dirty.push(key);
        }
    }

    #[inline]
    fn mark(&mut self, key: Key) {
        if let Some(s) = self.sessions.get_mut(key)
            && !s.out.dirty
        {
            s.out.dirty = true;
            self.dirty.push(key);
        }
    }

    /// Wakes the writers of sessions with new output.
    #[inline]
    pub(crate) fn flush(&mut self) {
        for key in self.dirty.drain(..) {
            if let Some(s) = self.sessions.get_mut(key) {
                s.out.dirty = false;
                if let Some(c) = &s.conn {
                    c.wake.notify_one();
                }
            }
        }
    }
}

//! State of one broker lane: its sessions, a copy of the subscription
//! index, and the packet handling for its connections.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::broker::Shared;
use crate::budget::{Charge, LaneTotal};
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
pub(crate) use control::{Control, Dump, SyncAck};
pub(crate) use route::Remote;
use subscribe::Held;

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
    /// The share of retained messages sent on subscribe, which the store
    /// counts.
    retained_share: Rc<Charge>,
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
        let state = LaneTotal::default();
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
            retained_share: Rc::new(Charge::new(state.clone(), 0)),
            state,
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
}

//! Byte accounting for `max_state_bytes`.
//!
//! Sessions charge their lane for what they hold; the retained store keeps
//! its own count. Charges are estimates: struct sizes plus the bytes of
//! strings and messages, scaled for hash table and allocator slack. Queues
//! and the arena that holds message copies count as allocated, since they
//! keep their room as they empty. A message that sessions of one lane share
//! counts once. A retained message counts with the store until no session
//! holds it either, so a session that got it on subscribe charges only its
//! slot. Each lane adds up all counts on its sweep, so a check costs no
//! atomics.

use std::cell::Cell;
use std::mem::size_of;
use std::rc::Rc;

use crate::index::Entry;
use crate::msg::Msg;
use crate::session::{Kept, Session};

/// Bytes charged to the sessions of one lane.
pub(crate) type LaneTotal = Rc<Cell<usize>>;

/// A session without subscriptions or messages, also counting its slot,
/// its entry in the client ID map and small allocations.
const SESSION: usize = size_of::<Session>() + 192;
/// One subscription in the session, besides its index entries.
const SUB: usize = 96;
/// A filter level in the index of one lane: a trie node, as if no other
/// filter shared it.
const LEVEL: usize = 384;
/// A QoS 1/2 message a session holds, besides its bytes and its queue slot:
/// allocator overhead.
const MSG: usize = 16;
/// The charge for a shared message, and its reference counts.
const SHARE: usize = size_of::<Charge>() + 2 * size_of::<usize>();
/// A level of the retained message tree: the node, its key and hash table
/// slack.
pub(crate) const NODE: usize = 320;

/// What a session costs before subscriptions and messages.
pub(crate) const fn session(client_id: &str) -> usize {
    SESSION + client_id.len()
}

/// What a subscription to `filter` costs, with an index entry on each of
/// `lanes`.
pub(crate) fn subscription(filter: &str, lanes: usize) -> usize {
    let levels = filter.bytes().filter(|&b| b == b'/').count() + 1;
    SUB + filter.len() + lanes * (size_of::<Entry>() + LEVEL * levels)
}

/// What a held QoS 1/2 message costs the session besides its slot: its
/// bytes, unless it has a share of them. The session charges the slots its
/// queues have room for itself.
pub(crate) const fn held(h: &Kept) -> usize {
    if h.share.is_some() {
        0
    } else {
        MSG + h.item.msg.size()
    }
}

/// What the bytes of a message cost once, for all sessions that share it.
pub(crate) const fn shared(msg: &Msg) -> usize {
    SHARE + MSG + msg.size()
}

/// The bytes one session charges its lane. Dropping it takes them back.
#[derive(Debug)]
pub(crate) struct Charge {
    own: usize,
    total: LaneTotal,
}

impl Charge {
    pub(crate) fn new(total: LaneTotal, n: usize) -> Charge {
        total.set(total.get() + n);
        Charge { own: n, total }
    }

    #[inline]
    pub(crate) fn add(&mut self, n: usize) {
        self.own += n;
        self.total.set(self.total.get() + n);
    }

    #[inline]
    pub(crate) fn sub(&mut self, n: usize) {
        self.own -= n;
        self.total.set(self.total.get() - n);
    }

    #[cfg(test)]
    pub(crate) const fn get(&self) -> usize {
        self.own
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.total.set(self.total.get() - self.own);
    }
}

//! Pair cursors: how far a lane's journal holds what another lane sent it,
//! for the acks of publishers on that lane.
//!
//! Each lane counts the deliveries it sends to every other lane, and the
//! receiver uses their absolute sequence numbers. Once the
//! receiver's journal holds every record it made for deliveries up to a
//! count, it moves the cursor of the pair to that count. Handoffs count
//! apart from deliveries: each lane has a slot for both, handoffs after all
//! the deliveries.

use std::collections::VecDeque;

use crate::durable::{Cursor, JournalId, Wait};

use super::Lane;

/// One lane's counts towards and from every lane.
#[derive(Debug, Default)]
pub(crate) struct Pairs {
    /// Counting is on: the broker has a data directory.
    on: bool,
    lanes: usize,
    /// Sent to each slot.
    sent: Box<[u64]>,
    /// The count each pair cursor reached.
    done: Box<[u64]>,
    /// Per sending slot: a count got and the last record of this lane's
    /// journal it needs, oldest first.
    pending: Box<[VecDeque<(u64, u64, bool)>]>,
    /// The last record appended to this lane's journal.
    pub(super) appended: u64,
    /// Records the journal wrote, or failed to.
    written: u64,
}

impl Pairs {
    pub(super) fn new(lanes: usize, on: bool) -> Pairs {
        if !on {
            return Pairs::default();
        }
        Pairs {
            on,
            lanes,
            sent: vec![0; 2 * lanes].into(),
            done: vec![0; 2 * lanes].into(),
            pending: (0..2 * lanes).map(|_| VecDeque::new()).collect(),
            appended: 0,
            written: 0,
        }
    }

    /// Counts a delivery lane `from` sends to lane `to`. Returns the
    /// record a publisher's ack waits for there, if counting is on.
    pub(super) fn sent(&mut self, from: u16, to: usize) -> Option<Wait> {
        let n = self.sent.get_mut(to)?;
        *n += 1;
        Some(Wait {
            journal: JournalId::Pair {
                from: usize::from(from),
                to,
            },
            n: *n,
        })
    }

    /// Counts a handoff lane `from` sends to lane `to`. Returns the record
    /// a publisher's ack waits for there, if counting is on.
    pub(super) fn sent_handoff(&mut self, from: u16, to: usize) -> Option<Wait> {
        let n = self.sent.get_mut(self.lanes + to)?;
        *n += 1;
        Some(Wait {
            journal: JournalId::Handoff {
                from: usize::from(from),
                to,
            },
            n: *n,
        })
    }

    /// Old waits lost their connections. Keep sequence numbers monotonic:
    /// shared cursors and journal completions outlive a lane's state.
    pub(super) fn reset(&mut self) {
        for queue in &mut self.pending {
            queue.clear();
        }
    }
}

impl Lane {
    /// Whether the broker has a data directory.
    pub(super) const fn pairs_on(&self) -> bool {
        self.pairs.on
    }

    /// The cursor of `slot`, towards this lane.
    fn pair(&self, slot: usize) -> &Cursor {
        let to = usize::from(self.id);
        let lanes = self.pairs.lanes;
        self.shared.cursor(if slot < lanes {
            JournalId::Pair { from: slot, to }
        } else {
            JournalId::Handoff {
                from: slot - lanes,
                to,
            }
        })
    }

    /// Records through delivery `count` from lane `from` are appended.
    /// Gaps are allowed: a reset can discard batches not shipped yet.
    pub(super) fn got_from(&mut self, from: u16, count: u64) {
        self.received_from(usize::from(from), count, false);
    }

    /// Records through handoff `count` from lane `from` are appended, or
    /// that handoff was refused, unless `written`.
    pub(super) fn got_handoff_from(&mut self, from: u16, count: u64, written: bool) {
        self.received_from(self.pairs.lanes + usize::from(from), count, !written);
    }

    fn received_from(&mut self, slot: usize, count: u64, failed: bool) {
        let p = &mut self.pairs;
        if !p.on || slot >= p.pending.len() {
            return;
        }
        let needs = p.appended;
        let queue = &mut p.pending[slot];
        if queue.is_empty() && needs <= p.written {
            let first = p.done[slot] + 1;
            p.done[slot] = count;
            if failed {
                self.pair(slot).fail(first, count);
            } else {
                self.pair(slot).advance(count);
            }
        } else if let Some(last) = queue.back_mut().filter(|l| l.1 == needs && l.2 == failed) {
            last.0 = count;
        } else {
            queue.push_back((count, needs, failed));
        }
    }

    /// The journal wrote records `first..=last`, or failed to with
    /// `failed`: moves the pair cursors past the deliveries they cover.
    pub(super) fn pairs_written(&mut self, first: u64, last: u64, failed: bool) {
        if !self.pairs.on {
            return;
        }
        self.pairs.written = self.pairs.written.max(last);
        for from in 0..self.pairs.pending.len() {
            while let Some(&(count, needs, refused)) = self.pairs.pending[from].front()
                && needs <= last
            {
                self.pairs.pending[from].pop_front();
                let cursor = self.pair(from);
                if refused || (failed && needs >= first) {
                    cursor.fail(self.pairs.done[from] + 1, count);
                } else {
                    cursor.advance(count);
                }
                self.pairs.done[from] = count;
            }
        }
    }
}

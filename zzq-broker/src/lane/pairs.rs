//! Pair cursors: how far a lane's journal holds what another lane sent it,
//! for the acks of publishers on that lane.
//!
//! Each lane counts the deliveries it sends to every other lane, and the
//! receiver counts them as it takes them, in the same order. Once the
//! receiver's journal holds every record it made for deliveries up to a
//! count, it moves the cursor of the pair to that count.

use std::collections::VecDeque;

use crate::durable::{Cursor, JournalId, Wait};

use super::Lane;

/// One lane's counts towards and from every lane.
#[derive(Debug, Default)]
pub(crate) struct Pairs {
    /// Counting is on: the broker has a data directory.
    on: bool,
    /// Deliveries sent to each lane.
    sent: Box<[u64]>,
    /// Deliveries got from each lane.
    got: Box<[u64]>,
    /// The count each pair cursor reached.
    done: Box<[u64]>,
    /// Per sending lane: a count got and the last record of this lane's
    /// journal it needs, oldest first.
    pending: Box<[VecDeque<(u64, u64)>]>,
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
            sent: vec![0; lanes].into(),
            got: vec![0; lanes].into(),
            done: vec![0; lanes].into(),
            pending: (0..lanes).map(|_| VecDeque::new()).collect(),
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
}

impl Lane {
    /// Whether the broker has a data directory.
    pub(super) const fn pairs_on(&self) -> bool {
        self.pairs.on
    }

    /// The cursor of the pair from lane `from` to this one.
    fn pair(&self, from: usize) -> &Cursor {
        self.shared.cursor(JournalId::Pair {
            from,
            to: usize::from(self.id),
        })
    }

    /// Counts `n` deliveries got from lane `from`, whose records are
    /// appended by now.
    pub(super) fn got_from(&mut self, from: u16, n: u64) {
        let from = usize::from(from);
        let p = &mut self.pairs;
        if !p.on || from >= p.got.len() {
            return;
        }
        p.got[from] += n;
        let count = p.got[from];
        let needs = p.appended;
        let queue = &mut p.pending[from];
        if queue.is_empty() && needs <= p.written {
            p.done[from] = count;
            self.pair(from).advance(count);
        } else if let Some(last) = queue.back_mut().filter(|l| l.1 == needs) {
            last.0 = count;
        } else {
            queue.push_back((count, needs));
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
            while let Some(&(count, needs)) = self.pairs.pending[from].front()
                && needs <= last
            {
                self.pairs.pending[from].pop_front();
                let cursor = self.pair(from);
                if failed && needs >= first {
                    cursor.fail(self.pairs.done[from] + 1, count);
                } else {
                    cursor.advance(count);
                }
                self.pairs.done[from] = count;
            }
        }
    }
}

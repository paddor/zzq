//! Work other lanes and the broker handle send to a lane.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::oneshot;
use zzq_tokio::signal::StateSignal;

use crate::dump::Part;
use crate::index::{Entry, SubRef};
use crate::msg::Msg;
use crate::state::SessionState;

use super::{Lane, LaneStats};

/// Counts the lanes that still have to apply a change, such as a new
/// subscription, and wakes the waiter once none has to.
#[derive(Debug, Default)]
pub(crate) struct SyncAck {
    remaining: AtomicUsize,
    changed: StateSignal,
}

impl SyncAck {
    /// A share for one more lane, counted down when the lane drops it.
    pub(crate) fn ticket(self: &Arc<Self>) -> Ticket {
        // The ticket travels to its lane through a queue, which orders this
        // before the countdown.
        self.remaining.fetch_add(1, Ordering::Relaxed);
        Ticket(self.clone())
    }

    /// Waits until every lane applied the change.
    pub(crate) async fn wait(&self) {
        self.changed
            .until(|| (self.remaining.load(Ordering::Acquire) == 0).then_some(()))
            .await;
    }
}

/// One lane's share of a [`SyncAck`]. The lane drops it once it applied
/// the change, and a lane that stopped drops it with its queue, so the
/// waiter never waits for a lane that is gone.
#[derive(Debug)]
pub(crate) struct Ticket(Arc<SyncAck>);

impl Ticket {
    /// A share of the same change for another lane.
    pub(crate) fn another(&self) -> Ticket {
        self.0.ticket()
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        // Release: what the lane did before is visible to the waiter.
        if self.0.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.changed.notify_changed();
        }
    }
}

/// Work sent to a lane by other lanes or the broker handle.
#[derive(Debug)]
pub(crate) enum Control {
    /// Add a subscription, then count down the ack.
    SubAdd(Box<str>, Entry, Option<Ticket>),
    /// Remove a subscription, then count down the ack.
    SubRemove(Box<str>, SubRef, Option<Ticket>),
    Publish(Msg),
    /// A session on another lane crossed its queue watermark.
    Congested(SubRef, bool),
    Stats(oneshot::Sender<LaneStats>),
    /// Close every connection and stop, then answer.
    Shutdown(Option<oneshot::Sender<()>>),
    /// Close every connection as lost and refuse new ones, then answer.
    Quiesce(oneshot::Sender<()>),
    /// Ship what is waiting for other lanes, then a marker to each.
    Drain(Ticket),
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

impl Lane {
    pub(super) fn broadcast(&self, make: &impl Fn() -> Control) {
        for (i, lane) in self.shared.lanes.iter().enumerate() {
            if i != usize::from(self.id) {
                let _ = lane.control.push(make());
            }
        }
    }

    /// Handles a control message.
    pub(crate) fn control(&mut self, c: Control) {
        match c {
            Control::SubAdd(filter, entry, ticket) => {
                self.index.add(&filter, entry);
                drop(ticket);
                self.stats.subscriptions = self.index.len() as u64;
            }
            Control::SubRemove(filter, sub, ticket) => {
                self.index.remove(&filter, sub);
                drop(ticket);
                self.stats.subscriptions = self.index.len() as u64;
            }
            Control::Publish(msg) => self.publish_owned(&msg),
            Control::Congested(sub, true) => {
                self.congestion.remote.insert(sub);
            }
            Control::Congested(sub, false) => {
                self.congestion.remote.remove(&sub);
                if let Some(waiters) = self.congestion.waiters.remove(&sub) {
                    self.send_acks(&waiters);
                }
            }
            Control::Stats(reply) => {
                let mut stats = self.stats;
                stats.sessions = self.sessions.len() as u64;
                stats.state_bytes = self.budget.total.get() as u64;
                // Removed sessions added theirs to `self.stats`.
                stats.dropped.expired += self
                    .sessions
                    .keys()
                    .filter_map(|k| self.sessions.get(k))
                    .map(|s| s.flow.expired)
                    .sum::<u64>();
                let _ = reply.send(stats);
            }
            Control::Quiesce(done) => {
                self.quiesce();
                let _ = done.send(());
            }
            Control::Drain(ticket) => self.drain(&ticket),
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
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::Remote;
    use super::*;

    #[tokio::test]
    async fn a_lane_that_never_handles_its_part_still_counts_down() {
        let sync = Arc::new(SyncAck::default());
        // Queued for a lane that stopped: dropped with its queue.
        drop(Remote::Drained(sync.ticket()));
        let waited = tokio::time::timeout(Duration::from_secs(1), sync.wait()).await;
        assert!(waited.is_ok(), "the waiter hangs");
    }
}

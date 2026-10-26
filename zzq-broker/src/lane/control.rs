//! Work other lanes and the broker handle send to a lane.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Notify, oneshot};

use crate::dump::Part;
use crate::index::{Entry, SubRef};
use crate::msg::Msg;
use crate::state::SessionState;

use super::{Lane, LaneStats};

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

    pub(super) fn done(&self) {
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
}

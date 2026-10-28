//! How far a journal's writes got, for acks that wait for the disk.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use zzq_tokio::signal::StateSignal;

/// Wait before asking again for a segment file that could not be made.
pub(crate) const RETRY: std::time::Duration = std::time::Duration::from_millis(100);

/// Failed writes a cursor tells apart. Waiters for records of older ones
/// count as failed.
const FAILURES: usize = 64;

/// A journal of the data directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JournalId {
    /// Of a stripe of retained messages.
    Retained(usize),
    /// Of a lane's sessions.
    Lane(usize),
    /// Of lane `to`, for the deliveries lane `from` sent it.
    Pair { from: usize, to: usize },
}

/// A journal record an ack waits for: its journal and its number there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Wait {
    pub(crate) journal: JournalId,
    pub(crate) n: u64,
}

/// What became of a change a lane made to journaled state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Journaled {
    /// It is record `wait.n` of its journal. `crowded` once records waiting
    /// for the disk there passed their cap.
    At { wait: Wait, crowded: bool },
    /// It could not be journaled.
    Failed,
}

/// What an ack waits for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Due {
    /// Nothing.
    Now,
    /// This record.
    After(Wait),
    /// Its change never reached a journal.
    Failed,
}

impl Due {
    /// This, or the later record of `j`. A failure stays.
    pub(crate) const fn then(self, j: Option<Journaled>) -> Due {
        match (self, j) {
            (Due::Failed, _) | (_, None) => self,
            (_, Some(Journaled::Failed)) => Due::Failed,
            (_, Some(Journaled::At { wait, .. })) => Due::After(wait),
        }
    }
}

/// The writes of one journal: records up to `done` are written or failed.
/// Waiters for a record wake once it is.
#[derive(Debug, Default)]
pub(crate) struct Cursor {
    done: AtomicU64,
    /// A write failed once: from then on, waiters check `failed`.
    any_failed: AtomicBool,
    /// The journal stopped: records after `done` are never written.
    closed: AtomicBool,
    failed: Mutex<Failed>,
    changed: StateSignal,
}

#[derive(Debug, Default)]
struct Failed {
    /// Failed record ranges, oldest first.
    ranges: VecDeque<(u64, u64)>,
    /// Records up to this one may be in ranges let go.
    forgotten: u64,
}

impl Cursor {
    /// Records up to `last` are written.
    pub(crate) fn advance(&self, last: u64) {
        // Release: a waiter that sees `last` sees a failure recorded
        // before it.
        self.done.fetch_max(last, Ordering::Release);
        self.changed.notify_changed();
    }

    /// Writing records `first..=last` failed. They may be on disk or not.
    pub(crate) fn fail(&self, first: u64, last: u64) {
        {
            let mut f = self.lock();
            if f.ranges.len() == FAILURES
                && let Some((_, end)) = f.ranges.pop_front()
            {
                f.forgotten = end;
            }
            f.ranges.push_back((first, last));
        }
        // Relaxed: `advance` publishes it with Release.
        self.any_failed.store(true, Ordering::Relaxed);
        self.advance(last);
    }

    /// No more writes come.
    pub(crate) fn close(&self) {
        // Release: pairs with the Acquire in `check`.
        self.closed.store(true, Ordering::Release);
        self.changed.notify_changed();
    }

    /// Waits until record `n` is written or failed. True if written.
    pub(crate) async fn wait(&self, n: u64) -> bool {
        self.changed.until(|| self.check(n)).await
    }

    /// Whether record `n` is written; `None` while it is on its way.
    pub(crate) fn check(&self, n: u64) -> Option<bool> {
        // Acquire: pairs with the Release in `advance`.
        if self.done.load(Ordering::Acquire) >= n {
            if !self.any_failed.load(Ordering::Relaxed) {
                return Some(true);
            }
            let f = self.lock();
            let failed = n <= f.forgotten || f.ranges.iter().any(|r| (r.0..=r.1).contains(&n));
            return Some(!failed);
        }
        self.closed.load(Ordering::Acquire).then_some(false)
    }

    fn lock(&self) -> MutexGuard<'_, Failed> {
        self.failed.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    #[test]
    fn records_are_written_once_the_cursor_passes_them() {
        let c = Cursor::default();
        assert_eq!(c.check(1), None);
        c.advance(3);
        assert_eq!(
            (c.check(1), c.check(3), c.check(4)),
            (Some(true), Some(true), None)
        );
    }

    #[test]
    fn records_of_a_failed_write_fail_and_later_ones_do_not() {
        let c = Cursor::default();
        c.advance(2);
        c.fail(3, 5);
        c.advance(6);
        let got: Vec<_> = (1..=7).map(|n| c.check(n)).collect();
        let t = Some(true);
        let f = Some(false);
        assert_eq!(got, [t, t, f, f, f, t, None]);
    }

    #[test]
    fn records_of_forgotten_failures_count_as_failed() {
        let c = Cursor::default();
        for i in 0..=FAILURES as u64 {
            c.fail(10 * i + 3, 10 * i + 4);
        }
        assert_eq!(c.check(1), Some(false), "up to the forgotten failure");
        assert_eq!(c.check(5), Some(true), "after it, not failed");
        assert_eq!(c.check(13), Some(false), "remembered");
    }

    #[test]
    fn a_closed_cursor_fails_records_it_never_reached() {
        let c = Cursor::default();
        c.advance(1);
        c.close();
        assert_eq!((c.check(1), c.check(2)), (Some(true), Some(false)));
    }

    #[tokio::test]
    async fn a_waiter_wakes_when_its_record_is_written() {
        let c = Arc::new(Cursor::default());
        let waiter = tokio::spawn({
            let c = c.clone();
            async move { c.wait(2).await }
        });
        c.advance(1);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        c.advance(2);
        let written = tokio::time::timeout(Duration::from_secs(5), waiter).await;
        assert!(written.unwrap().unwrap());
    }
}

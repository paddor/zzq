//! Wakes between tasks on different lanes.
//!
//! [`DataSignal`] tells one consumer that a queue has items:
//!
//! - The **producer** calls [`DataSignal::mark`] after the push that made
//!   the queue non-empty, as the lock that orders pushes and takes tells
//!   it. A burst then costs one wake.
//! - The **consumer** takes everything queued, and waits in
//!   [`DataSignal::ready`] once it finds nothing.
//! - A mark while the consumer is not waiting is kept, so a mark between
//!   its take and its wait is not lost. The consumer may then find nothing
//!   and wait again.
//!
//! [`StateSignal`] tells any number of waiters that something changed, such
//! as room in a full queue.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

/// A `Notify` whose waiter list is set up already.
fn new_notify() -> Notify {
    let notify = Notify::new();
    // Tokio's waiter mutex may allocate on first use. Register and remove
    // one waiter while the signal is private.
    {
        let mut waiter = std::pin::pin!(notify.notified());
        waiter.as_mut().enable();
    }
    notify
}

/// "Items available" for one consumer.
#[derive(Debug)]
pub struct DataSignal {
    notify: Notify,
}

impl DataSignal {
    /// A signal without a mark.
    #[must_use]
    pub fn new() -> Self {
        Self {
            notify: new_notify(),
        }
    }

    /// Producer: the queue went from empty to non-empty. Wakes the
    /// consumer, or keeps the mark until it waits.
    #[inline]
    pub fn mark(&self) {
        self.notify.notify_one();
    }

    /// Waits until marked. Returns at once for a mark kept since the last
    /// wait, also one whose items the consumer took already.
    ///
    /// A wait that was woken and then dropped, as by a `select!` branch
    /// that lost, keeps the mark for the next.
    pub async fn ready(&self) {
        self.notify.notified().await;
    }
}

impl Default for DataSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// "Something changed" for any number of waiters.
///
/// Every change bumps a generation. [`StateSignal::until`] reads it before
/// each attempt and waits for a newer one after a miss, so a change between
/// the attempt and the wait is not lost.
#[derive(Debug)]
pub struct StateSignal {
    generation: AtomicU64,
    notify: Notify,
}

impl StateSignal {
    /// A signal at generation zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            notify: new_notify(),
        }
    }

    /// Runs `attempt` until it returns `Some`, waiting for a change after
    /// each `None`.
    pub async fn until<T>(&self, mut attempt: impl FnMut() -> Option<T>) -> T {
        loop {
            let seen = self.generation();
            if let Some(out) = attempt() {
                return out;
            }
            self.changed_after(seen).await;
        }
    }

    #[inline]
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Bumps the generation and wakes every waiter.
    #[inline]
    pub fn notify_changed(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Waits until the generation differs from `seen`.
    async fn changed_after(&self, seen: u64) {
        if self.generation() != seen {
            return;
        }
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.generation() != seen {
            return;
        }
        notified.await;
    }
}

impl Default for StateSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    const SHORT: Duration = Duration::from_millis(20);
    const LONG: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn the_first_mark_wakes_a_waiting_consumer() {
        let sig = Arc::new(DataSignal::new());
        let s = sig.clone();
        let waiter = tokio::spawn(async move { timeout(LONG, s.ready()).await });
        tokio::task::yield_now().await;
        sig.mark();
        assert!(waiter.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn marks_before_the_wait_are_kept_as_one() {
        let sig = DataSignal::new();
        sig.mark();
        sig.mark();
        timeout(LONG, sig.ready()).await.expect("marked");
        assert!(
            timeout(SHORT, sig.ready()).await.is_err(),
            "no second wake without a new mark"
        );
    }

    #[tokio::test]
    async fn ready_stays_ready_after_a_dropped_waiter() {
        let sig = DataSignal::new();
        {
            let waiter = sig.ready();
            tokio::pin!(waiter);
            assert!(
                futures_poll_once(waiter.as_mut()).is_pending(),
                "idle before the mark"
            );
            sig.mark();
        }
        timeout(LONG, sig.ready()).await.expect("the mark is kept");
    }

    #[tokio::test]
    async fn a_state_change_reaches_waiters_before_and_after_they_wait() {
        let sig = Arc::new(StateSignal::new());
        let seen = sig.generation();
        sig.notify_changed();
        timeout(LONG, sig.changed_after(seen))
            .await
            .expect("a change before the wait counts");

        let seen = sig.generation();
        let waiters: Vec<_> = (0..3)
            .map(|_| {
                let s = sig.clone();
                tokio::spawn(async move { timeout(LONG, s.changed_after(seen)).await })
            })
            .collect();
        tokio::task::yield_now().await;
        sig.notify_changed();
        for w in waiters {
            assert!(w.await.unwrap().is_ok(), "every waiter wakes");
        }
    }

    #[tokio::test]
    async fn until_retries_after_each_change() {
        let sig = Arc::new(StateSignal::new());
        let tries = Arc::new(AtomicU64::new(0));
        let waiter = {
            let (sig, tries) = (sig.clone(), tries.clone());
            tokio::spawn(async move {
                sig.until(|| (tries.fetch_add(1, Ordering::SeqCst) == 2).then_some(()))
                    .await;
            })
        };
        for _ in 0..2 {
            tokio::task::yield_now().await;
            sig.notify_changed();
        }
        timeout(LONG, waiter).await.expect("done").unwrap();
        assert_eq!(tries.load(Ordering::SeqCst), 3);
    }

    fn futures_poll_once<F: Future>(f: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        f.poll(&mut cx)
    }
}

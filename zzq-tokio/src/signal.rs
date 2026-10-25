//! Wakes between tasks on different lanes, one per batch.
//!
//! Ported from omq.rs, which follows the libzmq ypipe discipline: a
//! producer wakes the consumer only when it may be idle, so a burst of
//! pushes costs one wake.
//!
//! [`DataSignal`] tells one consumer that a queue has items:
//!
//! - The **producer** calls [`DataSignal::mark`] after a push, or after the
//!   push that made the queue non-empty when a lock already orders pushes
//!   and drains. Only the idle-to-pending transition wakes; further marks
//!   coalesce.
//! - The **consumer** calls [`DataSignal::begin_drain`] before draining and
//!   [`DataSignal::clear_after`] after. A mark that races with the drain
//!   moves the signal to `DIRTY`, so a stale empty read still rearms it.
//! - A mark that finds the signal pending skips its wake, so its item must
//!   be visible to the consumer's next drain. That is a store-then-load on
//!   each side (item then state, state then item), so both sides need a
//!   sequentially consistent fence: `mark` before reading the state,
//!   `begin_drain` after writing it. Release and acquire alone let both
//!   loads read stale values, and the consumer parks with the item queued
//!   (`tests/loom_signal.rs`).
//!
//! [`StateSignal`] tells any number of waiters that something changed, such
//! as room in a full queue.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering, fence};

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

const IDLE: u8 = 0;
const PENDING: u8 = 1;
const DRAINING: u8 = 2;
const DIRTY: u8 = 3;

/// Coalesced "items available" for one consumer.
#[derive(Debug)]
pub struct DataSignal {
    state: AtomicU8,
    notify: Notify,
}

impl DataSignal {
    /// An idle signal.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: AtomicU8::new(IDLE),
            notify: new_notify(),
        }
    }

    /// Producer: items were pushed. Wakes the consumer only on the
    /// idle-to-pending transition.
    #[inline]
    pub fn mark(&self) {
        // Order the caller's push before the state read. See the module
        // docs.
        fence(Ordering::SeqCst);
        let state = self.state.load(Ordering::Acquire);
        if !matches!(state, PENDING | DIRTY) {
            self.mark_pending(state);
        }
    }

    fn mark_pending(&self, mut state: u8) {
        loop {
            match state {
                IDLE => match self.state.compare_exchange_weak(
                    IDLE,
                    PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.notify.notify_one();
                        return;
                    }
                    Err(next) => state = next,
                },
                PENDING | DIRTY => return,
                DRAINING => match self.state.compare_exchange_weak(
                    DRAINING,
                    DIRTY,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(next) => state = next,
                },
                _ => unreachable!("invalid DataSignal state"),
            }
        }
    }

    /// Consumer: about to drain.
    #[inline]
    pub fn begin_drain(&self) {
        if self.state.load(Ordering::Acquire) == PENDING {
            let _ =
                self.state
                    .compare_exchange(PENDING, DRAINING, Ordering::AcqRel, Ordering::Acquire);
        }
        // Order the state write before the drain's reads. See the module
        // docs.
        fence(Ordering::SeqCst);
    }

    /// Consumer: done draining. Rearms the signal if items remain, or if a
    /// producer marked during the drain. Returns whether it woke.
    #[inline]
    pub fn clear_after(&self, is_empty: bool) -> bool {
        if !is_empty {
            return self.rearm();
        }
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                DRAINING => match self.state.compare_exchange_weak(
                    DRAINING,
                    IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return false,
                    Err(next) => state = next,
                },
                DIRTY => return self.rearm(),
                PENDING | IDLE => return false,
                _ => unreachable!("invalid DataSignal state"),
            }
        }
    }

    #[inline]
    fn rearm(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                PENDING => return false,
                IDLE | DRAINING | DIRTY => match self.state.compare_exchange_weak(
                    state,
                    PENDING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.notify.notify_one();
                        return true;
                    }
                    Err(next) => state = next,
                },
                _ => unreachable!("invalid DataSignal state"),
            }
        }
    }

    /// Waits until items are marked.
    ///
    /// Stays ready after an earlier wait was woken and then dropped, as by
    /// a `select!` branch that lost.
    pub async fn ready(&self) {
        let notified = self.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if !self.is_idle() {
            return;
        }
        notified.await;
    }

    /// Whether nothing is marked.
    #[inline]
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == IDLE
    }
}

impl Default for DataSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// "Something changed" for any number of waiters.
///
/// Every change bumps a generation. A waiter reads the generation, checks
/// its condition, and then waits for a newer generation, so a change
/// between the check and the wait is not lost.
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

    /// The current generation.
    #[inline]
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    /// Bumps the generation and wakes every waiter.
    #[inline]
    pub fn notify_changed(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    /// Waits until the generation differs from `seen`.
    pub async fn changed_after(&self, seen: u64) {
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
    async fn marks_coalesce_until_the_consumer_drained() {
        let sig = Arc::new(DataSignal::new());
        sig.mark();
        sig.mark();
        timeout(LONG, sig.ready()).await.expect("marked");
        sig.begin_drain();
        assert!(!sig.clear_after(true));
        assert!(
            timeout(SHORT, sig.ready()).await.is_err(),
            "no wake after a drain without a new mark"
        );
    }

    #[tokio::test]
    async fn a_drain_that_left_items_rearms() {
        let sig = DataSignal::new();
        sig.mark();
        sig.begin_drain();
        assert!(sig.clear_after(false));
        timeout(LONG, sig.ready()).await.expect("rearmed");
    }

    #[tokio::test]
    async fn a_mark_during_the_drain_rearms_even_if_the_drain_saw_nothing() {
        let sig = DataSignal::new();
        sig.mark();
        sig.begin_drain();
        sig.mark();
        assert!(sig.clear_after(true));
        timeout(LONG, sig.ready())
            .await
            .expect("dirty drain stays ready");
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
        timeout(LONG, sig.ready()).await.expect("pending is kept");
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

    fn futures_poll_once<F: Future>(f: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        f.poll(&mut cx)
    }
}

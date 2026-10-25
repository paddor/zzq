//! Loom models of the `DataSignal` handoff in `src/signal.rs`.
//!
//! `DataSignal` waits on a Tokio `Notify`, which Loom cannot run, so the
//! state machine is mirrored here with Loom's atomics. Keep the transitions
//! and orderings in step with `src/signal.rs`.

#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU8, AtomicUsize, Ordering, fence};
use loom::thread;

/// A producer that publishes with a release store and then finds the
/// signal already pending skips its wake. The consumer must still see that
/// item before it parks, or the item waits for the next push.
#[test]
fn a_skipped_mark_cannot_strand_a_published_item() {
    loom::model(|| skipped_mark(true));
}

/// Without the fences the same handoff loses the item. This keeps the
/// model honest: it must be able to show the race it rules out.
#[test]
#[should_panic(expected = "consumer parked without a wake")]
fn without_fences_a_skipped_mark_strands_the_item() {
    loom::model(|| skipped_mark(false));
}

/// A push during a drain is either drained or leaves the signal pending.
#[test]
fn a_push_during_a_drain_is_not_lost() {
    loom::model(|| {
        let pushed = Arc::new(AtomicUsize::new(0));
        let signal = Arc::new(Model::new(IDLE, true));
        let producer = {
            let (pushed, signal) = (pushed.clone(), signal.clone());
            thread::spawn(move || {
                pushed.store(8, Ordering::Release);
                signal.mark();
            })
        };
        signal.begin_drain();
        let drained = pushed.load(Ordering::Acquire);
        signal.clear_after(true);
        producer.join().unwrap();
        assert!(drained == 8 || !signal.is_idle());
    });
}

fn skipped_mark(fenced: bool) {
    // One item is pushed and marked; the consumer has not drained yet.
    let tail = Arc::new(AtomicUsize::new(1));
    let signal = Arc::new(Model::new(PENDING, fenced));
    let producer = {
        let (tail, signal) = (tail.clone(), signal.clone());
        thread::spawn(move || {
            tail.store(2, Ordering::Release);
            signal.mark();
        })
    };
    // The consumer drains until the signal lets it park.
    let mut consumed = 0;
    for _ in 0..3 {
        signal.begin_drain();
        consumed = tail.load(Ordering::Acquire);
        let is_empty = tail.load(Ordering::Acquire) == consumed;
        signal.clear_after(is_empty);
        if signal.is_idle() {
            break;
        }
    }
    let parked = signal.is_idle();
    producer.join().unwrap();
    // A parked consumer without a pending wake must have seen every item.
    assert!(
        !(parked && signal.is_idle() && consumed < 2),
        "item 2 is pushed but the consumer parked without a wake"
    );
}

const IDLE: u8 = 0;
const PENDING: u8 = 1;
const DRAINING: u8 = 2;
const DIRTY: u8 = 3;

/// `DataSignal` with the atomics and orderings of `src/signal.rs`, without
/// the `Notify`.
struct Model {
    state: AtomicU8,
    /// `true` models the real code; `false` drops the sequentially
    /// consistent fences, which can strand an item.
    fenced: bool,
}

impl Model {
    fn new(state: u8, fenced: bool) -> Self {
        Self {
            state: AtomicU8::new(state),
            fenced,
        }
    }

    fn mark(&self) {
        if self.fenced {
            fence(Ordering::SeqCst);
        }
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            let next = match state {
                IDLE => PENDING,
                DRAINING => DIRTY,
                PENDING | DIRTY => return,
                _ => unreachable!("invalid state"),
            };
            match self
                .state
                .compare_exchange(state, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(now) => state = now,
            }
        }
    }

    fn begin_drain(&self) {
        if self.state.load(Ordering::Acquire) == PENDING {
            let _ =
                self.state
                    .compare_exchange(PENDING, DRAINING, Ordering::AcqRel, Ordering::Acquire);
        }
        if self.fenced {
            fence(Ordering::SeqCst);
        }
    }

    fn clear_after(&self, is_empty: bool) {
        if !is_empty {
            self.rearm();
            return;
        }
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            match state {
                DRAINING => match self.state.compare_exchange(
                    DRAINING,
                    IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return,
                    Err(now) => state = now,
                },
                DIRTY => {
                    self.rearm();
                    return;
                }
                PENDING | IDLE => return,
                _ => unreachable!("invalid state"),
            }
        }
    }

    fn rearm(&self) {
        let mut state = self.state.load(Ordering::Acquire);
        while state != PENDING {
            match self
                .state
                .compare_exchange(state, PENDING, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(now) => state = now,
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.state.load(Ordering::Acquire) == IDLE
    }
}

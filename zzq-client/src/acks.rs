//! Acks for QoS 1/2 publishes, from the session task to the waiting calls.
//!
//! Waking a task on another thread goes through its runtime, which costs a
//! current-thread runtime a syscall per wake. So the session wakes one of
//! the calls a read finished, and that call wakes the others from its own
//! thread. Should it not run soon, the session wakes them itself.

use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::{Error, Result};

/// Slots for the calls waiting for an ack.
#[derive(Default)]
pub(crate) struct Acks {
    state: Mutex<State>,
}

impl std::fmt::Debug for Acks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acks").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct State {
    slots: Vec<Slot>,
    free: Vec<usize>,
    /// Wakers of finished calls, for the next call that runs to wake.
    chained: Vec<Waker>,
}

#[derive(Default)]
enum Slot {
    #[default]
    Free,
    Waiting(Option<Waker>),
    Done(Result<()>),
    /// The call is gone; the session frees the slot once it is done.
    Abandoned,
}

impl Acks {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Finishes the calls in `done`, which it empties. Wakes one and
    /// returns whether it left others for that one to wake.
    pub(crate) fn finish(&self, done: &mut Vec<(usize, Result<()>)>) -> bool {
        let mut first = None;
        let mut st = self.lock();
        for (slot, res) in done.drain(..) {
            let Some(s) = st.slots.get_mut(slot) else {
                continue;
            };
            match std::mem::take(s) {
                Slot::Waiting(w) => {
                    *s = Slot::Done(res);
                    if let Some(w) = w {
                        if first.is_none() {
                            first = Some(w);
                        } else {
                            st.chained.push(w);
                        }
                    }
                }
                Slot::Abandoned => st.free.push(slot),
                other => *s = other,
            }
        }
        let chained = !st.chained.is_empty();
        drop(st);
        if let Some(w) = first {
            w.wake();
        }
        chained
    }

    /// Wakes the finished calls no call has woken yet.
    pub(crate) fn wake_chained(&self) {
        let chained = std::mem::take(&mut self.lock().chained);
        for w in chained {
            w.wake();
        }
    }
}

/// Waits for the ack of one queued publish.
#[derive(Debug)]
pub(crate) struct AckWait {
    acks: Arc<Acks>,
    slot: usize,
    /// The session has the slot, so only it may free it.
    queued: bool,
    done: bool,
}

impl AckWait {
    /// A new slot.
    pub(crate) fn new(acks: &Arc<Acks>) -> AckWait {
        let mut st = acks.lock();
        let slot = if let Some(slot) = st.free.pop() {
            st.slots[slot] = Slot::Waiting(None);
            slot
        } else {
            st.slots.push(Slot::Waiting(None));
            st.slots.len() - 1
        };
        AckWait {
            acks: acks.clone(),
            slot,
            queued: false,
            done: false,
        }
    }

    pub(crate) const fn slot(&self) -> usize {
        self.slot
    }

    /// The session got the slot with its publish.
    pub(crate) const fn queued(&mut self) {
        self.queued = true;
    }
}

impl Future for AckWait {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(Err(Error::Closed));
        }
        let mut st = this.acks.lock();
        let chained = std::mem::take(&mut st.chained);
        let res = match std::mem::take(&mut st.slots[this.slot]) {
            Slot::Done(res) => {
                st.free.push(this.slot);
                this.done = true;
                Poll::Ready(res)
            }
            Slot::Waiting(w) => {
                let w = match w {
                    Some(w) if w.will_wake(cx.waker()) => w,
                    _ => cx.waker().clone(),
                };
                st.slots[this.slot] = Slot::Waiting(Some(w));
                Poll::Pending
            }
            other => {
                st.slots[this.slot] = other;
                this.done = true;
                Poll::Ready(Err(Error::Closed))
            }
        };
        drop(st);
        for w in chained {
            w.wake();
        }
        res
    }
}

impl Drop for AckWait {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut st = self.acks.lock();
        let s = &mut st.slots[self.slot];
        if self.queued && matches!(s, Slot::Waiting(_)) {
            *s = Slot::Abandoned;
        } else {
            *s = Slot::Free;
            st.free.push(self.slot);
        }
        // This call may have been the one to wake the others.
        let chained = std::mem::take(&mut st.chained);
        drop(st);
        for w in chained {
            w.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use super::*;

    #[derive(Default)]
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn poll(w: &mut AckWait, count: &Arc<Count>) -> Poll<Result<()>> {
        let waker = Waker::from(count.clone());
        Pin::new(w).poll(&mut Context::from_waker(&waker))
    }

    #[test]
    fn one_call_is_woken_and_wakes_the_rest() {
        let acks = Arc::new(Acks::default());
        let counts: Vec<Arc<Count>> = (0..3).map(|_| Arc::default()).collect();
        let mut waits: Vec<AckWait> = (0..3).map(|_| AckWait::new(&acks)).collect();
        for (w, c) in waits.iter_mut().zip(&counts) {
            w.queued();
            assert!(poll(w, c).is_pending());
        }
        let mut done = waits.iter().map(|w| (w.slot(), Ok(()))).collect();
        assert!(acks.finish(&mut done), "two left for the first to wake");
        let woken: Vec<usize> = counts.iter().map(|c| c.0.load(Ordering::Relaxed)).collect();
        assert_eq!(woken, [1, 0, 0]);

        assert!(matches!(
            poll(&mut waits[0], &counts[0]),
            Poll::Ready(Ok(()))
        ));
        let woken: Vec<usize> = counts.iter().map(|c| c.0.load(Ordering::Relaxed)).collect();
        assert_eq!(woken, [1, 1, 1]);
        assert!(matches!(
            poll(&mut waits[1], &counts[1]),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn a_dropped_call_wakes_the_rest_and_frees_its_slot_once_done() {
        let acks = Arc::new(Acks::default());
        let counts: Vec<Arc<Count>> = (0..2).map(|_| Arc::default()).collect();
        let mut a = AckWait::new(&acks);
        let mut b = AckWait::new(&acks);
        for (w, c) in [&mut a, &mut b].into_iter().zip(&counts) {
            w.queued();
            assert!(poll(w, c).is_pending());
        }
        let mut done = vec![(a.slot(), Ok(())), (b.slot(), Ok(()))];
        acks.finish(&mut done);
        drop(a);
        assert_eq!(counts[1].0.load(Ordering::Relaxed), 1);
        assert!(matches!(poll(&mut b, &counts[1]), Poll::Ready(Ok(()))));
        assert_eq!(acks.lock().free.len(), 2);

        // Dropped before its ack: the slot stays taken until then.
        let mut c = AckWait::new(&acks);
        c.queued();
        let slot = c.slot();
        drop(c);
        assert!(matches!(acks.lock().slots[slot], Slot::Abandoned));
        acks.finish(&mut vec![(slot, Ok(()))]);
        assert_eq!(acks.lock().free.len(), 2);
    }
}

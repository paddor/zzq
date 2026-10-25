//! Work for a lane from other lanes and the broker handle.
//!
//! Senders queue under a lock and wake the lane through a [`DataSignal`]
//! only when the queue was empty, so a burst costs one wake, and the lane
//! takes the whole queue at once. Senders to a full mailbox wait for room
//! through a [`StateSignal`], which slows them down instead of dropping.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use zzq_tokio::signal::{DataSignal, StateSignal};

/// Keeps a value on cache lines of its own. x86 fetches lines in pairs.
#[repr(align(128))]
struct Line<T>(T);

impl<T> std::ops::Deref for Line<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

/// The lock and the wake, which both lanes touch for every batch, each get
/// their own lines.
struct Shared<T> {
    state: Line<Mutex<State<T>>>,
    /// For the lane: items queued, or the mailbox closed.
    ready: Line<DataSignal>,
    /// For waiting senders: the lane took a full queue, or it closed.
    room: StateSignal,
    cap: usize,
}

struct State<T> {
    items: Vec<T>,
    /// A sender waits for room.
    full: bool,
    closed: bool,
}

impl<T> Shared<T> {
    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Refuses further items and wakes everyone.
    fn close(&self) {
        self.lock().closed = true;
        self.ready.mark();
        self.room.notify_changed();
    }
}

/// A mailbox that makes [`Mailbox::send`] wait while `cap` items are
/// queued.
pub(crate) fn mailbox<T>(cap: usize) -> (Mailbox<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        state: Line(Mutex::new(State {
            items: Vec::new(),
            full: false,
            closed: false,
        })),
        ready: Line(DataSignal::new()),
        room: StateSignal::new(),
        cap: cap.max(1),
    });
    (
        Mailbox {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

/// The sending side, for any lane.
pub(crate) struct Mailbox<T> {
    shared: Arc<Shared<T>>,
}

impl<T> std::fmt::Debug for Mailbox<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mailbox").finish_non_exhaustive()
    }
}

impl<T> Mailbox<T> {
    /// Queues `item` without waiting, beyond the cap too. Hands it back
    /// once the mailbox is closed.
    pub(crate) fn push(&self, item: T) -> Result<(), T> {
        let mut st = self.shared.lock();
        if st.closed {
            return Err(item);
        }
        let wake = st.items.is_empty();
        st.items.push(item);
        drop(st);
        // The lock orders pushes and takes, so only the push into an empty
        // queue needs to wake the lane.
        if wake {
            self.shared.ready.mark();
        }
        Ok(())
    }

    /// Queues `item`, waiting while the mailbox is full. Hands it back once
    /// the mailbox is closed.
    pub(crate) async fn send(&self, item: T) -> Result<(), T> {
        let mut item = Some(item);
        self.shared
            .room
            .until(|| {
                let mut st = self.shared.lock();
                if st.closed {
                    return item.take().map(Err);
                }
                if st.items.len() < self.shared.cap {
                    let wake = st.items.is_empty();
                    st.items.extend(item.take());
                    drop(st);
                    if wake {
                        self.shared.ready.mark();
                    }
                    return Some(Ok(()));
                }
                st.full = true;
                None
            })
            .await
    }

    /// Refuses further items. The lane still gets those queued.
    pub(crate) fn close(&self) {
        self.shared.close();
    }
}

/// The lane's side. Dropping it closes the mailbox and drops what is
/// queued, so callers waiting for an answer in an item learn it never
/// comes.
pub(crate) struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl<T> Receiver<T> {
    /// Moves everything queued into `out`, which should be empty, waiting
    /// until there is something. False once the mailbox is closed and
    /// empty.
    pub(crate) async fn recv_all(&self, out: &mut Vec<T>) -> bool {
        loop {
            let (full, closed) = {
                let mut st = self.shared.lock();
                // Swapping hands the emptied vector back for reuse.
                std::mem::swap(&mut st.items, out);
                (std::mem::take(&mut st.full), st.closed)
            };
            if full {
                self.shared.room.notify_changed();
            }
            if !out.is_empty() {
                return true;
            }
            if closed {
                return false;
            }
            self.shared.ready.ready().await;
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let left = {
            let mut st = self.shared.lock();
            st.closed = true;
            std::mem::take(&mut st.items)
        };
        drop(left);
        self.shared.close();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn the_lane_takes_a_burst_at_once_and_senders_wait_for_room() {
        let (tx, rx) = mailbox::<u32>(2);
        let tx = Arc::new(tx);
        for i in 0..2 {
            assert!(tx.send(i).await.is_ok());
        }
        let t = tx.clone();
        let waiting = tokio::spawn(async move { t.send(2).await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished(), "full");
        assert!(tx.push(3).is_ok(), "push ignores the cap");

        let mut got = Vec::new();
        assert!(rx.recv_all(&mut got).await);
        assert_eq!(got, [0, 1, 3]);
        let sent = tokio::time::timeout(Duration::from_secs(1), waiting).await;
        assert!(matches!(sent, Ok(Ok(Ok(())))), "room wakes the sender");
        got.clear();
        assert!(rx.recv_all(&mut got).await);
        assert_eq!(got, [2]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn closing_drains_what_is_queued_and_dropping_the_lane_side_refuses_more() {
        let (tx, rx) = mailbox::<u32>(4);
        assert!(tx.push(1).is_ok());
        tx.close();
        assert_eq!(tx.push(2), Err(2));
        let mut got = Vec::new();
        assert!(rx.recv_all(&mut got).await);
        got.clear();
        assert!(!rx.recv_all(&mut got).await, "closed and empty");

        let (tx, rx) = mailbox::<std::sync::Arc<()>>(4);
        let item = std::sync::Arc::new(());
        assert!(tx.push(item.clone()).is_ok());
        drop(rx);
        assert_eq!(
            std::sync::Arc::strong_count(&item),
            1,
            "queued items dropped"
        );
        assert!(tx.send(item).await.is_err());
    }
}

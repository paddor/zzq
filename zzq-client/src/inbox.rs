//! Received messages, from the session task to the client handle.
//!
//! Messages cross in batches, so each side takes the lock and wakes the
//! other once per batch rather than once per message: the session queues
//! what one read decoded, and the handle takes the whole queue. Acks for
//! taken QoS 1/2 messages go back the same way.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;
use zzq_proto::QoS;

use crate::Message;

struct Shared {
    state: Mutex<State>,
    /// Wakes the handle once the queue is no longer empty or the session
    /// ends.
    ready: Notify,
    /// Wakes the session once the handle took the queue or is gone.
    room: Notify,
    /// Wakes the session once there are acks to send.
    acked: Notify,
    cap: usize,
}

#[derive(Default)]
struct State {
    queue: VecDeque<Message>,
    /// Packet IDs of taken QoS 1/2 messages, to ack.
    acks: Vec<(u16, QoS)>,
    /// The session waits for room.
    waiting: bool,
    /// The session is gone.
    closed: bool,
    /// The handle is gone.
    dropped: bool,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A queue for at most `cap` messages. The handle holds the batch it took
/// apart from them.
pub(crate) fn channel(cap: usize) -> (Sender, Receiver) {
    let shared = Arc::new(Shared {
        state: Mutex::default(),
        ready: Notify::new(),
        room: Notify::new(),
        acked: Notify::new(),
        cap: cap.max(1),
    });
    let tx = Sender {
        shared: shared.clone(),
        batch: Vec::new(),
    };
    let rx = Receiver {
        shared,
        batch: VecDeque::new(),
    };
    (tx, rx)
}

/// The session side.
pub(crate) struct Sender {
    shared: Arc<Shared>,
    batch: Vec<Message>,
}

impl std::fmt::Debug for Sender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender")
            .field("batch", &self.batch.len())
            .finish_non_exhaustive()
    }
}

/// The handle is gone.
pub(crate) struct Dropped;

impl Sender {
    /// Adds a message to the next batch.
    pub(crate) fn push(&mut self, m: Message) {
        self.batch.push(m);
    }

    /// Whether the next batch fills the queue.
    pub(crate) fn full(&self) -> bool {
        self.batch.len() >= self.shared.cap
    }

    /// Queues the batch, waiting for room as needed.
    pub(crate) async fn flush(&mut self) -> Result<(), Dropped> {
        while !self.batch.is_empty() {
            {
                let mut st = self.shared.lock();
                if st.dropped {
                    self.batch.clear();
                    return Err(Dropped);
                }
                let n = self
                    .shared
                    .cap
                    .saturating_sub(st.queue.len())
                    .min(self.batch.len());
                if n > 0 {
                    let wake = st.queue.is_empty();
                    st.queue.extend(self.batch.drain(..n));
                    if wake {
                        self.shared.ready.notify_one();
                    }
                }
                st.waiting = !self.batch.is_empty();
                if !st.waiting {
                    return Ok(());
                }
            }
            self.shared.room.notified().await;
        }
        Ok(())
    }

    /// Waits until there may be acks to send.
    pub(crate) async fn acked(&self) {
        self.shared.acked.notified().await;
    }

    /// Swaps the acks to send into `acks`, which should be empty.
    pub(crate) fn take_acks(&self, acks: &mut Vec<(u16, QoS)>) {
        std::mem::swap(&mut self.shared.lock().acks, acks);
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.shared.lock().closed = true;
        self.shared.ready.notify_one();
    }
}

/// The handle side.
pub(crate) struct Receiver {
    shared: Arc<Shared>,
    batch: VecDeque<Message>,
}

impl std::fmt::Debug for Receiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver")
            .field("batch", &self.batch.len())
            .finish_non_exhaustive()
    }
}

impl Receiver {
    /// Next message. `None` once the session is gone and the queue empty.
    /// Taking a QoS 1/2 message acks it.
    pub(crate) async fn recv(&mut self) -> Option<Message> {
        loop {
            if let Some(m) = self.batch.pop_front() {
                if let Some(pkid) = m.ack {
                    let mut st = self.shared.lock();
                    if st.acks.is_empty() {
                        self.shared.acked.notify_one();
                    }
                    st.acks.push((pkid, m.qos));
                }
                return Some(m);
            }
            {
                let mut st = self.shared.lock();
                if !st.queue.is_empty() {
                    std::mem::swap(&mut st.queue, &mut self.batch);
                    if std::mem::take(&mut st.waiting) {
                        self.shared.room.notify_one();
                    }
                    continue;
                }
                if st.closed {
                    return None;
                }
            }
            self.shared.ready.notified().await;
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.shared.lock().dropped = true;
        self.shared.room.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use zzq_proto::QoS;

    use super::*;

    fn msg(i: usize) -> Message {
        Message {
            topic: i.to_string(),
            payload: Bytes::new(),
            qos: QoS::AtMostOnce,
            retain: false,
            subscription_ids: Vec::new(),
            ack: None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batches_larger_than_the_queue_wait_and_keep_order() {
        let (mut tx, mut rx) = channel(3);
        let send = tokio::spawn(async move {
            for i in 0..10 {
                tx.push(msg(i));
            }
            assert!(tx.full());
            assert!(tx.flush().await.is_ok());
        });
        for i in 0..10 {
            assert_eq!(rx.recv().await.unwrap().topic, i.to_string());
        }
        send.await.unwrap();
        assert!(rx.recv().await.is_none(), "closed once the sender is gone");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_fails_once_the_receiver_is_gone() {
        let (mut tx, rx) = channel(1);
        drop(rx);
        tx.push(msg(0));
        assert!(tx.flush().await.is_err());
    }
}

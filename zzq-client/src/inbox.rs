//! Received messages, from the session task to the client handle.
//!
//! Messages cross in batches, so each side takes the lock and wakes the
//! other once per batch rather than once per message: the session queues
//! what one read decoded, and the handle takes the whole queue. Acks for
//! taken QoS 1/2 messages go back the same way. Each direction wakes through
//! a [`DataSignal`].
//!
//! A large read goes over in parts, so the handle starts, and its acks
//! flow, before the session decoded all of it. The handle wakes the session
//! for acks only once [`ACK_LWM`] gathered or it ran out of messages, and
//! the session takes them whenever it handles a read anyway.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use zzq_proto::QoS;
use zzq_tokio::signal::DataSignal;

use crate::Message;

/// Messages needing an ack that the session hands over before a read is
/// fully decoded.
const HANDOFF_ACKS: usize = 32;
/// Payload bytes the session hands over before a read is fully decoded.
const HANDOFF_BYTES: usize = 16 << 10;
/// Acks the handle gathers before it wakes the session for them.
const ACK_LWM: usize = 16;

struct Shared {
    state: Mutex<State>,
    /// For the handle: messages queued, or the session ended.
    ready: DataSignal,
    /// For the session: the handle took a full queue or is gone.
    room: DataSignal,
    /// For the session: [`ACK_LWM`] acks gathered, or any once the handle
    /// ran out of messages or is gone.
    acked: DataSignal,
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
        ready: DataSignal::new(),
        room: DataSignal::new(),
        acked: DataSignal::new(),
        cap: cap.max(1),
    });
    let tx = Sender {
        shared: shared.clone(),
        batch: Vec::new(),
        batch_acks: 0,
        batch_bytes: 0,
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
    /// Messages in `batch` that need an ack, and their payload bytes.
    batch_acks: usize,
    batch_bytes: usize,
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
        self.batch_acks += usize::from(m.ack.is_some());
        self.batch_bytes += m.payload.len();
        self.batch.push(m);
    }

    /// Whether to queue the next batch before the read is decoded.
    pub(crate) fn hand_off(&self) -> bool {
        self.full() || self.batch_acks >= HANDOFF_ACKS || self.batch_bytes >= HANDOFF_BYTES
    }

    /// Whether the next batch fills the queue.
    pub(crate) fn full(&self) -> bool {
        self.batch.len() >= self.shared.cap
    }

    /// Queues the batch, waiting for room as needed.
    pub(crate) async fn flush(&mut self) -> Result<(), Dropped> {
        while !self.batch.is_empty() {
            let (filled, waiting) = {
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
                let filled = n > 0 && st.queue.is_empty();
                if n > 0 {
                    st.queue.extend(self.batch.drain(..n));
                    self.batch_acks = self.batch.iter().filter(|m| m.ack.is_some()).count();
                    self.batch_bytes = self.batch.iter().map(|m| m.payload.len()).sum();
                }
                st.waiting = !self.batch.is_empty();
                (filled, st.waiting)
            };
            // The lock orders pushes and takes, so only the push into an
            // empty queue needs to wake the handle.
            if filled {
                self.shared.ready.mark();
            }
            if !waiting {
                return Ok(());
            }
            self.shared.room.ready().await;
        }
        Ok(())
    }

    /// Waits until there may be acks to send.
    pub(crate) async fn acked(&self) {
        self.shared.acked.ready().await;
    }

    /// Swaps the acks to send into `acks`, which should be empty.
    pub(crate) fn take_acks(&self, acks: &mut Vec<(u16, QoS)>) {
        std::mem::swap(&mut self.shared.lock().acks, acks);
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.shared.lock().closed = true;
        self.shared.ready.mark();
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
    /// Taking a QoS 1/2 message acks it, at the latest once the handle took
    /// [`ACK_LWM`] more or ran out.
    pub(crate) async fn recv(&mut self) -> Option<Message> {
        loop {
            if let Some(m) = self.batch.pop_front() {
                if let Some(pkid) = m.ack {
                    let gathered = {
                        let mut st = self.shared.lock();
                        st.acks.push((pkid, m.qos));
                        st.acks.len()
                    };
                    // Later acks coalesce into the pending wake.
                    if gathered == ACK_LWM || self.batch.is_empty() {
                        self.shared.acked.mark();
                    }
                }
                return Some(m);
            }
            let (room, closed) = {
                let mut st = self.shared.lock();
                std::mem::swap(&mut st.queue, &mut self.batch);
                let room = !self.batch.is_empty() && std::mem::take(&mut st.waiting);
                (room, st.closed)
            };
            if room {
                self.shared.room.mark();
            }
            if !self.batch.is_empty() {
                continue;
            }
            if closed {
                return None;
            }
            self.shared.ready.ready().await;
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let acks = {
            let mut st = self.shared.lock();
            st.dropped = true;
            !st.acks.is_empty()
        };
        if acks {
            self.shared.acked.mark();
        }
        self.shared.room.mark();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use zzq_proto::QoS;

    use super::*;

    fn msg(i: usize) -> Message {
        Message {
            topic: i.to_string().into(),
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

    /// Whether the session would wake for acks now.
    fn acked(tx: &Sender) -> bool {
        let wait = std::pin::pin!(tx.acked());
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        wait.poll(&mut cx).is_ready()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn acks_wake_the_session_at_the_lwm_and_once_the_handle_ran_out() {
        let (mut tx, mut rx) = channel(100);
        for i in 0..ACK_LWM + 4 {
            tx.push(Message {
                qos: QoS::AtLeastOnce,
                ack: Some(i as u16 + 1),
                ..msg(i)
            });
        }
        assert!(tx.flush().await.is_ok());
        let mut ids = Vec::new();

        for _ in 1..ACK_LWM {
            rx.recv().await.unwrap();
        }
        assert!(!acked(&tx), "fewer than the LWM wait");
        rx.recv().await.unwrap();
        assert!(acked(&tx));
        tx.take_acks(&mut ids);
        assert_eq!(ids.len(), ACK_LWM);

        ids.clear();
        for _ in 0..3 {
            rx.recv().await.unwrap();
        }
        assert!(!acked(&tx), "the handle has one more at hand");
        rx.recv().await.unwrap();
        assert!(acked(&tx), "the handle ran out");
        tx.take_acks(&mut ids);
        assert_eq!(ids, [17, 18, 19, 20].map(|id| (id, QoS::AtLeastOnce)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn flush_fails_once_the_receiver_is_gone() {
        let (mut tx, rx) = channel(1);
        drop(rx);
        tx.push(msg(0));
        assert!(tx.flush().await.is_err());
    }
}

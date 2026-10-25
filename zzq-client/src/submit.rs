//! Requests, from the client handle to the session task.
//!
//! The handle encodes publishes itself, into a buffer it shares with the
//! session, and queues other requests between them in order. The session
//! takes the whole queue at once, so a burst of publishes costs a lock per
//! message but a wake per batch, through a [`DataSignal`]. Publishes wait
//! while the queue holds [`QUEUED`] bytes.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::oneshot;
use zzq_proto::QoS;
use zzq_proto::encode::{self, PublishHeader};
use zzq_tokio::signal::{DataSignal, StateSignal};

use crate::{Cmd, Error, Result};

/// Encoded publishes the queue takes before publishing waits.
pub(crate) const QUEUED: usize = 1 << 20;

/// Spare buffers larger than this are not kept.
const KEEP: usize = 4 << 20;

/// A queued request.
#[derive(Debug)]
pub(crate) enum Op {
    /// The next `len` bytes of [`Batch::bytes`]: PUBLISH packets, with
    /// packet ID 0 until sent. QoS 0 publishes in a row share one op.
    Publish {
        len: usize,
        qos: QoS,
        slot: usize,
    },
    Cmd(Cmd),
}

/// Queued requests.
#[derive(Debug, Default)]
pub(crate) struct Batch {
    pub(crate) ops: VecDeque<Op>,
    pub(crate) bytes: Vec<u8>,
}

impl Batch {
    /// Empties the batch for reuse.
    pub(crate) fn clear(&mut self) {
        self.ops.clear();
        if self.bytes.capacity() > KEEP {
            self.bytes = Vec::new();
        }
        self.bytes.clear();
    }
}

struct Shared {
    state: Mutex<State>,
    /// For the session: requests queued, or the handle is gone.
    ready: DataSignal,
    /// For waiting publishes: the session took a full queue or is gone.
    room: StateSignal,
}

#[derive(Default)]
struct State {
    batch: Batch,
    /// The session is gone.
    ended: bool,
    /// The handle is gone.
    dropped: bool,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub(crate) fn channel() -> (Sender, Receiver) {
    let shared = Arc::new(Shared {
        state: Mutex::default(),
        ready: DataSignal::new(),
        room: StateSignal::new(),
    });
    (
        Sender {
            shared: shared.clone(),
        },
        Receiver { shared },
    )
}

/// The handle side.
pub(crate) struct Sender {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Sender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl Sender {
    /// Queues a publish, waiting while the queue is full. `slot` is the
    /// ack slot of a QoS 1/2 publish.
    pub(crate) async fn publish(
        &self,
        h: &PublishHeader<'_>,
        payload: &[u8],
        slot: usize,
    ) -> Result<()> {
        self.shared
            .room
            .until(|| match self.try_publish(h, payload, slot) {
                Ok(false) => None,
                done => Some(done.map(drop)),
            })
            .await
    }

    /// Queues a publish unless the queue is full.
    pub(crate) fn try_publish(
        &self,
        h: &PublishHeader<'_>,
        payload: &[u8],
        slot: usize,
    ) -> Result<bool> {
        let mut st = self.shared.lock();
        if st.ended {
            return Err(Error::Closed);
        }
        if st.batch.bytes.len() >= QUEUED {
            return Ok(false);
        }
        let wake = st.batch.ops.is_empty();
        let start = st.batch.bytes.len();
        encode::publish(h, payload, &mut st.batch.bytes);
        let n = st.batch.bytes.len() - start;
        match st.batch.ops.back_mut() {
            Some(Op::Publish { len, qos, .. })
                if *qos == QoS::AtMostOnce && h.qos == QoS::AtMostOnce =>
            {
                *len += n;
            }
            _ => st.batch.ops.push_back(Op::Publish {
                len: n,
                qos: h.qos,
                slot,
            }),
        }
        drop(st);
        // The lock orders pushes and takes, so only the push into an empty
        // queue needs to wake the session.
        if wake {
            self.shared.ready.mark();
        }
        Ok(true)
    }

    /// Queues a request other than a publish.
    pub(crate) fn send(&self, cmd: Cmd) -> Result<()> {
        let mut st = self.shared.lock();
        if st.ended {
            return Err(Error::Closed);
        }
        // An offline session leaves requests queued, but looks for this one.
        let wake = st.batch.ops.is_empty() || matches!(cmd, Cmd::Disconnect { .. });
        st.batch.ops.push_back(Op::Cmd(cmd));
        drop(st);
        if wake {
            self.shared.ready.mark();
        }
        Ok(())
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.shared.lock().dropped = true;
        self.shared.ready.mark();
    }
}

/// The session side.
pub(crate) struct Receiver {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Receiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

impl Receiver {
    /// Waits until there may be requests, or the handle may be gone.
    pub(crate) async fn ready(&self) {
        self.shared.ready.ready().await;
    }

    /// Swaps the queued requests into `batch`, which should be empty.
    /// Returns whether the handle is gone, so no more will come.
    pub(crate) fn take(&self, batch: &mut Batch) -> bool {
        let mut st = self.shared.lock();
        std::mem::swap(&mut st.batch, batch);
        let dropped = st.dropped;
        drop(st);
        if batch.bytes.len() >= QUEUED {
            self.shared.room.notify_changed();
        }
        dropped
    }

    /// Whether the handle is gone.
    pub(crate) fn dropped(&self) -> bool {
        self.shared.lock().dropped
    }

    /// Takes a queued DISCONNECT out of the queue. The other requests stay
    /// for the next connection and wake no one until it takes them.
    pub(crate) fn take_disconnect(&self) -> Option<oneshot::Sender<()>> {
        Self::remove_disconnect(&mut self.shared.lock())
    }

    fn remove_disconnect(st: &mut State) -> Option<oneshot::Sender<()>> {
        let ops = &mut st.batch.ops;
        let at = ops
            .iter()
            .position(|op| matches!(op, Op::Cmd(Cmd::Disconnect { .. })))?;
        match ops.remove(at) {
            Some(Op::Cmd(Cmd::Disconnect { done, .. })) => Some(done),
            _ => None,
        }
    }

    /// Refuses further requests and returns those still queued.
    pub(crate) fn close(&self) -> Batch {
        let mut st = self.shared.lock();
        st.ended = true;
        let batch = std::mem::take(&mut st.batch);
        drop(st);
        self.shared.room.notify_changed();
        batch
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use zzq_proto::Version;

    use super::*;

    fn header(qos: QoS) -> PublishHeader<'static> {
        PublishHeader {
            version: Version::V5,
            dup: false,
            qos,
            retain: false,
            topic: "t",
            pkid: 0,
            properties: &[],
        }
    }

    #[test]
    fn qos0_publishes_in_a_row_share_an_op_and_a_full_queue_waits() {
        let (tx, rx) = channel();
        let mut cx = Context::from_waker(Waker::noop());
        let (q0, q1) = (header(QoS::AtMostOnce), header(QoS::AtLeastOnce));
        let payload = vec![0; QUEUED / 2];
        for _ in 0..2 {
            let f = pin!(tx.publish(&q0, &payload, 0));
            assert!(f.poll(&mut cx).is_ready());
        }
        let mut full = pin!(tx.publish(&q1, b"x", 7));
        assert!(full.as_mut().poll(&mut cx).is_pending());

        let mut batch = Batch::default();
        assert!(!rx.take(&mut batch));
        assert_eq!(batch.ops.len(), 1, "one op for both");
        assert!(matches!(full.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
        batch.clear();
        rx.take(&mut batch);
        assert!(matches!(
            batch.ops.front(),
            Some(Op::Publish {
                qos: QoS::AtLeastOnce,
                slot: 7,
                ..
            })
        ));
    }

    #[test]
    fn a_waiting_publish_fails_once_the_session_is_gone() {
        let (tx, rx) = channel();
        let mut cx = Context::from_waker(Waker::noop());
        let q0 = header(QoS::AtMostOnce);
        let payload = vec![0; QUEUED];
        let f = pin!(tx.publish(&q0, &payload, 0));
        assert!(f.poll(&mut cx).is_ready());
        let mut waiting = pin!(tx.publish(&q0, b"x", 0));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        drop(rx.close());
        assert!(matches!(
            waiting.poll(&mut cx),
            Poll::Ready(Err(Error::Closed))
        ));
    }
}

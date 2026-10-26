//! The connection's bytes: whole packets in the read buffer, and the
//! writer task.

use std::cell::RefCell;
use std::rc::Rc;

use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{Notify, oneshot};
use zzq_proto::packet::fixed_header;

use crate::{Error, Result};

use super::{MAX_PACKET, READ_BUF, READ_MIN};

/// Output past which the session takes no new requests until the writer
/// catches up.
const OUT_ROOM: usize = 1 << 20;

/// Output shared with the writer task.
#[derive(Debug, Default)]
pub(super) struct Out {
    pub(super) buf: RefCell<Vec<u8>>,
    pub(super) wake: Notify,
    pub(super) closed: std::cell::Cell<bool>,
    /// Answered once DISCONNECT is written.
    pub(super) bye: RefCell<Option<oneshot::Sender<()>>>,
    /// Wakes the session once the writer took full output.
    pub(super) drained: Notify,
    /// The writer failed.
    pub(super) broken: std::cell::Cell<bool>,
}

impl Out {
    /// Appends output. The writer was woken already if there was some.
    pub(super) fn push(&self, f: impl FnOnce(&mut Vec<u8>)) {
        let mut buf = self.buf.borrow_mut();
        let wake = buf.is_empty();
        f(&mut buf);
        if wake {
            self.wake.notify_one();
        }
    }

    /// Whether the session may take more requests.
    pub(super) fn has_room(&self) -> bool {
        self.buf.borrow().len() < OUT_ROOM
    }

    pub(super) fn close(&self) {
        self.closed.set(true);
        self.wake.notify_one();
    }
}

/// Length of the whole packets at the front of `buf`.
pub(super) fn complete_len(buf: &[u8]) -> Result<usize> {
    let mut at = 0;
    loop {
        match fixed_header(&buf[at..]) {
            Ok(Some(h)) if h.packet_len() > MAX_PACKET => {
                return Err(Error::Protocol("malformed packet"));
            }
            Ok(Some(h)) if at + h.packet_len() <= buf.len() => at += h.packet_len(),
            Ok(_) => return Ok(at),
            Err(_) => return Err(Error::Protocol("malformed packet")),
        }
    }
}

/// Makes room for the next read, and for all of a partial packet. Messages
/// still hold slices of a full buffer, so a new one takes over.
pub(super) fn make_room(buf: &mut BytesMut) {
    let partial = match fixed_header(buf) {
        Ok(Some(h)) => h.packet_len().min(MAX_PACKET),
        _ => 0,
    };
    // All of the packet, but no more than twice what came so far, so a
    // header alone cannot make the client allocate a large packet.
    let partial = partial.min(buf.len().saturating_mul(2).max(READ_BUF));
    if buf.capacity() - buf.len() < READ_MIN || buf.capacity() < partial {
        buf.reserve((READ_BUF / 2).max(partial.saturating_sub(buf.len())));
    }
}

pub(super) async fn writer<W: AsyncWrite + Unpin>(out: Rc<Out>, mut w: W) {
    let mut spare = Vec::new();
    loop {
        let buf = std::mem::replace(&mut *out.buf.borrow_mut(), std::mem::take(&mut spare));
        if buf.len() >= OUT_ROOM {
            out.drained.notify_one();
        }
        if buf.is_empty() {
            if out.closed.get() {
                let _ = w.shutdown().await;
                if let Some(done) = out.bye.borrow_mut().take() {
                    let _ = done.send(());
                }
                return;
            }
            spare = buf;
            // TLS may only have buffered the last bytes.
            if w.flush().await.is_err() {
                return;
            }
            out.wake.notified().await;
            continue;
        }
        if w.write_all(&buf).await.is_err() {
            out.broken.set(true);
            out.drained.notify_one();
            return;
        }
        spare = buf;
        spare.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_alone_does_not_reserve_its_whole_packet() {
        // A PUBLISH header that says 200 MiB follow.
        let mut len = [0u8; 4];
        let n = zzq_proto::varint::encode(200 << 20, &mut len);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&[0x30]);
        buf.extend_from_slice(&len[..n]);
        make_room(&mut buf);
        assert!(buf.capacity() <= 2 * READ_BUF, "{}", buf.capacity());
        // With most of it in, the rest fits in one go.
        let mut buf = BytesMut::with_capacity(150 << 20);
        buf.extend_from_slice(&[0x30]);
        buf.extend_from_slice(&len[..n]);
        buf.resize(150 << 20, 0);
        make_room(&mut buf);
        assert!(buf.capacity() >= (200 << 20) + 5);
    }
}

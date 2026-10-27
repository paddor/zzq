//! The writer: hands the session's encoded packets to the socket.

use std::rc::Rc;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::ConnShared;
use crate::lane::LaneRef;
use crate::slab::Key;

pub(super) async fn writer<W: AsyncWrite + Unpin>(
    lane: LaneRef,
    key: Key,
    conn: Rc<ConnShared>,
    mut w: W,
) {
    loop {
        conn.wake.notified().await;
        loop {
            let buf = {
                let mut l = lane.borrow_mut();
                let max_out = l.shared.opts.max_out_bytes;
                match l.sessions.get_mut(key) {
                    Some(s) if s.has_conn(conn.id) && !s.out.buf.is_empty() => {
                        conn.connacked.set(true);
                        let out = s.take_out();
                        if out.len() >= max_out {
                            conn.drained.notify_one();
                        }
                        out
                    }
                    _ => break,
                }
            };
            if w.write_all(&buf).await.is_err() {
                conn.close(Vec::new());
                return;
            }
            // Keep the buffer for the next swap. The lane sweep frees it once
            // the session goes idle.
            let mut buf = buf;
            buf.clear();
            let mut l = lane.borrow_mut();
            if let Some(s) = l.sessions.get_mut(key)
                && s.has_conn(conn.id)
            {
                s.out.spare = buf;
            }
        }
        // TLS may only have buffered the last bytes: push them out before
        // waiting for more.
        if w.flush().await.is_err() {
            conn.close(Vec::new());
            return;
        }
        if conn.is_closed() {
            let bye = std::mem::take(&mut *conn.farewell.borrow_mut());
            if !bye.is_empty() {
                let _ = w.write_all(&bye).await;
            }
            let _ = w.shutdown().await;
            return;
        }
    }
}

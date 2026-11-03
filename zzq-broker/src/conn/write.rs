//! The writer: hands the session's encoded packets to the socket.

use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::Conn;
use crate::durable::Due;

impl Conn {
    pub(super) async fn writer<W: AsyncWrite + Unpin>(self, mut w: W) {
        let (lane, key, conn) = (&self.lane, self.key, &self.shared);
        loop {
            conn.wake.notified().await;
            loop {
                if !self.output_ready().await {
                    break;
                }
                let buf = {
                    let mut l = lane.borrow_mut();
                    let max_out = l.shared.opts.max_out_bytes;
                    match l.session_of_mut(key, conn.id) {
                        Some(s) if !s.out.buf.is_empty() => {
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
                if let Some(s) = l.session_of_mut(key, conn.id) {
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

    /// Rechecks after every wait: more packets may have joined the output.
    async fn output_ready(&self) -> bool {
        let (lane, conn) = (&self.lane, &self.shared);
        loop {
            let due = {
                let l = lane.borrow();
                let Some(s) = l.session_of(self.key, conn.id) else {
                    return false;
                };
                s.out.due
            };
            let written = match due {
                Due::Now => return true,
                Due::Failed => false,
                Due::After(w) => {
                    let shared = lane.borrow().shared.clone();
                    match shared.cursor(w.journal).check(w.n) {
                        Some(true) => return true,
                        Some(false) => false,
                        None => {
                            // Cancel-safe: the cursor owns the write. The wake may
                            // also be new output, whose later barrier is checked next.
                            tokio::select! {
                                ok = shared.cursor(w.journal).wait(w.n) => ok,
                                () = conn.wake.notified() => !conn.is_closed(),
                            }
                        }
                    }
                }
            };
            if !written {
                conn.close(Vec::new());
                return false;
            }
        }
    }
}

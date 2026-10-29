//! The read loop: reads into the lane's blocks or a buffer of the
//! connection's own and hands complete packets to the lane.

use std::collections::VecDeque;
use std::io;
use std::pin::pin;
use std::task::{Poll, ready};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt};
use zzq_proto::packet::fixed_header;
use zzq_proto::{DecodeError, reason};

use super::ConnShared;
use crate::durable::Due;
use crate::lane::{Close, LaneRef};
use crate::slab::Key;

/// Read buffer sizes of a connection with a buffer of its own. It starts
/// small and doubles while reads fill the buffer.
pub(super) const MIN_CHUNK: usize = 1 << 10;
const READ_CHUNK: usize = 256 << 10;

/// A connection without a buffer of its own reads into a block of its
/// lane, so an idle connection holds no read buffer. Packets are sliced out
/// of the block; a partial packet at the end of a read moves into a buffer
/// of the connection's own.
const BLOCK: usize = 64 << 10;
/// Room a read gets in the block at least. With less, the next block
/// starts.
const BLOCK_ROOM: usize = 4 << 10;
/// Used blocks a lane keeps to fill again once nothing refers to them.
/// Beyond these, the oldest is let go.
const SPENT_MAX: usize = 8;

/// A lane's read blocks: the one being filled, and earlier ones that
/// packets may still refer to. A lane fills its own blocks again, so it
/// does not allocate per block, and a block is not freed by the lane that
/// drops its last packet.
#[derive(Debug)]
pub(crate) struct ReadBlocks {
    current: BytesMut,
    /// The ends of earlier blocks, oldest first.
    spent: VecDeque<BytesMut>,
}

impl Default for ReadBlocks {
    fn default() -> Self {
        ReadBlocks {
            // A block from the start: an empty buffer in `spent` could never
            // be filled again.
            current: BytesMut::with_capacity(BLOCK),
            spent: VecDeque::new(),
        }
    }
}

impl ReadBlocks {
    /// The current block, with at least [`BLOCK_ROOM`] room.
    fn room(&mut self) -> &mut BytesMut {
        if self.current.capacity() < BLOCK_ROOM {
            let reusable = self.spent.front_mut().is_some_and(|b| b.try_reclaim(BLOCK));
            let next = reusable.then(|| self.spent.pop_front()).flatten();
            let next = next.unwrap_or_else(|| BytesMut::with_capacity(BLOCK));
            let used = std::mem::replace(&mut self.current, next);
            if self.spent.len() == SPENT_MAX {
                self.spent.pop_front();
            }
            self.spent.push_back(used);
        }
        &mut self.current
    }
}

/// Length of the complete packets at the start of `buf`, and the full length
/// of the partial packet after them once its header is in.
#[inline]
fn complete_len(buf: &[u8], max: usize) -> Result<(usize, usize), DecodeError> {
    let mut at = 0;
    while let Some(h) = fixed_header(&buf[at..])? {
        let n = h.packet_len();
        if n > max {
            return Err(DecodeError::TooLarge);
        }
        if at + n > buf.len() {
            return Ok((at, n));
        }
        at += n;
    }
    Ok((at, 0))
}

/// Capacity for the rest of a packet of `partial` bytes in a buffer that
/// holds `have`: all of it, but no more than twice what came so far, so a
/// header alone cannot make the connection allocate a large packet.
fn packet_room(have: usize, partial: usize) -> usize {
    partial.min(have.saturating_mul(2).max(READ_CHUNK))
}

/// Next read size: double after a full read, halve after a small one.
fn next_chunk(chunk: usize, read: usize, room: usize) -> usize {
    if read == room {
        (chunk * 2).min(READ_CHUNK)
    } else if read < chunk / 16 {
        (chunk / 2).max(MIN_CHUNK)
    } else {
        chunk
    }
}

pub(super) async fn read_loop<R: AsyncRead + Unpin>(
    lane: &LaneRef,
    key: Key,
    conn: &ConnShared,
    r: &mut R,
    buf: BytesMut,
) -> Close {
    let max = lane.borrow().shared.opts.max_packet_size;
    let mut input = Input {
        buf,
        chunk: MIN_CHUNK,
        own: true,
    };
    loop {
        let (complete, partial) = match complete_len(&input.buf, max) {
            Ok(n) => n,
            Err(e) => return Close::Protocol(e.reason_code()),
        };
        if complete == 0 {
            if let Some(close) = output_room(lane, key, conn).await {
                return close;
            }
            if let Some(close) = read_more(lane, conn, r, &mut input, partial).await {
                return close;
            }
            continue;
        }
        let chunk = input.buf.split_to(complete).freeze();
        let result = {
            let mut l = lane.borrow_mut();
            let result = l.handle_chunk(key, &chunk);
            l.flush();
            result
        };
        drop(chunk);
        conn.held.set(true);
        let mut waited = crate::broker::ship(lane).await;
        // Let other connections on this lane run, such as subscribers whose
        // acks free up their window.
        tokio::task::yield_now().await;
        let held = lane.borrow_mut().take_held(conn.id);
        waited |= !held.is_empty();
        for h in held {
            if let Some(sync) = &h.sync {
                sync.wait().await;
            }
            if let Err(close) = written(lane, conn, h.due).await {
                return close;
            }
            lane.borrow_mut().release(&h);
        }
        match disk_acks(lane, key, conn).await {
            Ok(w) => waited |= w,
            Err(close) => return close,
        }
        conn.held.set(false);
        // Keep alive counts from the end of a wait the broker made.
        if waited {
            conn.last_rx.set(crate::time::now());
        }
        if let Err(close) = result {
            return close;
        }
        if conn.is_closed() {
            return Close::Kicked;
        }
    }
}

/// Waits for the journal write `due`. A failed one closes the connection:
/// the client learns of it, and a new connection finds the journal
/// written again.
async fn written(lane: &LaneRef, conn: &ConnShared, due: Due) -> Result<(), Close> {
    let w = match due {
        Due::Now => return Ok(()),
        Due::After(w) => w,
        Due::Failed => return Err(Close::Protocol(reason::UNSPECIFIED_ERROR)),
    };
    let shared = lane.borrow().shared.clone();
    // Cancel-safe as in `read_more`.
    tokio::select! {
        biased;
        () = conn.stop.notified() => Err(Close::Kicked),
        ok = shared.cursor(w.journal).wait(w.n) => {
            if ok { Ok(()) } else { Err(Close::Protocol(reason::UNSPECIFIED_ERROR)) }
        }
    }
}

/// Waits for the journal writes the connection's held acks need, then
/// sends them. True if it held any.
async fn disk_acks(lane: &LaneRef, key: Key, conn: &ConnShared) -> Result<bool, Close> {
    let (shared, waits) = {
        let mut l = lane.borrow_mut();
        let Some(waits) = l.hold_waits(key, conn.id) else {
            return Ok(false);
        };
        (l.shared.clone(), waits)
    };
    let mut written = true;
    for w in waits {
        // Cancel-safe as in `read_more`; a lost wait leaves the record to
        // its cursor.
        tokio::select! {
            biased;
            () = conn.stop.notified() => return Err(Close::Kicked),
            ok = shared.cursor(w.journal).wait(w.n) => written &= ok,
        }
    }
    lane.borrow_mut().release_acks(key, conn.id, written)?;
    Ok(true)
}

/// Waits while the session's replies wait over `max_out_bytes` for the
/// writer: a client that does not read them is not read either. `Some`
/// once the connection ends.
async fn output_room(lane: &LaneRef, key: Key, conn: &ConnShared) -> Option<Close> {
    loop {
        let full = {
            let l = lane.borrow();
            let max = l.shared.opts.max_out_bytes;
            l.sessions
                .get(key)
                .is_some_and(|s| s.has_conn(conn.id) && s.out.buf.len() >= max)
        };
        if !full {
            return None;
        }
        // Cancel-safe as in `read_more`.
        tokio::select! {
            biased;
            () = conn.stop.notified() => return Some(Close::Kicked),
            () = conn.drained.notified() => {}
        }
    }
}

/// A connection's unread bytes, and the size a buffer of its own grows by.
struct Input {
    buf: BytesMut,
    chunk: usize,
    /// `buf` is a buffer of the connection's own, not part of the lane's
    /// block.
    own: bool,
}

/// Reads more into `input`, which holds at most the start of a packet of
/// `partial` bytes. A connection with nothing left and small reads goes
/// into the lane's block, and `input` becomes the bytes read; any other
/// into a buffer of its own, grown by `chunk`. `Some` once the connection
/// ends.
async fn read_more<R: AsyncRead + Unpin>(
    lane: &LaneRef,
    conn: &ConnShared,
    r: &mut R,
    input: &mut Input,
    partial: usize,
) -> Option<Close> {
    let Input { buf, chunk, own } = input;
    let shared = buf.is_empty() && *chunk == MIN_CHUNK;
    if shared {
        // Waits without a buffer.
        *buf = BytesMut::new();
        *own = false;
    } else if !*own {
        // Sized here: growing a slice of the block would allocate as much
        // as the block.
        let mut b =
            BytesMut::with_capacity((buf.len() + *chunk).max(packet_room(buf.len(), partial)));
        b.extend_from_slice(buf);
        *buf = b;
        *own = true;
    } else {
        // Grow only now, when the buffer holds at most a partial packet:
        // a new buffer copies what is left in the old one.
        if buf.capacity() - buf.len() < *chunk / 4 {
            buf.reserve(*chunk);
        }
        // Room for a large packet in few allocations.
        let room = packet_room(buf.len(), partial);
        if room > buf.capacity() {
            buf.reserve(room - buf.len());
        }
    }
    let room = buf.capacity() - buf.len();
    let read = std::future::poll_fn(|cx| {
        if !shared {
            return pin!(r.read_buf(&mut *buf)).poll(cx);
        }
        let mut l = lane.borrow_mut();
        let block = l.read_blocks.room();
        let n = ready!(pin!(r.read_buf(&mut *block)).poll(cx))?;
        *buf = block.split();
        Poll::Ready(Ok::<_, io::Error>(n))
    });
    // Cancel-safe: a `notify_one` that a dropped `notified()` got passes on
    // to the next one, and a dropped read took no bytes.
    tokio::select! {
        biased;
        () = conn.stop.notified() => Some(Close::Kicked),
        n = read => match n {
            Ok(0) | Err(_) => Some(Close::Lost),
            Ok(n) => {
                conn.last_rx.set(crate::time::now());
                *chunk = if shared {
                    // A large read makes it a busy connection.
                    if n > MIN_CHUNK { 2 * MIN_CHUNK } else { MIN_CHUNK }
                } else {
                    next_chunk(*chunk, n, room)
                };
                None
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_alone_does_not_get_room_for_its_whole_packet() {
        assert_eq!(packet_room(5, 16 << 20), READ_CHUNK);
        assert_eq!(packet_room(1 << 20, 16 << 20), 2 << 20);
        assert_eq!(packet_room(10 << 20, 16 << 20), 16 << 20);
        assert_eq!(packet_room(5, 100), 100);
    }
}

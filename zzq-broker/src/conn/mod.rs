//! One client connection: CONNECT handshake, read loop and writer.

mod handshake;
mod read;
mod write;

use std::cell::{Cell, RefCell};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Notify;
use zzq_proto::{PacketType, encode};
use zzq_tokio::transport::Driver;

use crate::lane::{Close, LaneRef};

pub(crate) use handshake::OwnedConnect;
pub(crate) use read::ReadBlocks;

/// How long a closed connection's writer may take to write what is left,
/// such as to a peer that stopped reading, before it is stopped.
const CLOSE_LINGER: Duration = Duration::from_secs(2);

/// State shared by a connection's reader, writer and its session.
#[derive(Debug)]
pub(crate) struct ConnShared {
    pub(crate) id: u64,
    /// Wakes the writer.
    pub(crate) wake: Notify,
    /// Wakes the reader once the writer took replies that were over
    /// `max_out_bytes`.
    pub(crate) drained: Notify,
    /// Wakes the reader to stop.
    stop: Notify,
    closed: Cell<bool>,
    /// Last packet to write before closing.
    farewell: RefCell<Vec<u8>>,
    pub(crate) last_rx: Cell<Instant>,
    /// The read loop waits for the broker, for room to ship or for other
    /// lanes, so the client may be sending while nothing is read.
    pub(crate) held: Cell<bool>,
    /// Keep Alive in seconds.
    pub(crate) keep_alive: Cell<u16>,
    /// The writer took the CONNACK, so the client knows it connected.
    pub(crate) connacked: Cell<bool>,
}

impl ConnShared {
    fn new(id: u64, keep_alive: u16) -> ConnShared {
        ConnShared {
            id,
            wake: Notify::new(),
            drained: Notify::new(),
            stop: Notify::new(),
            closed: Cell::new(false),
            farewell: RefCell::new(Vec::new()),
            last_rx: Cell::new(crate::time::now()),
            held: Cell::new(false),
            keep_alive: Cell::new(keep_alive),
            connacked: Cell::new(false),
        }
    }

    /// Closes the connection after writing `farewell`.
    pub(crate) fn close(&self, farewell: Vec<u8>) {
        if self.closed.replace(true) {
            return;
        }
        *self.farewell.borrow_mut() = farewell;
        self.stop.notify_one();
        self.wake.notify_one();
    }

    const fn is_closed(&self) -> bool {
        self.closed.get()
    }
}

/// Runs broker connections on a lane.
#[derive(Debug)]
pub(crate) struct ConnDriver {
    pub(crate) lane: LaneRef,
    /// Bytes read before the connection reached this lane.
    pub(crate) initial: BytesMut,
    /// DER of the client certificate, if any.
    pub(crate) peer_cert: Option<Vec<u8>>,
}

impl Driver for ConnDriver {
    type Output = ();

    fn run<R, W>(self, reader: R, writer: W) -> impl Future<Output = ()>
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
        run(self, reader, writer)
    }
}

async fn run<R, W>(d: ConnDriver, mut r: R, w: W)
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    let lane = d.lane;
    // Boxed, so the handshake's state is freed once it is done instead of
    // sizing every connection's future.
    let started = Box::pin(handshake::start(&lane, d.initial, d.peer_cert, &mut r, w)).await;
    let Some((key, conn, version, buf, mut writer)) = started else {
        return;
    };
    let close = read::read_loop(&lane, key, &conn, &mut r, buf).await;
    if let Close::Protocol(code) = close {
        let mut bye = Vec::new();
        if version.is_v5() {
            encode::reason_only(PacketType::Disconnect, version, code, &[], &mut bye);
        }
        conn.close(bye);
    } else {
        conn.close(Vec::new());
    }
    {
        let mut l = lane.borrow_mut();
        l.detach(key, conn.id, close);
        l.flush();
    }
    crate::broker::ship(&lane).await;
    if tokio::time::timeout(CLOSE_LINGER, &mut writer)
        .await
        .is_err()
    {
        writer.abort();
    }
}

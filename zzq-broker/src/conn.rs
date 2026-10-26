//! One client connection: CONNECT handshake, read loop and writer.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::io;
use std::pin::pin;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::task::{Poll, ready};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use zzq_proto::packet::{Connect, decode, fixed_header};
use zzq_proto::property;
use zzq_proto::{DecodeError, Packet, PacketType, Version, encode, reason};
use zzq_tokio::transport::Driver;

use crate::lane::{Close, LaneRef, Phase};
use crate::msg::Msg;
use crate::options::{AuthDecision, Authenticator, ClientInfo, ConnectInfo};
use crate::session::Will;
use crate::slab::Key;

/// Read buffer sizes of a connection with a buffer of its own. It starts
/// small and doubles while reads fill the buffer.
const MIN_CHUNK: usize = 1 << 10;
const READ_CHUNK: usize = 256 << 10;

/// How long a closed connection's writer may take to write what is left,
/// such as to a peer that stopped reading, before it is stopped.
const CLOSE_LINGER: Duration = Duration::from_secs(2);

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

/// State shared by a connection's reader, writer and its session.
#[derive(Debug)]
pub(crate) struct ConnShared {
    pub(crate) id: u64,
    /// Wakes the writer.
    pub(crate) wake: Notify,
    /// Wakes the reader to stop.
    stop: Notify,
    closed: Cell<bool>,
    /// Last packet to write before closing.
    farewell: RefCell<Vec<u8>>,
    pub(crate) last_rx: Cell<Instant>,
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
            stop: Notify::new(),
            closed: Cell::new(false),
            farewell: RefCell::new(Vec::new()),
            last_rx: Cell::new(Instant::now()),
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

/// CONNECT fields kept after the read buffer moves on.
#[derive(Debug)]
pub(crate) struct OwnedConnect {
    pub(crate) version: Version,
    pub(crate) clean_start: bool,
    pub(crate) keep_alive: u16,
    pub(crate) client_id: String,
    /// The broker chose the client ID.
    pub(crate) assigned: bool,
    pub(crate) session_expiry: u32,
    pub(crate) receive_maximum: u16,
    pub(crate) max_packet_size: usize,
    pub(crate) will: Option<Will>,
    pub(crate) username: Option<String>,
    /// The broker changed `keep_alive`, to tell v5 clients.
    pub(crate) server_keep_alive: bool,
}

impl OwnedConnect {
    fn from_packet(c: &Connect<'_>) -> OwnedConnect {
        let v5 = c.version.is_v5();
        let p = &c.properties;
        let session_expiry = if v5 {
            p.get_int(property::SESSION_EXPIRY_INTERVAL).unwrap_or(0)
        } else if c.clean_start {
            0
        } else {
            u32::MAX
        };
        let will = c.will.map(|w| {
            let mut props = Vec::new();
            for prop in w
                .properties
                .iter()
                .filter(|x| x.id() != property::WILL_DELAY_INTERVAL)
            {
                prop.encode(&mut props);
            }
            Will {
                msg: Msg::new(w.topic, w.payload, &props, w.qos, w.retain),
                delay: w
                    .properties
                    .get_int(property::WILL_DELAY_INTERVAL)
                    .unwrap_or(0),
            }
        });
        OwnedConnect {
            version: c.version,
            clean_start: c.clean_start,
            keep_alive: c.keep_alive,
            client_id: c.client_id.to_owned(),
            assigned: false,
            session_expiry,
            receive_maximum: p
                .get_int(property::RECEIVE_MAXIMUM)
                .map_or(u16::MAX, |v| v as u16),
            max_packet_size: p
                .get_int(property::MAXIMUM_PACKET_SIZE)
                .map_or(usize::MAX, |v| v as usize),
            will,
            username: c.username.map(str::to_owned),
            server_keep_alive: false,
        }
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
    let started = Box::pin(start(&lane, d.initial, d.peer_cert, &mut r, w)).await;
    let Some((key, conn, version, buf, mut writer)) = started else {
        return;
    };
    let close = read_loop(&lane, key, &conn, &mut r, buf).await;
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

/// Reads CONNECT, attaches the session and starts the writer.
async fn start<R, W>(
    lane: &LaneRef,
    mut buf: BytesMut,
    peer_cert: Option<Vec<u8>>,
    r: &mut R,
    w: W,
) -> Option<(Key, Rc<ConnShared>, Version, BytesMut, JoinHandle<()>)>
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    let (limit, max, auth) = {
        let l = lane.borrow();
        let o = &l.shared.opts;
        (
            o.connect_timeout,
            o.max_packet_size,
            o.authenticator.clone(),
        )
    };
    let read = tokio::time::timeout(
        limit,
        read_connect(r, &mut buf, max, auth.as_deref(), peer_cert.as_deref()),
    )
    .await;
    let mut c = match read {
        Ok(Ok(c)) => c,
        Ok(Err((Some(version), code))) => {
            refuse(w, version, code).await;
            return None;
        }
        Ok(Err((None, _))) | Err(_) => return None,
    };
    let version = c.version;
    let conn = {
        let mut l = lane.borrow_mut();
        Rc::new(ConnShared::new(l.next_conn_id(), c.keep_alive))
    };
    let key = match establish(lane, &mut c, &conn) {
        Ok(key) => key,
        Err(code) => {
            refuse(w, version, code).await;
            return None;
        }
    };
    lane.borrow_mut().flush();
    // A will published on takeover may go to other lanes.
    crate::broker::ship(lane).await;
    let writer = tokio::task::spawn_local(writer(lane.clone(), key, conn.clone(), w));
    Some((key, conn, version, buf, writer))
}

type ConnectError = (Option<Version>, u8);

async fn read_connect<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut BytesMut,
    max: usize,
    auth: Option<&dyn Authenticator>,
    peer_cert: Option<&[u8]>,
) -> Result<OwnedConnect, ConnectError> {
    loop {
        // The first byte is enough to refuse a packet that is not CONNECT
        // [MQTT-3.1.0-1], without buffering its body.
        if buf.first().is_some_and(|&b| b >> 4 != 1) {
            return Err((None, reason::PROTOCOL_ERROR));
        }
        match decode(buf, Version::V5, max) {
            Ok(Some((Packet::Connect(c), n))) => {
                // zzq supports no enhanced authentication [MQTT-4.12.0-1].
                if c.properties.get(property::AUTHENTICATION_METHOD).is_some() {
                    return Err((Some(c.version), reason::BAD_AUTHENTICATION_METHOD));
                }
                if let Some(auth) = auth {
                    let info = ConnectInfo {
                        client_id: c.client_id,
                        username: c.username,
                        password: c.password,
                        version: c.version,
                        peer_cert,
                    };
                    if let AuthDecision::Deny(code) = auth.authenticate(&info) {
                        // A code below 0x80 would accept the connection.
                        let code = u8::from(code);
                        let code = if code < 0x80 {
                            reason::NOT_AUTHORIZED
                        } else {
                            code
                        };
                        return Err((Some(c.version), code));
                    }
                }
                let owned = OwnedConnect::from_packet(&c);
                let _ = buf.split_to(n);
                return Ok(owned);
            }
            Ok(Some(_)) => return Err((None, reason::PROTOCOL_ERROR)),
            Ok(None) => {}
            Err(DecodeError::UnsupportedVersion(_)) => {
                return Err((Some(Version::V311), reason::UNSUPPORTED_PROTOCOL_VERSION));
            }
            // A v5 client learns why its CONNECT failed; earlier versions
            // have no code for it.
            Err(e) => return Err((is_v5_connect(buf).then_some(Version::V5), e.reason_code())),
        }
        buf.reserve(MIN_CHUNK);
        match r.read_buf(buf).await {
            Ok(0) | Err(_) => return Err((None, reason::UNSPECIFIED_ERROR)),
            Ok(_) => {}
        }
    }
}

/// Whether `buf` starts with a CONNECT that names MQTT 5.0.
fn is_v5_connect(buf: &[u8]) -> bool {
    let Ok(Some(fh)) = fixed_header(buf) else {
        return false;
    };
    buf.get(fh.header_len..fh.header_len + 7) == Some(&[0, 4, b'M', b'Q', b'T', b'T', 5][..])
}

/// Sends a refusing CONNACK and closes. Over QUIC, closing without
/// `shutdown` would drop the CONNACK.
async fn refuse<W: AsyncWrite + Unpin>(mut w: W, version: Version, code: u8) {
    let mut out = Vec::new();
    connack_error(version, code, &mut out);
    let _ = w.write_all(&out).await;
    let _ = w.shutdown().await;
}

fn connack_error(version: Version, code: u8, out: &mut Vec<u8>) {
    let code = if version.is_v5() {
        code
    } else {
        reason::v311::from_v5(code)
    };
    let c = encode::ConnAckFields {
        code,
        ..encode::ConnAckFields::default()
    };
    encode::connack(&c, version, out);
}

/// Resolves the client ID and attaches the connection to its session.
fn establish(lane: &LaneRef, c: &mut OwnedConnect, conn: &Rc<ConnShared>) -> Result<Key, u8> {
    if c.client_id.is_empty() {
        if !c.version.is_v5() && !c.clean_start {
            return Err(reason::CLIENT_IDENTIFIER_NOT_VALID);
        }
        c.client_id = lane.borrow_mut().assign_client_id();
        c.assigned = c.version.is_v5();
    }
    if let Some(w) = &c.will {
        let l = lane.borrow();
        let client = ClientInfo {
            client_id: &c.client_id,
            username: c.username.as_deref(),
        };
        if crate::msg::too_deep(w.msg.topic(), l.shared.opts.max_topic_levels) {
            return Err(reason::TOPIC_NAME_INVALID);
        }
        let allowed = !w.msg.topic().starts_with("$SYS/")
            && l.shared
                .opts
                .authorizer
                .as_deref()
                .is_none_or(|acl| acl.may_publish(&client, w.msg.topic()));
        if !allowed {
            return Err(reason::NOT_AUTHORIZED);
        }
    }
    {
        let l = lane.borrow();
        let o = &l.shared.opts;
        if let Some(max) = o.max_keep_alive
            && (c.keep_alive == 0 || c.keep_alive > max)
        {
            c.keep_alive = max;
            c.server_keep_alive = true;
            conn.keep_alive.set(max);
        }
        if l.phase != Phase::Open {
            return Err(reason::SERVER_UNAVAILABLE);
        }
        // A new session that outlives its connection needs room in the state
        // budget.
        if l.budget.full
            && c.session_expiry.min(o.max_session_expiry) != 0
            && !l.has_session(&c.client_id)
        {
            return Err(reason::QUOTA_EXCEEDED);
        }
        // A takeover frees the old connection's slot for the new one.
        let takeover = l.is_connected(&c.client_id);
        // `try_update`, the new name, needs Rust 1.99.
        #[allow(deprecated)]
        let reserved = takeover
            || l.shared
                .connections
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                    (n < o.max_connections).then_some(n + 1)
                })
                .is_ok();
        if !reserved {
            return Err(reason::QUOTA_EXCEEDED);
        }
    }
    let attached = lane.borrow_mut().attach(c, conn.clone());
    Ok(attached.key)
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

async fn read_loop<R: AsyncRead + Unpin>(
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
        crate::broker::ship(lane).await;
        // Let other connections on this lane run, such as subscribers whose
        // acks free up their window.
        tokio::task::yield_now().await;
        let held = lane.borrow_mut().take_held(conn.id);
        for h in held {
            h.sync.wait().await;
            lane.borrow_mut().release(&h);
        }
        if let Err(close) = result {
            return close;
        }
        if conn.is_closed() {
            return Close::Kicked;
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
        let mut b = BytesMut::with_capacity((buf.len() + *chunk).max(partial));
        b.extend_from_slice(buf);
        *buf = b;
        *own = true;
    } else {
        // Grow only now, when the buffer holds at most a partial packet:
        // a new buffer copies what is left in the old one.
        if buf.capacity() - buf.len() < *chunk / 4 {
            buf.reserve(*chunk);
        }
        // Room for a large packet in one allocation.
        if partial > buf.capacity() {
            buf.reserve(partial - buf.len());
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
    tokio::select! {
        biased;
        () = conn.stop.notified() => Some(Close::Kicked),
        n = read => match n {
            Ok(0) | Err(_) => Some(Close::Lost),
            Ok(n) => {
                conn.last_rx.set(Instant::now());
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

async fn writer<W: AsyncWrite + Unpin>(lane: LaneRef, key: Key, conn: Rc<ConnShared>, mut w: W) {
    loop {
        conn.wake.notified().await;
        loop {
            let buf = {
                let mut l = lane.borrow_mut();
                match l.sessions.get_mut(key) {
                    Some(s) if s.has_conn(conn.id) && !s.out.buf.is_empty() => {
                        conn.connacked.set(true);
                        s.take_out()
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

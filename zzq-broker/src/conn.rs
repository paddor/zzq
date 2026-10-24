//! One client connection: CONNECT handshake, read loop and writer.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Notify;
use zzq_proto::packet::{Connect, decode, fixed_header};
use zzq_proto::property;
use zzq_proto::{DecodeError, Packet, PacketType, Version, encode, reason};
use zzq_tokio::transport::Driver;

use crate::lane::{Close, LaneRef};
use crate::msg::Msg;
use crate::options::{AuthDecision, Authenticator, ClientInfo, ConnectInfo};
use crate::session::Will;
use crate::slab::Key;

/// Read buffer sizes. A connection starts small and doubles while reads
/// fill the buffer, so idle connections stay cheap.
const MIN_CHUNK: usize = 1 << 10;
const READ_CHUNK: usize = 256 << 10;

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
    let Some((key, conn, version, buf)) = start(&lane, d.initial, d.peer_cert, &mut r, w).await
    else {
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
}

/// Reads CONNECT, attaches the session and starts the writer. Locals end
/// here, so they do not stay in every connection's future.
async fn start<R, W>(
    lane: &LaneRef,
    mut buf: BytesMut,
    peer_cert: Option<Vec<u8>>,
    r: &mut R,
    w: W,
) -> Option<(Key, Rc<ConnShared>, Version, BytesMut)>
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
    tokio::task::spawn_local(writer(lane.clone(), key, conn.clone(), w));
    Some((key, conn, version, buf))
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
        match decode(buf, Version::V5, max) {
            Ok(Some((Packet::Connect(c), n))) => {
                if let Some(auth) = auth {
                    let info = ConnectInfo {
                        client_id: c.client_id,
                        username: c.username,
                        password: c.password,
                        version: c.version,
                        peer_cert,
                    };
                    if let AuthDecision::Deny(code) = auth.authenticate(&info) {
                        return Err((Some(c.version), code.into()));
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
            Err(e) => return Err((None, e.reason_code())),
        }
        buf.reserve(MIN_CHUNK);
        match r.read_buf(buf).await {
            Ok(0) | Err(_) => return Err((None, reason::UNSPECIFIED_ERROR)),
            Ok(_) => {}
        }
    }
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
    encode::connack(version, false, code, &[], out);
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
        // A takeover frees the old connection's slot for the new one.
        let takeover = l.is_connected(&c.client_id);
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
    mut buf: BytesMut,
) -> Close {
    let max = lane.borrow().shared.opts.max_packet_size;
    let mut chunk = MIN_CHUNK;
    loop {
        if buf.capacity() - buf.len() < chunk / 4 {
            buf.reserve(chunk);
        }
        let (complete, partial) = match complete_len(&buf, max) {
            Ok(n) => n,
            Err(e) => return Close::Protocol(e.reason_code()),
        };
        if complete == 0 {
            // Room for a large packet in one allocation.
            if partial > buf.capacity() {
                buf.reserve(partial - buf.len());
            }
            let room = buf.capacity() - buf.len();
            tokio::select! {
                biased;
                () = conn.stop.notified() => return Close::Kicked,
                n = r.read_buf(&mut buf) => match n {
                    Ok(0) | Err(_) => return Close::Lost,
                    Ok(n) => {
                        conn.last_rx.set(Instant::now());
                        chunk = next_chunk(chunk, n, room);
                    }
                },
            }
            continue;
        }
        let chunk = buf.split_to(complete).freeze();
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
        let held = lane.borrow_mut().take_held();
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

async fn writer<W: AsyncWrite + Unpin>(lane: LaneRef, key: Key, conn: Rc<ConnShared>, mut w: W) {
    loop {
        conn.wake.notified().await;
        loop {
            let buf = {
                let mut l = lane.borrow_mut();
                match l.sessions.get_mut(key) {
                    Some(s) if s.has_conn(conn.id) && !s.out.is_empty() => {
                        let spare = std::mem::take(&mut s.spare);
                        std::mem::replace(&mut s.out, spare)
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
                s.spare = buf;
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

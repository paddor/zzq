//! The CONNECT handshake: read, authenticate and attach to a session.

use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinHandle;
use zzq_proto::packet::{Connect, decode, fixed_header};
use zzq_proto::property;
use zzq_proto::{DecodeError, Packet, Version, encode, reason};

use super::ConnShared;
use super::read::MIN_CHUNK;
use super::write::writer;
use crate::lane::{LaneRef, Phase};
use crate::msg::Msg;
use crate::options::{AuthDecision, Authenticator, ClientInfo, ConnectInfo};
use crate::session::Will;
use crate::slab::Key;

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

/// Reads CONNECT, attaches the session and starts the writer.
pub(super) async fn start<R, W>(
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
    // A will published on takeover may go to other lanes. Keep alive counts
    // from the end of a wait the broker made, as in the read loop.
    conn.held.set(true);
    if crate::broker::ship(lane).await {
        conn.last_rx.set(Instant::now());
    }
    conn.held.set(false);
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
        // Relaxed: a count for the limit, no data rides on it.
        let reserved = takeover
            || l.shared
                .connections
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
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

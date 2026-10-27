//! CONNECT and CONNACK.

use bytes::{Buf, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use zzq_proto::encode::{self, ConnectFields, WillFields};
use zzq_proto::packet::{ConnAck, decode};
use zzq_proto::property::{self, Property};
use zzq_proto::{Packet, Version, reason};

use crate::{ClientOptions, Connected, Error, Result};

use super::MAX_PACKET;

/// Sends CONNECT and waits for CONNACK. A resumed session does not start
/// clean if it outlives the connection.
pub(super) async fn handshake<R, W>(
    opts: &ClientOptions,
    resume: bool,
    r: &mut R,
    w: &mut W,
    buf: &mut BytesMut,
) -> Result<(Connected, u16)>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut props = Vec::new();
    if opts.session_expiry > 0 {
        props.push(Property::U32(
            property::SESSION_EXPIRY_INTERVAL,
            opts.session_expiry,
        ));
    }
    // Unacked QoS 1/2 messages fit in the inbox, so it never blocks them.
    let window = u16::try_from(opts.inbox.max(1)).unwrap_or(u16::MAX);
    if window < u16::MAX {
        props.push(Property::U16(property::RECEIVE_MAXIMUM, window));
    }
    let keeps_session = if opts.version.is_v5() {
        opts.session_expiry > 0
    } else {
        !opts.clean_start
    };
    let will_props = opts
        .will
        .as_ref()
        .filter(|w| w.delay > 0)
        .map(|w| [Property::U32(property::WILL_DELAY_INTERVAL, w.delay)]);
    let fields = ConnectFields {
        version: opts.version,
        clean_start: opts.clean_start && !(resume && keeps_session),
        keep_alive: opts.keep_alive,
        properties: &props,
        client_id: &opts.client_id,
        will: opts.will.as_ref().map(|w| WillFields {
            qos: w.qos,
            retain: w.retain,
            properties: will_props.as_ref().map_or(&[][..], |p| &p[..]),
            topic: &w.topic,
            payload: &w.payload,
        }),
        username: opts.username.as_deref(),
        password: opts.password.as_deref(),
    };
    let mut out = Vec::new();
    encode::connect(&fields, &mut out);
    w.write_all(&out)
        .await
        .map_err(|e| Error::Transport(e.into()))?;
    loop {
        match decode(buf, opts.version, MAX_PACKET) {
            Ok(Some((Packet::ConnAck(c), n))) => {
                let res = connack(&c, opts.version);
                buf.advance(n);
                return res;
            }
            Ok(Some(_)) | Err(_) => return Err(Error::Protocol("expected CONNACK")),
            Ok(None) => {}
        }
        match r.read_buf(buf).await {
            Ok(0) | Err(_) => return Err(Error::Closed),
            Ok(_) => {}
        }
    }
}

/// CONNACK details and the broker's Receive Maximum.
fn connack(c: &ConnAck<'_>, version: Version) -> Result<(Connected, u16)> {
    if c.code != 0 {
        // v3.1.1 return codes become the v5 codes with the same meaning.
        let code = if version.is_v5() {
            c.code
        } else {
            reason::v311::to_v5(c.code)
        };
        return Err(Error::Refused(code.into()));
    }
    let connected = Connected {
        session_present: c.session_present,
        assigned_client_id: c
            .properties
            .get_str(property::ASSIGNED_CLIENT_IDENTIFIER)
            .map(str::to_owned),
    };
    let quota = c
        .properties
        .get_int(property::RECEIVE_MAXIMUM)
        .map_or(u16::MAX, |v| u16::try_from(v).unwrap_or(u16::MAX));
    Ok((connected, quota))
}

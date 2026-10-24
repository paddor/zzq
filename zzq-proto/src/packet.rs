//! Packet views and the decoder.
//!
//! [`decode`] parses one packet from the front of a buffer without copying.
//! Every view borrows from that buffer.

use crate::cursor::Cursor;
use crate::topic::{validate_filter, validate_name};
use crate::{DecodeError, Properties, QoS, SubscriptionOptions, Version, varint};

/// Fixed header of a packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixedHeader {
    /// First byte: packet type and flags.
    pub byte1: u8,
    /// Remaining length.
    pub remaining: usize,
    /// Size of the fixed header itself.
    pub header_len: usize,
}

impl FixedHeader {
    /// Size of the whole packet.
    #[inline]
    pub const fn packet_len(&self) -> usize {
        self.header_len + self.remaining
    }
}

/// Parses a fixed header. `Ok(None)` means more bytes are needed.
#[inline]
pub fn fixed_header(buf: &[u8]) -> Result<Option<FixedHeader>, DecodeError> {
    let Some(&byte1) = buf.first() else {
        return Ok(None);
    };
    let Some((remaining, n)) = varint::decode(&buf[1..])? else {
        return Ok(None);
    };
    Ok(Some(FixedHeader {
        byte1,
        remaining,
        header_len: 1 + n,
    }))
}

/// CONNECT will message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Will<'a> {
    pub qos: QoS,
    pub retain: bool,
    pub properties: Properties<'a>,
    pub topic: &'a str,
    pub payload: &'a [u8],
}

/// CONNECT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Connect<'a> {
    pub version: Version,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub properties: Properties<'a>,
    pub client_id: &'a str,
    pub will: Option<Will<'a>>,
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
}

/// CONNACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnAck<'a> {
    pub session_present: bool,
    /// v5 reason code or v3.1.1 return code.
    pub code: u8,
    pub properties: Properties<'a>,
}

/// PUBLISH.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    /// Empty when a v5 publisher uses a topic alias.
    pub topic: &'a str,
    /// Zero for QoS 0.
    pub pkid: u16,
    pub properties: Properties<'a>,
    pub payload: &'a [u8],
}

/// PUBACK, PUBREC, PUBREL or PUBCOMP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ack<'a> {
    pub pkid: u16,
    pub reason: u8,
    pub properties: Properties<'a>,
}

/// SUBSCRIBE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subscribe<'a> {
    pub pkid: u16,
    pub properties: Properties<'a>,
    version: Version,
    raw: &'a [u8],
}

impl<'a> Subscribe<'a> {
    /// The topic filters with their options.
    pub const fn filters(&self) -> FilterIter<'a> {
        FilterIter {
            cursor: Cursor::new(self.raw),
            version: self.version,
            options: true,
        }
    }
}

/// UNSUBSCRIBE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unsubscribe<'a> {
    pub pkid: u16,
    pub properties: Properties<'a>,
    raw: &'a [u8],
}

impl<'a> Unsubscribe<'a> {
    /// The topic filters.
    pub fn filters(&self) -> impl Iterator<Item = &'a str> + use<'a> {
        FilterIter {
            cursor: Cursor::new(self.raw),
            version: Version::V311,
            options: false,
        }
        .map(|(f, _)| f)
    }
}

/// Iterator over validated topic filters.
#[derive(Debug)]
pub struct FilterIter<'a> {
    cursor: Cursor<'a>,
    version: Version,
    options: bool,
}

impl<'a> Iterator for FilterIter<'a> {
    type Item = (&'a str, SubscriptionOptions);

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.is_empty() {
            return None;
        }
        let filter = self.cursor.string().ok()?;
        let opts = if self.options {
            SubscriptionOptions::from_byte(self.cursor.u8().ok()?, self.version).ok()?
        } else {
            SubscriptionOptions::default()
        };
        Some((filter, opts))
    }
}

/// SUBACK or UNSUBACK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubAck<'a> {
    pub pkid: u16,
    pub properties: Properties<'a>,
    /// One reason code per filter. Empty for a v3.1.1 UNSUBACK.
    pub codes: &'a [u8],
}

/// DISCONNECT or AUTH.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReasonOnly<'a> {
    pub reason: u8,
    pub properties: Properties<'a>,
}

/// A decoded packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Packet<'a> {
    Connect(Connect<'a>),
    ConnAck(ConnAck<'a>),
    Publish(Publish<'a>),
    PubAck(Ack<'a>),
    PubRec(Ack<'a>),
    PubRel(Ack<'a>),
    PubComp(Ack<'a>),
    Subscribe(Subscribe<'a>),
    SubAck(SubAck<'a>),
    Unsubscribe(Unsubscribe<'a>),
    UnsubAck(SubAck<'a>),
    PingReq,
    PingResp,
    Disconnect(ReasonOnly<'a>),
    Auth(ReasonOnly<'a>),
}

/// Decodes one packet from the front of `buf`.
///
/// Returns `Ok(None)` if `buf` does not hold a whole packet yet, and the
/// packet with its wire size otherwise. `version` is the negotiated version;
/// CONNECT carries its own. Packets larger than `max_size` are rejected as
/// soon as the fixed header is complete.
#[inline]
pub fn decode(
    buf: &[u8],
    version: Version,
    max_size: usize,
) -> Result<Option<(Packet<'_>, usize)>, DecodeError> {
    let Some(fh) = fixed_header(buf)? else {
        return Ok(None);
    };
    let total = fh.packet_len();
    if total > max_size {
        return Err(DecodeError::TooLarge);
    }
    if buf.len() < total {
        return Ok(None);
    }
    let packet = decode_body(fh.byte1, &buf[fh.header_len..total], version)?;
    Ok(Some((packet, total)))
}

/// Flags each packet type must carry. PUBLISH flags are checked separately.
const REQUIRED_FLAGS: [u8; 16] = [0xff, 0, 0, 0, 0, 0, 2, 0, 2, 0, 2, 0, 0, 0, 0, 0];

/// Decodes a packet body given its first header byte.
pub fn decode_body(byte1: u8, body: &[u8], version: Version) -> Result<Packet<'_>, DecodeError> {
    let flags = byte1 & 0x0f;
    let kind = byte1 >> 4;
    let v5 = version.is_v5();
    if kind == 3 {
        return Ok(Packet::Publish(publish(flags, body, v5)?));
    }
    if REQUIRED_FLAGS[usize::from(kind)] != flags {
        return Err(DecodeError::Malformed);
    }
    Ok(match kind {
        1 => Packet::Connect(connect(body)?),
        2 => Packet::ConnAck(connack(body, v5)?),
        4 => Packet::PubAck(ack(body, v5)?),
        5 => Packet::PubRec(ack(body, v5)?),
        6 => Packet::PubRel(ack(body, v5)?),
        7 => Packet::PubComp(ack(body, v5)?),
        8 => Packet::Subscribe(subscribe(body, version)?),
        9 => Packet::SubAck(suback(body, v5)?),
        10 => Packet::Unsubscribe(unsubscribe(body, v5)?),
        11 => Packet::UnsubAck(suback(body, v5)?),
        12 if body.is_empty() => Packet::PingReq,
        13 if body.is_empty() => Packet::PingResp,
        14 => Packet::Disconnect(reason_only(body, v5)?),
        15 if v5 => Packet::Auth(reason_only(body, v5)?),
        _ => return Err(DecodeError::Malformed),
    })
}

/// A PUBLISH topic name: UTF-8 without U+0000 or wildcards. Empty is allowed
/// here for v5 topic aliases.
#[inline]
#[expect(
    unsafe_code,
    reason = "skips a second UTF-8 scan on the hot decode path"
)]
fn topic_name(b: &[u8]) -> Result<&str, DecodeError> {
    if b.iter().all(|&c| c < 0x80 && !matches!(c, 0 | b'+' | b'#')) {
        // SAFETY: every byte is ASCII, and ASCII is valid UTF-8.
        return Ok(unsafe { core::str::from_utf8_unchecked(b) });
    }
    if b.iter().any(|&c| matches!(c, 0 | b'+' | b'#')) {
        return Err(if b.contains(&0) {
            DecodeError::InvalidUtf8
        } else {
            DecodeError::InvalidTopic
        });
    }
    core::str::from_utf8(b).map_err(|_| DecodeError::InvalidUtf8)
}

#[inline]
fn publish(flags: u8, body: &[u8], v5: bool) -> Result<Publish<'_>, DecodeError> {
    let qos = QoS::try_from((flags >> 1) & 0x03)?;
    let dup = flags & 0x08 != 0;
    if dup && qos == QoS::AtMostOnce {
        return Err(DecodeError::Malformed);
    }
    let mut c = Cursor::new(body);
    let topic_raw = c.binary()?;
    let pkid = if qos == QoS::AtMostOnce {
        0
    } else {
        let id = c.u16()?;
        if id == 0 {
            return Err(DecodeError::Protocol);
        }
        id
    };
    let properties = if v5 {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    let topic = topic_name(topic_raw)?;
    if topic.is_empty() && (!v5 || properties.get(crate::property::TOPIC_ALIAS).is_none()) {
        return Err(DecodeError::InvalidTopic);
    }
    Ok(Publish {
        dup,
        qos,
        retain: flags & 0x01 != 0,
        topic,
        pkid,
        properties,
        payload: c.rest(),
    })
}

fn connect(body: &[u8]) -> Result<Connect<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    let name = c.binary()?;
    let level = c.u8()?;
    let version = match (name, level) {
        (b"MQTT", 4) => Version::V311,
        (b"MQTT", 5) => Version::V5,
        (b"MQIsdp", 3) => Version::V31,
        (b"MQTT" | b"MQIsdp", l) => return Err(DecodeError::UnsupportedVersion(l)),
        _ => return Err(DecodeError::Protocol),
    };
    let flags = c.u8()?;
    if flags & 0x01 != 0 {
        return Err(DecodeError::Malformed);
    }
    let keep_alive = c.u16()?;
    let properties = if version.is_v5() {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    let client_id = c.string()?;
    let will = will(&mut c, flags, version.is_v5())?;
    let username = if flags & 0x80 != 0 {
        Some(c.string()?)
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        Some(c.binary()?)
    } else {
        None
    };
    if !c.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(Connect {
        version,
        clean_start: flags & 0x02 != 0,
        keep_alive,
        properties,
        client_id,
        will,
        username,
        password,
    })
}

fn will<'a>(c: &mut Cursor<'a>, flags: u8, v5: bool) -> Result<Option<Will<'a>>, DecodeError> {
    if flags & 0x04 == 0 {
        if flags & 0x38 != 0 {
            return Err(DecodeError::Malformed);
        }
        return Ok(None);
    }
    let properties = if v5 {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    let topic = c.string()?;
    validate_name(topic)?;
    Ok(Some(Will {
        qos: QoS::try_from((flags >> 3) & 0x03)?,
        retain: flags & 0x20 != 0,
        properties,
        topic,
        payload: c.binary()?,
    }))
}

fn connack(body: &[u8], v5: bool) -> Result<ConnAck<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    let flags = c.u8()?;
    if flags & 0xfe != 0 {
        return Err(DecodeError::Malformed);
    }
    let code = c.u8()?;
    let properties = if v5 && !c.is_empty() {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    Ok(ConnAck {
        session_present: flags & 0x01 != 0,
        code,
        properties,
    })
}

#[inline]
fn ack(body: &[u8], v5: bool) -> Result<Ack<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    let pkid = c.u16()?;
    let mut reason = 0;
    let mut properties = Properties::EMPTY;
    if v5 {
        if !c.is_empty() {
            reason = c.u8()?;
        }
        if !c.is_empty() {
            properties = c.properties()?;
        }
    }
    if !c.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(Ack {
        pkid,
        reason,
        properties,
    })
}

fn packet_id(c: &mut Cursor<'_>) -> Result<u16, DecodeError> {
    match c.u16()? {
        0 => Err(DecodeError::Protocol),
        id => Ok(id),
    }
}

fn subscribe(body: &[u8], version: Version) -> Result<Subscribe<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    let pkid = packet_id(&mut c)?;
    let properties = if version.is_v5() {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    let raw = c.rest();
    let mut check = Cursor::new(raw);
    if check.is_empty() {
        return Err(DecodeError::Protocol);
    }
    while !check.is_empty() {
        validate_filter(check.string()?)?;
        SubscriptionOptions::from_byte(check.u8()?, version)?;
    }
    Ok(Subscribe {
        pkid,
        properties,
        version,
        raw,
    })
}

fn unsubscribe(body: &[u8], v5: bool) -> Result<Unsubscribe<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    let pkid = packet_id(&mut c)?;
    let properties = if v5 {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    let raw = c.rest();
    let mut check = Cursor::new(raw);
    if check.is_empty() {
        return Err(DecodeError::Protocol);
    }
    while !check.is_empty() {
        validate_filter(check.string()?)?;
    }
    Ok(Unsubscribe {
        pkid,
        properties,
        raw,
    })
}

fn suback(body: &[u8], v5: bool) -> Result<SubAck<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    let pkid = c.u16()?;
    let properties = if v5 {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    Ok(SubAck {
        pkid,
        properties,
        codes: c.rest(),
    })
}

fn reason_only(body: &[u8], v5: bool) -> Result<ReasonOnly<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    if !v5 {
        if !c.is_empty() {
            return Err(DecodeError::Malformed);
        }
        return Ok(ReasonOnly {
            reason: 0,
            properties: Properties::EMPTY,
        });
    }
    let reason = if c.is_empty() { 0 } else { c.u8()? };
    let properties = if c.remaining() > 0 {
        c.properties()?
    } else {
        Properties::EMPTY
    };
    if !c.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(ReasonOnly { reason, properties })
}

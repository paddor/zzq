//! MQTT-SN 1.2 codec.
//!
//! One message per datagram. [`decode`] borrows from the datagram and
//! [`encode`] writes into a [`Sink`].

use crate::cursor::{Cursor, utf8};
use crate::{DecodeError, QoS, Sink};

/// Return code: accepted.
pub const ACCEPTED: u8 = 0x00;
/// Return code: rejected, congestion.
pub const REJECTED_CONGESTION: u8 = 0x01;
/// Return code: rejected, invalid topic ID.
pub const REJECTED_INVALID_TOPIC_ID: u8 = 0x02;
/// Return code: rejected, not supported.
pub const REJECTED_NOT_SUPPORTED: u8 = 0x03;

/// Protocol ID in CONNECT.
pub const PROTOCOL_ID: u8 = 0x01;

/// Longest message: its length field is a `u16`.
pub const MAX_LEN: usize = 65_535;

/// MQTT-SN QoS, which adds QoS -1: publish without a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnQoS {
    /// QoS 0.
    #[default]
    AtMostOnce,
    /// QoS 1.
    AtLeastOnce,
    /// QoS 2.
    ExactlyOnce,
    /// QoS -1: no connection, predefined or short topics only.
    NoConnection,
}

impl SnQoS {
    /// The MQTT QoS a gateway uses for this level.
    pub const fn mqtt(self) -> QoS {
        match self {
            SnQoS::AtMostOnce | SnQoS::NoConnection => QoS::AtMostOnce,
            SnQoS::AtLeastOnce => QoS::AtLeastOnce,
            SnQoS::ExactlyOnce => QoS::ExactlyOnce,
        }
    }

    const fn bits(self) -> u8 {
        match self {
            SnQoS::AtMostOnce => 0,
            SnQoS::AtLeastOnce => 1,
            SnQoS::ExactlyOnce => 2,
            SnQoS::NoConnection => 3,
        }
    }

    const fn from_bits(b: u8) -> SnQoS {
        match b & 0x03 {
            0 => SnQoS::AtMostOnce,
            1 => SnQoS::AtLeastOnce,
            2 => SnQoS::ExactlyOnce,
            _ => SnQoS::NoConnection,
        }
    }
}

/// How a PUBLISH names its topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Topic {
    /// A topic ID from REGISTER.
    Id(u16),
    /// A topic ID both sides know in advance.
    Predefined(u16),
    /// A two-character topic name.
    Short([u8; 2]),
}

impl Topic {
    const fn id_type(self) -> u8 {
        match self {
            Topic::Id(_) => 0,
            Topic::Predefined(_) => 1,
            Topic::Short(_) => 2,
        }
    }

    fn put<S: Sink + ?Sized>(self, out: &mut S) {
        match self {
            Topic::Id(id) | Topic::Predefined(id) => out.put_u16(id),
            Topic::Short(s) => out.put(&s),
        }
    }
}

/// How a SUBSCRIBE or UNSUBSCRIBE names its topic or filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Filter<'a> {
    /// A topic name or filter.
    Name(&'a str),
    /// A topic ID both sides know in advance.
    Predefined(u16),
    /// A two-character topic name.
    Short([u8; 2]),
}

impl Filter<'_> {
    const fn id_type(&self) -> u8 {
        match self {
            Filter::Name(_) => 0,
            Filter::Predefined(_) => 1,
            Filter::Short(_) => 2,
        }
    }

    const fn encoded_len(&self) -> usize {
        match self {
            Filter::Name(n) => n.len(),
            _ => 2,
        }
    }

    fn put<S: Sink + ?Sized>(&self, out: &mut S) {
        match self {
            Filter::Name(n) => out.put(n.as_bytes()),
            Filter::Predefined(id) => out.put_u16(*id),
            Filter::Short(s) => out.put(s),
        }
    }
}

/// The flags byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[expect(clippy::struct_excessive_bools, reason = "mirrors the flags byte")]
pub struct Flags {
    /// The message is a retransmission.
    pub dup: bool,
    /// QoS of a PUBLISH, or the requested or granted QoS.
    pub qos: SnQoS,
    /// The message is or should be retained.
    pub retain: bool,
    /// CONNECT: the client sends a will.
    pub will: bool,
    /// CONNECT: start a new session.
    pub clean_session: bool,
}

impl Flags {
    const fn byte(self, topic_id_type: u8) -> u8 {
        let mut b = (self.qos.bits() << 5) | (topic_id_type & 0x03);
        if self.dup {
            b |= 0x80;
        }
        if self.retain {
            b |= 0x10;
        }
        if self.will {
            b |= 0x08;
        }
        if self.clean_session {
            b |= 0x04;
        }
        b
    }

    const fn parse(b: u8) -> (Flags, u8) {
        let f = Flags {
            dup: b & 0x80 != 0,
            qos: SnQoS::from_bits(b >> 5),
            retain: b & 0x10 != 0,
            will: b & 0x08 != 0,
            clean_session: b & 0x04 != 0,
        };
        (f, b & 0x03)
    }
}

/// An MQTT-SN message. Message IDs match a reply to its request; topic IDs
/// come from REGISTER or are predefined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Packet<'a> {
    /// A gateway announces itself.
    Advertise {
        /// Gateway ID.
        gw_id: u8,
        /// Seconds until the next ADVERTISE.
        duration: u16,
    },
    /// A client looks for a gateway.
    SearchGw {
        /// Broadcast radius in hops.
        radius: u8,
    },
    /// A gateway or client answers SEARCHGW.
    GwInfo {
        /// Gateway ID.
        gw_id: u8,
        /// Gateway address, empty when a gateway sends it.
        address: &'a [u8],
    },
    /// A client connects, or wakes up from sleep.
    Connect {
        /// Will and clean session flags.
        flags: Flags,
        /// Keep alive in seconds.
        duration: u16,
        /// Client ID.
        client_id: &'a str,
    },
    /// The gateway answers CONNECT.
    ConnAck {
        /// Return code, such as [`ACCEPTED`].
        code: u8,
    },
    /// The gateway asks for the will topic.
    WillTopicReq,
    /// The will topic. `None` deletes the will.
    WillTopic {
        /// Will QoS and retain flag, and the will topic name.
        will: Option<(Flags, &'a str)>,
    },
    /// The gateway asks for the will message.
    WillMsgReq,
    /// The will message.
    WillMsg {
        /// Will payload.
        message: &'a [u8],
    },
    /// Assigns a topic ID to a topic name.
    Register {
        /// The topic ID, 0 when a client asks for one.
        topic_id: u16,
        /// Message ID.
        msg_id: u16,
        /// Topic name.
        topic: &'a str,
    },
    /// Answers REGISTER.
    RegAck {
        /// The assigned topic ID.
        topic_id: u16,
        /// Message ID of the REGISTER.
        msg_id: u16,
        /// Return code.
        code: u8,
    },
    /// A message on a topic.
    Publish {
        /// DUP, QoS and retain flags.
        flags: Flags,
        /// The topic.
        topic: Topic,
        /// Message ID, 0 for QoS 0 and -1.
        msg_id: u16,
        /// Payload.
        data: &'a [u8],
    },
    /// Acks a QoS 1 PUBLISH, or rejects any PUBLISH.
    PubAck {
        /// Topic ID of the PUBLISH.
        topic_id: u16,
        /// Message ID of the PUBLISH.
        msg_id: u16,
        /// Return code.
        code: u8,
    },
    /// First QoS 2 ack.
    PubRec {
        /// Message ID of the PUBLISH.
        msg_id: u16,
    },
    /// Second QoS 2 step.
    PubRel {
        /// Message ID of the PUBLISH.
        msg_id: u16,
    },
    /// Last QoS 2 ack.
    PubComp {
        /// Message ID of the PUBLISH.
        msg_id: u16,
    },
    /// Subscribes to a topic or filter.
    Subscribe {
        /// DUP flag, requested QoS and topic ID type.
        flags: Flags,
        /// Message ID.
        msg_id: u16,
        /// The topic or filter.
        topic: Filter<'a>,
    },
    /// Answers SUBSCRIBE.
    SubAck {
        /// Granted QoS.
        flags: Flags,
        /// Topic ID for a plain topic name, else 0.
        topic_id: u16,
        /// Message ID of the SUBSCRIBE.
        msg_id: u16,
        /// Return code.
        code: u8,
    },
    /// Unsubscribes from a topic or filter.
    Unsubscribe {
        /// Topic ID type.
        flags: Flags,
        /// Message ID.
        msg_id: u16,
        /// The topic or filter.
        topic: Filter<'a>,
    },
    /// Answers UNSUBSCRIBE.
    UnsubAck {
        /// Message ID of the UNSUBSCRIBE.
        msg_id: u16,
    },
    /// Keep alive. A sleeping client wakes up with its client ID.
    PingReq {
        /// Client ID of a sleeping client.
        client_id: Option<&'a str>,
    },
    /// Answers PINGREQ.
    PingResp,
    /// Ends the connection, or asks to sleep.
    Disconnect {
        /// Seconds to sleep.
        duration: Option<u16>,
    },
    /// Changes the will topic. `None` deletes the will.
    WillTopicUpd {
        /// Will QoS and retain flag, and the will topic name.
        will: Option<(Flags, &'a str)>,
    },
    /// Answers WILLTOPICUPD.
    WillTopicResp {
        /// Return code.
        code: u8,
    },
    /// Changes the will message.
    WillMsgUpd {
        /// Will payload.
        message: &'a [u8],
    },
    /// Answers WILLMSGUPD.
    WillMsgResp {
        /// Return code.
        code: u8,
    },
    /// A message relayed by a forwarder.
    Encapsulated {
        /// Control byte with the broadcast radius.
        ctrl: u8,
        /// The wireless node ID of the client.
        node_id: &'a [u8],
        /// The relayed message.
        message: &'a [u8],
    },
}

mod kind {
    pub(super) const ADVERTISE: u8 = 0x00;
    pub(super) const SEARCHGW: u8 = 0x01;
    pub(super) const GWINFO: u8 = 0x02;
    pub(super) const CONNECT: u8 = 0x04;
    pub(super) const CONNACK: u8 = 0x05;
    pub(super) const WILLTOPICREQ: u8 = 0x06;
    pub(super) const WILLTOPIC: u8 = 0x07;
    pub(super) const WILLMSGREQ: u8 = 0x08;
    pub(super) const WILLMSG: u8 = 0x09;
    pub(super) const REGISTER: u8 = 0x0a;
    pub(super) const REGACK: u8 = 0x0b;
    pub(super) const PUBLISH: u8 = 0x0c;
    pub(super) const PUBACK: u8 = 0x0d;
    pub(super) const PUBCOMP: u8 = 0x0e;
    pub(super) const PUBREC: u8 = 0x0f;
    pub(super) const PUBREL: u8 = 0x10;
    pub(super) const SUBSCRIBE: u8 = 0x12;
    pub(super) const SUBACK: u8 = 0x13;
    pub(super) const UNSUBSCRIBE: u8 = 0x14;
    pub(super) const UNSUBACK: u8 = 0x15;
    pub(super) const PINGREQ: u8 = 0x16;
    pub(super) const PINGRESP: u8 = 0x17;
    pub(super) const DISCONNECT: u8 = 0x18;
    pub(super) const WILLTOPICUPD: u8 = 0x1a;
    pub(super) const WILLTOPICRESP: u8 = 0x1b;
    pub(super) const WILLMSGUPD: u8 = 0x1c;
    pub(super) const WILLMSGRESP: u8 = 0x1d;
    pub(super) const ENCAPSULATED: u8 = 0xfe;
}

/// Decodes the message at the start of `datagram`. Returns it with its
/// length.
///
/// # Errors
///
/// A [`DecodeError`] if the datagram is shorter than its length field, has
/// an unknown message type, or a field is truncated or not valid UTF-8.
pub fn decode(datagram: &[u8]) -> Result<(Packet<'_>, usize), DecodeError> {
    let (len, header) = match datagram.first() {
        Some(0x01) => {
            let hi = *datagram.get(1).ok_or(DecodeError::Malformed)?;
            let lo = *datagram.get(2).ok_or(DecodeError::Malformed)?;
            (usize::from(u16::from_be_bytes([hi, lo])), 3)
        }
        Some(&n) => (usize::from(n), 1),
        None => return Err(DecodeError::Malformed),
    };
    if len <= header || len > datagram.len() {
        return Err(DecodeError::Malformed);
    }
    let kind = datagram[header];
    let body = &datagram[header + 1..len];
    if kind == kind::ENCAPSULATED {
        // The length covers the encapsulation header only; the relayed
        // message runs to the end of the datagram.
        let (&ctrl, node_id) = body.split_first().ok_or(DecodeError::Malformed)?;
        let message = &datagram[len..];
        return Ok((
            Packet::Encapsulated {
                ctrl,
                node_id,
                message,
            },
            datagram.len(),
        ));
    }
    Ok((body_of(kind, body)?, len))
}

fn topic_of(id_type: u8, c: &mut Cursor<'_>) -> Result<Topic, DecodeError> {
    Ok(match id_type {
        0 => Topic::Id(c.u16()?),
        1 => Topic::Predefined(c.u16()?),
        2 => {
            let b = c.bytes(2)?;
            Topic::Short([b[0], b[1]])
        }
        _ => return Err(DecodeError::Malformed),
    })
}

fn filter_of<'a>(id_type: u8, c: &mut Cursor<'a>) -> Result<Filter<'a>, DecodeError> {
    Ok(match id_type {
        0 => Filter::Name(utf8(c.rest())?),
        1 => Filter::Predefined(c.u16()?),
        2 => {
            let b = c.bytes(2)?;
            Filter::Short([b[0], b[1]])
        }
        _ => return Err(DecodeError::Malformed),
    })
}

fn will_of<'a>(c: &mut Cursor<'a>) -> Result<Option<(Flags, &'a str)>, DecodeError> {
    if c.is_empty() {
        return Ok(None);
    }
    let (flags, _) = Flags::parse(c.u8()?);
    Ok(Some((flags, utf8(c.rest())?)))
}

#[expect(clippy::too_many_lines, reason = "one arm per message type")]
fn body_of(kind: u8, body: &[u8]) -> Result<Packet<'_>, DecodeError> {
    let mut c = Cursor::new(body);
    let p = match kind {
        kind::ADVERTISE => Packet::Advertise {
            gw_id: c.u8()?,
            duration: c.u16()?,
        },
        kind::SEARCHGW => Packet::SearchGw { radius: c.u8()? },
        kind::GWINFO => Packet::GwInfo {
            gw_id: c.u8()?,
            address: c.rest(),
        },
        kind::CONNECT => {
            let (flags, _) = Flags::parse(c.u8()?);
            if c.u8()? != PROTOCOL_ID {
                return Err(DecodeError::Protocol);
            }
            Packet::Connect {
                flags,
                duration: c.u16()?,
                client_id: utf8(c.rest())?,
            }
        }
        kind::CONNACK => Packet::ConnAck { code: c.u8()? },
        kind::WILLTOPICREQ => Packet::WillTopicReq,
        kind::WILLTOPIC => Packet::WillTopic {
            will: will_of(&mut c)?,
        },
        kind::WILLMSGREQ => Packet::WillMsgReq,
        kind::WILLMSG => Packet::WillMsg { message: c.rest() },
        kind::REGISTER => Packet::Register {
            topic_id: c.u16()?,
            msg_id: c.u16()?,
            topic: utf8(c.rest())?,
        },
        kind::REGACK => Packet::RegAck {
            topic_id: c.u16()?,
            msg_id: c.u16()?,
            code: c.u8()?,
        },
        kind::PUBLISH => {
            let (flags, id_type) = Flags::parse(c.u8()?);
            let topic = topic_of(id_type, &mut c)?;
            Packet::Publish {
                flags,
                topic,
                msg_id: c.u16()?,
                data: c.rest(),
            }
        }
        kind::PUBACK => Packet::PubAck {
            topic_id: c.u16()?,
            msg_id: c.u16()?,
            code: c.u8()?,
        },
        kind::PUBREC => Packet::PubRec { msg_id: c.u16()? },
        kind::PUBREL => Packet::PubRel { msg_id: c.u16()? },
        kind::PUBCOMP => Packet::PubComp { msg_id: c.u16()? },
        kind::SUBSCRIBE | kind::UNSUBSCRIBE => {
            let (flags, id_type) = Flags::parse(c.u8()?);
            let msg_id = c.u16()?;
            let topic = filter_of(id_type, &mut c)?;
            if kind == kind::SUBSCRIBE {
                Packet::Subscribe {
                    flags,
                    msg_id,
                    topic,
                }
            } else {
                Packet::Unsubscribe {
                    flags,
                    msg_id,
                    topic,
                }
            }
        }
        kind::SUBACK => Packet::SubAck {
            flags: Flags::parse(c.u8()?).0,
            topic_id: c.u16()?,
            msg_id: c.u16()?,
            code: c.u8()?,
        },
        kind::UNSUBACK => Packet::UnsubAck { msg_id: c.u16()? },
        kind::PINGREQ => Packet::PingReq {
            client_id: if c.is_empty() {
                None
            } else {
                Some(utf8(c.rest())?)
            },
        },
        kind::PINGRESP => Packet::PingResp,
        kind::DISCONNECT => Packet::Disconnect {
            duration: if c.is_empty() { None } else { Some(c.u16()?) },
        },
        kind::WILLTOPICUPD => Packet::WillTopicUpd {
            will: will_of(&mut c)?,
        },
        kind::WILLTOPICRESP => Packet::WillTopicResp { code: c.u8()? },
        kind::WILLMSGUPD => Packet::WillMsgUpd { message: c.rest() },
        kind::WILLMSGRESP => Packet::WillMsgResp { code: c.u8()? },
        _ => return Err(DecodeError::Malformed),
    };
    if !c.is_empty() {
        return Err(DecodeError::Malformed);
    }
    Ok(p)
}

fn body_len(p: &Packet<'_>) -> usize {
    match p {
        Packet::Advertise { .. } => 3,
        Packet::SearchGw { .. }
        | Packet::ConnAck { .. }
        | Packet::WillTopicResp { .. }
        | Packet::WillMsgResp { .. } => 1,
        Packet::GwInfo { address, .. } => 1 + address.len(),
        Packet::Connect { client_id, .. } => 4 + client_id.len(),
        Packet::WillTopicReq | Packet::WillMsgReq | Packet::PingResp => 0,
        Packet::WillTopic { will } | Packet::WillTopicUpd { will } => {
            will.map_or(0, |(_, topic)| 1 + topic.len())
        }
        Packet::WillMsg { message } | Packet::WillMsgUpd { message } => message.len(),
        Packet::Register { topic, .. } => 4 + topic.len(),
        Packet::RegAck { .. } | Packet::PubAck { .. } => 5,
        Packet::Publish { data, .. } => 5 + data.len(),
        Packet::PubRec { .. }
        | Packet::PubRel { .. }
        | Packet::PubComp { .. }
        | Packet::UnsubAck { .. } => 2,
        Packet::Subscribe { topic, .. } | Packet::Unsubscribe { topic, .. } => {
            3 + topic.encoded_len()
        }
        Packet::SubAck { .. } => 6,
        Packet::PingReq { client_id } => client_id.map_or(0, str::len),
        Packet::Disconnect { duration } => {
            if duration.is_some() {
                2
            } else {
                0
            }
        }
        Packet::Encapsulated { node_id, .. } => 1 + node_id.len(),
    }
}

/// Value of the length field: the whole message, or only the header of
/// an Encapsulated frame.
fn length_field(p: &Packet<'_>) -> usize {
    let body = 1 + body_len(p);
    if body < 255 { body + 1 } else { body + 3 }
}

/// Wire size of `p`.
pub fn encoded_len(p: &Packet<'_>) -> usize {
    match p {
        Packet::Encapsulated { message, .. } => length_field(p) + message.len(),
        _ => length_field(p),
    }
}

/// Writes `p`.
///
/// # Panics
///
/// In debug builds, if [`encoded_len`] is over [`MAX_LEN`]. Release builds
/// write a malformed length.
#[expect(clippy::too_many_lines, reason = "one arm per message type")]
pub fn encode<S: Sink + ?Sized>(p: &Packet<'_>, out: &mut S) {
    let len = length_field(p);
    debug_assert!(len <= MAX_LEN, "message over sn::MAX_LEN");
    if len <= 255 {
        out.put_u8(len as u8);
    } else {
        out.put_u8(0x01);
        out.put_u16(len as u16);
    }
    out.put_u8(kind_of(p));
    match *p {
        Packet::Advertise { gw_id, duration } => {
            out.put_u8(gw_id);
            out.put_u16(duration);
        }
        Packet::SearchGw { radius } => out.put_u8(radius),
        Packet::GwInfo { gw_id, address } => {
            out.put_u8(gw_id);
            out.put(address);
        }
        Packet::Connect {
            flags,
            duration,
            client_id,
        } => {
            out.put_u8(flags.byte(0));
            out.put_u8(PROTOCOL_ID);
            out.put_u16(duration);
            out.put(client_id.as_bytes());
        }
        Packet::ConnAck { code }
        | Packet::WillTopicResp { code }
        | Packet::WillMsgResp { code } => out.put_u8(code),
        Packet::WillTopicReq | Packet::WillMsgReq | Packet::PingResp => {}
        Packet::WillTopic { will } | Packet::WillTopicUpd { will } => {
            if let Some((f, topic)) = will {
                out.put_u8(f.byte(0));
                out.put(topic.as_bytes());
            }
        }
        Packet::WillMsg { message } | Packet::WillMsgUpd { message } => out.put(message),
        Packet::Register {
            topic_id,
            msg_id,
            topic,
        } => {
            out.put_u16(topic_id);
            out.put_u16(msg_id);
            out.put(topic.as_bytes());
        }
        Packet::RegAck {
            topic_id,
            msg_id,
            code,
        }
        | Packet::PubAck {
            topic_id,
            msg_id,
            code,
        } => {
            out.put_u16(topic_id);
            out.put_u16(msg_id);
            out.put_u8(code);
        }
        Packet::Publish {
            flags,
            topic,
            msg_id,
            data,
        } => {
            out.put_u8(flags.byte(topic.id_type()));
            topic.put(out);
            out.put_u16(msg_id);
            out.put(data);
        }
        Packet::PubRec { msg_id }
        | Packet::PubRel { msg_id }
        | Packet::PubComp { msg_id }
        | Packet::UnsubAck { msg_id } => {
            out.put_u16(msg_id);
        }
        Packet::Subscribe {
            flags,
            msg_id,
            topic,
        }
        | Packet::Unsubscribe {
            flags,
            msg_id,
            topic,
        } => {
            out.put_u8(flags.byte(topic.id_type()));
            out.put_u16(msg_id);
            topic.put(out);
        }
        Packet::SubAck {
            flags,
            topic_id,
            msg_id,
            code,
        } => {
            out.put_u8(flags.byte(0));
            out.put_u16(topic_id);
            out.put_u16(msg_id);
            out.put_u8(code);
        }
        Packet::PingReq { client_id } => {
            if let Some(id) = client_id {
                out.put(id.as_bytes());
            }
        }
        Packet::Disconnect { duration } => {
            if let Some(d) = duration {
                out.put_u16(d);
            }
        }
        Packet::Encapsulated {
            ctrl,
            node_id,
            message,
        } => {
            out.put_u8(ctrl);
            out.put(node_id);
            out.put(message);
        }
    }
}

const fn kind_of(p: &Packet<'_>) -> u8 {
    match p {
        Packet::Advertise { .. } => kind::ADVERTISE,
        Packet::SearchGw { .. } => kind::SEARCHGW,
        Packet::GwInfo { .. } => kind::GWINFO,
        Packet::Connect { .. } => kind::CONNECT,
        Packet::ConnAck { .. } => kind::CONNACK,
        Packet::WillTopicReq => kind::WILLTOPICREQ,
        Packet::WillTopic { .. } => kind::WILLTOPIC,
        Packet::WillMsgReq => kind::WILLMSGREQ,
        Packet::WillMsg { .. } => kind::WILLMSG,
        Packet::Register { .. } => kind::REGISTER,
        Packet::RegAck { .. } => kind::REGACK,
        Packet::Publish { .. } => kind::PUBLISH,
        Packet::PubAck { .. } => kind::PUBACK,
        Packet::PubRec { .. } => kind::PUBREC,
        Packet::PubRel { .. } => kind::PUBREL,
        Packet::PubComp { .. } => kind::PUBCOMP,
        Packet::Subscribe { .. } => kind::SUBSCRIBE,
        Packet::SubAck { .. } => kind::SUBACK,
        Packet::Unsubscribe { .. } => kind::UNSUBSCRIBE,
        Packet::UnsubAck { .. } => kind::UNSUBACK,
        Packet::PingReq { .. } => kind::PINGREQ,
        Packet::PingResp => kind::PINGRESP,
        Packet::Disconnect { .. } => kind::DISCONNECT,
        Packet::WillTopicUpd { .. } => kind::WILLTOPICUPD,
        Packet::WillTopicResp { .. } => kind::WILLTOPICRESP,
        Packet::WillMsgUpd { .. } => kind::WILLMSGUPD,
        Packet::WillMsgResp { .. } => kind::WILLMSGRESP,
        Packet::Encapsulated { .. } => kind::ENCAPSULATED,
    }
}

// The tests encode into `Vec`, a `Sink` only with `alloc`.
#[cfg(test)]
#[cfg(feature = "alloc")]
mod tests {
    use super::*;
    use std::vec::Vec;

    fn round_trip(p: &Packet<'_>) {
        let mut out = Vec::new();
        encode(p, &mut out);
        assert_eq!(out.len(), encoded_len(p), "{p:?}");
        let (back, n) = decode(&out).unwrap();
        assert_eq!(n, out.len());
        assert_eq!(&back, p);
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one sample per message type")]
    fn all_packets_round_trip() {
        let flags = Flags {
            dup: true,
            qos: SnQoS::ExactlyOnce,
            retain: true,
            will: true,
            clean_session: true,
        };
        let big = [7u8; 300];
        let inner = {
            let mut v = Vec::new();
            encode(&Packet::PingResp, &mut v);
            v
        };
        let packets = [
            Packet::Advertise {
                gw_id: 1,
                duration: 900,
            },
            Packet::SearchGw { radius: 3 },
            Packet::GwInfo {
                gw_id: 2,
                address: b"\x0a\x00\x00\x01",
            },
            Packet::Connect {
                flags,
                duration: 60,
                client_id: "sensor-1",
            },
            Packet::ConnAck { code: ACCEPTED },
            Packet::WillTopicReq,
            Packet::WillTopic {
                will: Some((flags, "w/t")),
            },
            Packet::WillTopic { will: None },
            Packet::WillMsgReq,
            Packet::WillMsg { message: b"bye" },
            Packet::Register {
                topic_id: 5,
                msg_id: 9,
                topic: "a/b",
            },
            Packet::RegAck {
                topic_id: 5,
                msg_id: 9,
                code: ACCEPTED,
            },
            Packet::Publish {
                flags,
                topic: Topic::Id(5),
                msg_id: 10,
                data: b"x",
            },
            Packet::Publish {
                flags,
                topic: Topic::Predefined(1),
                msg_id: 0,
                data: b"",
            },
            Packet::Publish {
                flags,
                topic: Topic::Short(*b"ab"),
                msg_id: 1,
                data: &big,
            },
            Packet::PubAck {
                topic_id: 5,
                msg_id: 10,
                code: REJECTED_INVALID_TOPIC_ID,
            },
            Packet::PubRec { msg_id: 1 },
            Packet::PubRel { msg_id: 2 },
            Packet::PubComp { msg_id: 3 },
            Packet::Subscribe {
                flags,
                msg_id: 4,
                topic: Filter::Name("a/+"),
            },
            Packet::Subscribe {
                flags,
                msg_id: 4,
                topic: Filter::Predefined(7),
            },
            Packet::SubAck {
                flags: Flags::default(),
                topic_id: 6,
                msg_id: 4,
                code: ACCEPTED,
            },
            Packet::Unsubscribe {
                flags: Flags::default(),
                msg_id: 8,
                topic: Filter::Short(*b"zz"),
            },
            Packet::UnsubAck { msg_id: 8 },
            Packet::PingReq {
                client_id: Some("sensor-1"),
            },
            Packet::PingReq { client_id: None },
            Packet::PingResp,
            Packet::Disconnect {
                duration: Some(300),
            },
            Packet::Disconnect { duration: None },
            Packet::WillTopicUpd {
                will: Some((Flags::default(), "w")),
            },
            Packet::WillTopicResp { code: ACCEPTED },
            Packet::WillMsgUpd { message: b"m" },
            Packet::WillMsgResp { code: ACCEPTED },
            Packet::Encapsulated {
                ctrl: 0,
                node_id: b"\x01\x02",
                message: &inner,
            },
        ];
        for p in &packets {
            round_trip(p);
        }
    }

    #[test]
    fn rejects_bad_lengths() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[5, 0x16]).is_err());
        assert!(decode(&[1]).is_err());
        assert!(decode(&[0x01, 0x00]).is_err());
        assert!(decode(&[2, 0x04]).is_err());
    }
}

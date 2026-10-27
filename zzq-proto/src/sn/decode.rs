//! MQTT-SN messages from datagrams.

use super::{Filter, Flags, PROTOCOL_ID, Packet, Topic, kind};
use crate::DecodeError;
use crate::cursor::{Cursor, utf8};

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

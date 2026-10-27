//! MQTT-SN messages to bytes.

use super::{MAX_LEN, PROTOCOL_ID, Packet, kind};
use crate::Sink;

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

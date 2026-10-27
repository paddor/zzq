//! Encoding packets into the session's output.

use std::time::Instant;

use zzq_proto::encode::{self, AckFields, PublishHeader};
use zzq_proto::property::{MESSAGE_EXPIRY_INTERVAL, Property, SUBSCRIPTION_IDENTIFIER};
use zzq_proto::{PacketType, QoS};

use crate::index::SubIds;
use crate::msg::{Publication, same_family};

use super::{Kept, Outcome, Session};

/// One PUBLISH to encode for the client.
pub(super) struct Fields<'a> {
    topic: &'a str,
    payload: &'a [u8],
    props: &'a [u8],
    qos: QoS,
    retain: bool,
    dup: bool,
    pkid: u16,
    sub_id: SubIds,
    expires_at: Option<Instant>,
}

impl<'a> Fields<'a> {
    pub(super) fn of(item: &'a Kept, pkid: u16, dup: bool) -> Fields<'a> {
        Fields {
            topic: item.msg.topic(),
            payload: item.msg.payload(),
            props: item.msg.props(),
            qos: item.qos,
            retain: item.retain,
            dup,
            pkid,
            sub_id: item.sub_id,
            expires_at: item.msg.expires_at,
        }
    }
}

impl Session {
    /// Delivers a QoS 0 message to a connected session.
    #[inline]
    pub(crate) fn deliver_qos0(
        &mut self,
        p: &Publication<'_>,
        retain: bool,
        sub_id: SubIds,
        max_out: usize,
    ) -> Outcome {
        if self.out.buf.len() >= max_out {
            return Outcome::BufferFull;
        }
        if let Some((raw, version)) = p.raw
            && sub_id.is_empty()
            && same_family(version, self.version)
            && raw.len() <= self.max_packet
        {
            self.out.buf.push((raw[0] & !0x01) | u8::from(retain));
            self.out.buf.extend_from_slice(&raw[1..]);
            return Outcome::Sent;
        }
        let item = Fields {
            topic: p.topic,
            payload: p.payload,
            props: p.props,
            qos: QoS::AtMostOnce,
            retain,
            dup: false,
            pkid: 0,
            sub_id,
            expires_at: p.expires_at,
        };
        if self.encode(&item) {
            Outcome::Sent
        } else {
            Outcome::TooLarge
        }
    }

    /// Writes an ack.
    pub(crate) fn ack(&mut self, kind: PacketType, pkid: u16, reason: u8) {
        let a = AckFields {
            reason,
            ..AckFields::new(kind, pkid)
        };
        encode::ack(&a, self.version, &mut self.out.buf);
    }

    pub(super) fn encode(&mut self, f: &Fields<'_>) -> bool {
        let mut props_buf = Vec::new();
        let props: &[u8] =
            if self.version.is_v5() && (!f.sub_id.is_empty() || f.expires_at.is_some()) {
                if let Some(at) = f.expires_at {
                    // Forward the time left, not the time the publisher asked for.
                    let left = at
                        .saturating_duration_since(Instant::now())
                        .as_secs()
                        .max(1);
                    let parsed = zzq_proto::Properties::try_from(f.props).unwrap_or_default();
                    for p in parsed.iter().filter(|p| p.id() != MESSAGE_EXPIRY_INTERVAL) {
                        p.encode(&mut props_buf);
                    }
                    Property::U32(
                        MESSAGE_EXPIRY_INTERVAL,
                        u32::try_from(left).unwrap_or(u32::MAX),
                    )
                    .encode(&mut props_buf);
                } else {
                    props_buf.extend_from_slice(f.props);
                }
                for id in f.sub_id.iter() {
                    Property::VarInt(SUBSCRIPTION_IDENTIFIER, id).encode(&mut props_buf);
                }
                &props_buf
            } else {
                f.props
            };
        let h = PublishHeader {
            version: self.version,
            dup: f.dup,
            qos: f.qos,
            retain: f.retain,
            topic: f.topic,
            pkid: f.pkid,
            properties: props,
        };
        if encode::packet_len(h.remaining(f.payload.len())) > self.max_packet {
            return false;
        }
        encode::publish(&h, f.payload, &mut self.out.buf);
        true
    }
}

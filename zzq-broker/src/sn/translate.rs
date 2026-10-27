//! Translation between MQTT-SN datagrams and MQTT packets.

use zzq_proto::encode::{self, AckFields, PublishHeader};
use zzq_proto::sn::{self, Flags, SnQoS, Topic};
use zzq_proto::{Packet, PacketType, QoS, SubscriptionOptions, Version};

use super::topics::plain;
use super::{Client, Inbound, MAX_SEND};

/// Publishes held for a topic while its REGISTER waits for the REGACK.
const HELD_MAX: usize = 256;

impl Client {
    /// Translates a datagram from the client into MQTT bytes in `out`.
    pub(super) async fn inbound(&mut self, packet: &sn::Packet<'_>, out: &mut Vec<u8>) -> Inbound {
        let v = Version::V311;
        match *packet {
            sn::Packet::Register { msg_id, topic, .. } => self.register(msg_id, topic).await,
            sn::Packet::RegAck { msg_id, code, .. } => {
                if let Some(topic_id) = self.topics.registering.remove(&msg_id) {
                    let held = self.topics.held.remove(&topic_id).unwrap_or_default();
                    if code == sn::ACCEPTED {
                        for d in held {
                            self.deliver(d).await;
                        }
                    } else {
                        // Refused: forget the ID, so the next message asks
                        // again, and drop what waited for it.
                        if let Some(name) = self.topics.by_id.remove(&topic_id) {
                            self.topics.ids.remove(&name);
                        }
                        for d in held {
                            self.drop_datagram(&d, out);
                        }
                    }
                }
            }
            sn::Packet::Publish { .. } => self.publish_in(packet, out).await,
            sn::Packet::PubAck { msg_id, .. } => ack(out, PacketType::PubAck, msg_id),
            sn::Packet::PubRec { msg_id } => ack(out, PacketType::PubRec, msg_id),
            sn::Packet::PubRel { msg_id } => ack(out, PacketType::PubRel, msg_id),
            sn::Packet::PubComp { msg_id } => ack(out, PacketType::PubComp, msg_id),
            sn::Packet::Subscribe {
                flags,
                msg_id,
                topic,
            } => self.subscribe_in(flags, msg_id, topic, out).await,
            sn::Packet::Unsubscribe { msg_id, topic, .. } => {
                if let Some(filter) = self.topic_name(topic) {
                    encode::unsubscribe(v, msg_id.max(1), &[], &[&filter], out);
                }
            }
            sn::Packet::PingReq { .. } => encode::pingreq(out),
            sn::Packet::Disconnect {
                duration: Some(seconds),
            } => return Inbound::Sleep(seconds),
            sn::Packet::Disconnect { duration: None } => {
                encode::reason_only(PacketType::Disconnect, v, 0, &[], out);
                return Inbound::Disconnect;
            }
            sn::Packet::WillTopicUpd { .. } => {
                self.send(&sn::Packet::WillTopicResp {
                    code: sn::REJECTED_NOT_SUPPORTED,
                })
                .await;
            }
            sn::Packet::WillMsgUpd { .. } => {
                self.send(&sn::Packet::WillMsgResp {
                    code: sn::REJECTED_NOT_SUPPORTED,
                })
                .await;
            }
            _ => {}
        }
        Inbound::Continue
    }

    /// REGISTER from the client: assigns a topic ID.
    async fn register(&mut self, msg_id: u16, topic: &str) {
        let (topic_id, code) = if zzq_proto::topic::validate_name(topic).is_err() {
            (0, sn::REJECTED_INVALID_TOPIC_ID)
        } else if let Some(id) = self.topic_id_for(topic) {
            (id, sn::ACCEPTED)
        } else {
            (0, sn::REJECTED_CONGESTION)
        };
        self.send(&sn::Packet::RegAck {
            topic_id,
            msg_id,
            code,
        })
        .await;
    }

    /// PUBLISH from the client.
    async fn publish_in(&mut self, packet: &sn::Packet<'_>, out: &mut Vec<u8>) {
        let sn::Packet::Publish {
            flags,
            topic,
            msg_id,
            data,
        } = *packet
        else {
            return;
        };
        let Some(name) = self.topic_name(topic) else {
            let topic_id = match topic {
                Topic::Id(id) | Topic::Predefined(id) => id,
                _ => 0,
            };
            let code = sn::REJECTED_INVALID_TOPIC_ID;
            self.send(&sn::Packet::PubAck {
                topic_id,
                msg_id,
                code,
            })
            .await;
            return;
        };
        let qos = flags.qos.mqtt();
        if qos == QoS::AtLeastOnce
            && let Topic::Id(id) | Topic::Predefined(id) = topic
        {
            self.replies.puback_topic.insert(msg_id, id);
        }
        let h = PublishHeader {
            version: Version::V311,
            dup: flags.dup && qos != QoS::AtMostOnce,
            qos,
            retain: flags.retain,
            topic: &name,
            pkid: if qos == QoS::AtMostOnce {
                0
            } else {
                msg_id.max(1)
            },
            properties: &[],
        };
        encode::publish(&h, data, out);
    }

    /// SUBSCRIBE from the client.
    async fn subscribe_in(
        &mut self,
        flags: Flags,
        msg_id: u16,
        topic: Topic<'_>,
        out: &mut Vec<u8>,
    ) {
        let filter = self.topic_name(topic);
        // A plain filter gets its topic ID now, so SUBACK has one.
        let code = match &filter {
            None => sn::REJECTED_INVALID_TOPIC_ID,
            Some(f) if plain(f) && self.topic_id_for(f).is_none() => sn::REJECTED_CONGESTION,
            Some(_) => sn::ACCEPTED,
        };
        let Some(filter) = filter.filter(|_| code == sn::ACCEPTED) else {
            self.send(&sn::Packet::SubAck {
                flags,
                topic_id: 0,
                msg_id,
                code,
            })
            .await;
            return;
        };
        let opts = SubscriptionOptions::from(flags.qos.mqtt());
        encode::subscribe(Version::V311, msg_id.max(1), &[], &[(&filter, opts)], out);
        self.replies.sub_filter.insert(msg_id.max(1), filter);
    }

    /// SUBACK fields for the client: granted QoS, the topic ID of a plain
    /// filter, and the return code.
    fn suback(&mut self, pkid: u16, mqtt_code: u8) -> (Flags, u16, u8) {
        let granted = mqtt_code < 0x80;
        let filter = self.replies.sub_filter.remove(&pkid).unwrap_or_default();
        let topic_id = if granted && plain(&filter) {
            self.topics.ids.get(&filter).copied().unwrap_or(0)
        } else {
            0
        };
        let qos = match mqtt_code {
            1 => SnQoS::AtLeastOnce,
            2 => SnQoS::ExactlyOnce,
            _ => SnQoS::AtMostOnce,
        };
        let code = if granted {
            sn::ACCEPTED
        } else {
            sn::REJECTED_NOT_SUPPORTED
        };
        (
            Flags {
                qos,
                ..Flags::default()
            },
            topic_id,
            code,
        )
    }

    /// Translates one MQTT packet from the broker into datagrams.
    pub(super) fn outbound(&mut self, packet: &Packet<'_>) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut push = |p: &sn::Packet<'_>| {
            let mut d = Vec::with_capacity(sn::encoded_len(p));
            sn::encode(p, &mut d);
            out.push(d);
        };
        match *packet {
            Packet::ConnAck(c) => {
                let code = if c.code == 0 {
                    sn::ACCEPTED
                } else {
                    sn::REJECTED_NOT_SUPPORTED
                };
                self.connack = Some(code);
                push(&sn::Packet::ConnAck { code });
            }
            Packet::Publish(p) => return self.publish_out(&p),
            Packet::PubAck(a) => {
                let topic_id = self.replies.puback_topic.remove(&a.pkid).unwrap_or(0);
                push(&sn::Packet::PubAck {
                    topic_id,
                    msg_id: a.pkid,
                    code: sn::ACCEPTED,
                });
            }
            Packet::PubRec(a) => push(&sn::Packet::PubRec { msg_id: a.pkid }),
            Packet::PubRel(a) if self.replies.dropped.remove(&a.pkid) => {
                ack(&mut self.to_broker, PacketType::PubComp, a.pkid);
            }
            Packet::PubRel(a) => push(&sn::Packet::PubRel { msg_id: a.pkid }),
            Packet::PubComp(a) => push(&sn::Packet::PubComp { msg_id: a.pkid }),
            Packet::SubAck(s) => {
                let (flags, topic_id, code) =
                    self.suback(s.pkid, s.codes.first().copied().unwrap_or(0x80));
                push(&sn::Packet::SubAck {
                    flags,
                    topic_id,
                    msg_id: s.pkid,
                    code,
                });
            }
            Packet::UnsubAck(s) => push(&sn::Packet::UnsubAck { msg_id: s.pkid }),
            Packet::PingResp => push(&sn::Packet::PingResp),
            _ => {}
        }
        out
    }

    /// A broker PUBLISH for the client. Registers the topic first if the
    /// client does not know it yet.
    fn publish_out(&mut self, p: &zzq_proto::packet::Publish<'_>) -> Vec<Vec<u8>> {
        let qos = match p.qos {
            QoS::AtMostOnce => SnQoS::AtMostOnce,
            QoS::AtLeastOnce => SnQoS::AtLeastOnce,
            QoS::ExactlyOnce => SnQoS::ExactlyOnce,
        };
        let flags = Flags {
            dup: p.dup,
            qos,
            retain: p.retain,
            ..Flags::default()
        };
        // The topic ID form is the longest PUBLISH header.
        let probe = sn::Packet::Publish {
            flags,
            topic: Topic::Id(0),
            msg_id: p.pkid,
            data: p.payload,
        };
        if sn::encoded_len(&probe) > MAX_SEND {
            self.drop_publish(p);
            return Vec::new();
        }
        let mut out = Vec::new();
        let Some((topic, pending)) = self.topic_out(p.topic, &mut out) else {
            self.drop_publish(p);
            return out;
        };
        let publish = sn::Packet::Publish {
            flags,
            topic,
            msg_id: p.pkid,
            data: p.payload,
        };
        let mut d = Vec::with_capacity(sn::encoded_len(&publish));
        sn::encode(&publish, &mut d);
        match pending {
            Some(id) => {
                let held = self.topics.held.entry(id).or_default();
                if held.len() < HELD_MAX {
                    held.push(d);
                } else {
                    self.drop_publish(p);
                }
            }
            None => out.push(d),
        }
        out
    }

    /// Drops a broker PUBLISH the client cannot get, for lack of a topic ID
    /// or because it does not fit a datagram, and acks it so the session's
    /// window moves on.
    fn drop_publish(&mut self, p: &zzq_proto::packet::Publish<'_>) {
        let mut acks = std::mem::take(&mut self.to_broker);
        self.drop_qos(p.qos, p.pkid, &mut acks);
        self.to_broker = acks;
    }

    /// Drops a translated PUBLISH datagram that waited for a REGACK, acking
    /// it to the broker in `out`.
    fn drop_datagram(&mut self, d: &[u8], out: &mut Vec<u8>) {
        if let Ok((sn::Packet::Publish { flags, msg_id, .. }, _)) = sn::decode(d) {
            self.drop_qos(flags.qos.mqtt(), msg_id, out);
        }
    }

    /// Acks a dropped message of `qos` to the broker in `out`.
    fn drop_qos(&mut self, qos: QoS, pkid: u16, out: &mut Vec<u8>) {
        match qos {
            QoS::AtMostOnce => {}
            QoS::AtLeastOnce => ack(out, PacketType::PubAck, pkid),
            QoS::ExactlyOnce => {
                self.replies.dropped.insert(pkid);
                ack(out, PacketType::PubRec, pkid);
            }
        }
    }
}

fn ack(out: &mut Vec<u8>, kind: PacketType, msg_id: u16) {
    encode::ack(&AckFields::new(kind, msg_id), Version::V311, out);
}

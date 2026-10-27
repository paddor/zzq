//! Packets from the broker, the messages they carry, and their acks.

use bytes::{Bytes, BytesMut};
use zzq_proto::encode::{self, AckFields};
use zzq_proto::packet::{Publish, decode};
use zzq_proto::property::{self, Property};
use zzq_proto::{Packet, PacketType, QoS};

use crate::{Error, Message, Result, Topic};

use super::io::complete_len;
use super::{Conn, MAX_PACKET, Pending};

impl Conn<'_> {
    /// Acks the messages the application took.
    pub(super) fn ack_taken(&mut self) {
        let mut acks = std::mem::take(&mut self.s.scratch.taken);
        self.s.inbox.take_acks(&mut acks);
        if !acks.is_empty() {
            for (pkid, qos) in &acks {
                if *qos == QoS::ExactlyOnce {
                    self.s.inbound.unconsumed.remove(pkid);
                }
            }
            let v = self.s.opts.version;
            self.out.push(|b| {
                for &(pkid, qos) in &acks {
                    let kind = if qos == QoS::ExactlyOnce {
                        PacketType::PubRec
                    } else {
                        PacketType::PubAck
                    };
                    encode::ack(&AckFields::new(kind, pkid), v, b);
                }
            });
            acks.clear();
        }
        self.s.scratch.taken = acks;
    }

    /// Handles the whole packets in `buf`, then hands their messages to the
    /// client handle. Stops at a full inbox, keeping the rest of the read
    /// until the handle makes room.
    pub(super) fn input(&mut self, buf: &mut BytesMut) -> Result<()> {
        let n = complete_len(buf)?;
        if n == 0 {
            return Ok(());
        }
        let chunk = buf.split_to(n).freeze();
        // Acks the handle gathered go out without a wake of their own.
        self.ack_taken();
        self.packets(chunk, 0)
    }

    /// Queues what waited once the handle made room, then handles the rest
    /// of the read.
    pub(super) fn resume(&mut self) -> Result<()> {
        if !self.deliver()? {
            return Ok(());
        }
        match self.s.scratch.rest.take() {
            Some((chunk, at)) => self.packets(chunk, at),
            None => Ok(()),
        }
    }

    /// Handles the packets of `chunk` from `at`.
    fn packets(&mut self, chunk: Bytes, mut at: usize) -> Result<()> {
        let mut res = Ok(());
        while at < chunk.len() {
            let Ok(Some((packet, n))) = decode(&chunk[at..], self.s.opts.version, MAX_PACKET)
            else {
                res = Err(Error::Protocol("malformed packet"));
                break;
            };
            at += n;
            match self.packet(packet, &chunk) {
                Ok(Some(m)) => {
                    self.s.inbox.push(m);
                    if self.s.inbox.hand_off() && !self.deliver()? {
                        self.s.scratch.rest = Some((chunk, at));
                        break;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    res = Err(e);
                    break;
                }
            }
        }
        self.finish_acks();
        // Messages before a bad packet still go out.
        self.deliver()?;
        res
    }

    /// Queues the decoded messages for the handle. False while some wait for
    /// room.
    fn deliver(&mut self) -> Result<bool> {
        self.s.inbox.try_flush().map_err(|_| {
            self.s.ended = true;
            Error::Closed
        })
    }

    /// Handles one packet of `chunk`. A PUBLISH becomes a message.
    fn packet(&mut self, packet: Packet<'_>, chunk: &Bytes) -> Result<Option<Message>> {
        let v = self.s.opts.version;
        match packet {
            Packet::Publish(p) => return Ok(self.publish(&p, chunk)),
            Packet::PubAck(a) | Packet::PubComp(a) => self.done(a.pkid, a.reason),
            Packet::PubRec(a) => {
                if a.reason >= 0x80 {
                    self.done(a.pkid, a.reason);
                } else if let Some((_, packet)) = self.s.requests.unacked.get_mut(&a.pkid) {
                    // Resend PUBREL, not the PUBLISH, from now on.
                    packet.clear();
                    encode::ack(&AckFields::new(PacketType::PubRel, a.pkid), v, packet);
                    let rel = packet.clone();
                    self.out.push(|b| b.extend_from_slice(&rel));
                }
            }
            Packet::PubRel(a) => {
                self.s.inbound.awaiting_rel.remove(&a.pkid);
                self.out
                    .push(|b| encode::ack(&AckFields::new(PacketType::PubComp, a.pkid), v, b));
            }
            Packet::SubAck(ack) | Packet::UnsubAck(ack) => {
                self.s.requests.unacked.remove(&ack.pkid);
                match self.s.requests.pending.remove(&ack.pkid) {
                    Some(Pending::Sub(tx, change)) => {
                        self.apply(change, ack.codes);
                        let _ = tx.send(Ok(ack.codes.iter().map(|&c| c.into()).collect()));
                    }
                    Some(Pending::Restore | Pending::Publish(_)) | None => {}
                }
                self.send_waiting();
            }
            Packet::PingResp => {}
            Packet::Disconnect(_) => return Err(Error::Closed),
            _ => return Err(Error::Protocol("unexpected packet")),
        }
        Ok(None)
    }

    fn publish(&mut self, p: &Publish<'_>, chunk: &Bytes) -> Option<Message> {
        // Acks wait until the application takes the message.
        let ack = match p.qos {
            QoS::AtMostOnce => None,
            QoS::AtLeastOnce => Some(p.pkid),
            QoS::ExactlyOnce => {
                if !self.s.inbound.awaiting_rel.insert(p.pkid) {
                    // A resend. Answer it if the application has the first.
                    if !self.s.inbound.unconsumed.contains(&p.pkid) {
                        let v = self.s.opts.version;
                        self.out.push(|b| {
                            encode::ack(&AckFields::new(PacketType::PubRec, p.pkid), v, b);
                        });
                    }
                    return None;
                }
                self.s.inbound.unconsumed.insert(p.pkid);
                Some(p.pkid)
            }
        };
        let subscription_ids = p
            .properties
            .iter()
            .filter_map(|x| match x {
                Property::VarInt(property::SUBSCRIPTION_IDENTIFIER, id) => Some(id),
                _ => None,
            })
            .collect();
        Some(Message {
            topic: Topic::slice_of(chunk, p.topic),
            payload: chunk.slice_ref(p.payload),
            qos: p.qos,
            retain: p.retain,
            subscription_ids,
            ack,
            epoch: 0,
        })
    }
}

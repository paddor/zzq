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
    /// client handle. Stops at a full inbox, parking the rest of the read
    /// until the handle makes room.
    pub(super) fn input(&mut self, buf: &mut BytesMut) -> Result<()> {
        let n = complete_len(buf)?;
        if n == 0 {
            return Ok(());
        }
        let chunk = buf.split_to(n).freeze();
        // Acks the handle gathered go out without a wake of their own.
        self.ack_taken();
        if self.s.inbox.blocked() {
            self.read_ahead(&chunk)
        } else {
            self.packets(&chunk)
        }
    }

    /// Queues what waited once the handle made room, then handles what was
    /// parked.
    pub(super) fn resume(&mut self) -> Result<()> {
        if !self.deliver()? {
            return Ok(());
        }
        while let Some(chunk) = self.s.scratch.parked.pop_front() {
            self.s.scratch.parked_bytes -= chunk.len();
            self.packets(&chunk)?;
            if self.s.inbox.blocked() {
                break;
            }
        }
        Ok(())
    }

    /// Handles the acks of the client's own requests in `chunk`, read while
    /// the inbox is full, so a call waiting for one does not wait for
    /// `recv`. Parks the other packets, in order, behind what waits.
    fn read_ahead(&mut self, chunk: &Bytes) -> Result<()> {
        let mut res = Ok(());
        let mut at = 0;
        // Start of the packets to park.
        let mut run = 0;
        while at < chunk.len() {
            let Ok(Some((packet, n))) = decode(&chunk[at..], self.s.opts.version, MAX_PACKET)
            else {
                res = Err(Error::Protocol("malformed packet"));
                break;
            };
            if is_reply(&packet) {
                self.park(chunk.slice(run..at), false);
                if let Err(e) = self.control(packet) {
                    res = Err(e);
                    break;
                }
                run = at + n;
            }
            at += n;
        }
        if res.is_ok() {
            self.park(chunk.slice(run..), false);
        }
        self.finish_acks();
        res
    }

    /// Parks packets for once the inbox has room, at the front or the back.
    fn park(&mut self, packets: Bytes, front: bool) {
        if packets.is_empty() {
            return;
        }
        self.s.scratch.parked_bytes += packets.len();
        if front {
            self.s.scratch.parked.push_front(packets);
        } else {
            self.s.scratch.parked.push_back(packets);
        }
    }

    /// Handles the packets of `chunk`.
    fn packets(&mut self, chunk: &Bytes) -> Result<()> {
        let mut at = 0;
        let mut res = Ok(());
        while at < chunk.len() {
            let Ok(Some((packet, n))) = decode(&chunk[at..], self.s.opts.version, MAX_PACKET)
            else {
                res = Err(Error::Protocol("malformed packet"));
                break;
            };
            at += n;
            // Messages take the inline path; the rest goes out of line, so
            // it cannot keep this loop from being optimized as one.
            let Packet::Publish(p) = packet else {
                if let Err(e) = self.control(packet) {
                    res = Err(e);
                    break;
                }
                continue;
            };
            if let Some(m) = self.publish(&p, chunk) {
                self.s.inbox.push(m);
                if self.s.inbox.hand_off() && !self.deliver()? {
                    // Parked until the handle makes room, which wakes the
                    // session only while the inbox stays blocked: no second
                    // flush, which could succeed by now.
                    self.park(chunk.slice(at..), true);
                    self.finish_acks();
                    return Ok(());
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

    /// Handles a packet other than PUBLISH: acks, PINGRESP and DISCONNECT.
    fn control(&mut self, packet: Packet<'_>) -> Result<()> {
        let v = self.s.opts.version;
        match packet {
            Packet::PubAck(a) | Packet::PubComp(a) => self.done(a.pkid, a.reason)?,
            Packet::PubRec(a) => {
                if a.reason >= 0x80 {
                    self.done(a.pkid, a.reason)?;
                } else if !self.awaits(a.pkid, true)? {
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
                if self.awaits(ack.pkid, false)? {
                    self.s.requests.unacked.remove(&ack.pkid);
                    if let Some(Pending::Sub(tx, change)) =
                        self.s.requests.pending.remove(&ack.pkid)
                    {
                        self.apply(change, ack.codes);
                        let _ = tx.send(Ok(ack.codes.iter().map(|&c| c.into()).collect()));
                    }
                }
                self.send_waiting();
            }
            Packet::PingResp => {}
            Packet::Disconnect(_) => return Err(Error::Closed),
            _ => return Err(Error::Protocol("unexpected packet")),
        }
        Ok(())
    }

    /// Whether request `pkid` waits for an ack: a publish for PUBACK, PUBREC
    /// and PUBCOMP, a subscribe or unsubscribe for SUBACK and UNSUBACK. An
    /// ack of the other kind breaks the protocol.
    fn awaits(&self, pkid: u16, publish: bool) -> Result<bool> {
        match self.s.requests.pending.get(&pkid) {
            None => Ok(false),
            Some(p) if matches!(p, Pending::Publish(_)) == publish => Ok(true),
            Some(_) => Err(Error::Protocol("ack of the wrong kind")),
        }
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

/// Whether `packet` answers a request of the client, so it may be handled
/// ahead of messages that wait for room.
const fn is_reply(packet: &Packet<'_>) -> bool {
    matches!(
        packet,
        Packet::PubAck(_)
            | Packet::PubRec(_)
            | Packet::PubComp(_)
            | Packet::SubAck(_)
            | Packet::UnsubAck(_)
            | Packet::PingResp
    )
}

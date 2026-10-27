//! QoS 1/2 flows towards the client: the inflight window, the queue
//! behind it and packet IDs.

use std::mem::size_of;
use std::time::Instant;

use zzq_proto::{PacketType, QoS};

use crate::budget;
use crate::msg::{Msg, expired};

use super::output::Fields;
use super::{Inflight, Kept, Outcome, QueueLimit, Session};

/// Unacked messages beyond which a session counts as slow: new messages
/// for it get their own copy instead of pinning read buffers.
const SLOW_INFLIGHT: usize = 256;

impl Session {
    /// QoS 1/2 messages waiting for the inflight window.
    #[inline]
    pub(crate) fn queued(&self) -> usize {
        self.flow.queue.len()
    }

    /// QoS 1/2 messages not delivered yet: queued, or sent and not acked.
    /// PUBREC means the client has the message.
    pub(crate) fn held(&self) -> u64 {
        (self.flow.queue.len() + self.flow.inflight.iter().filter(|f| !f.released).count()) as u64
    }

    /// Delivers a QoS 1 or 2 message: sends it if the window allows, queues
    /// it otherwise. The oldest queued messages make room for it.
    pub(crate) fn deliver_qos12(&mut self, h: Kept, limit: QueueLimit) -> Outcome {
        if !self.would_queue() {
            self.charge.add(budget::held(&h));
            return self.send_inflight(h);
        }
        if limit.state_full {
            return Outcome::StateFull;
        }
        let size = h.item.msg.size();
        if size > limit.bytes {
            return Outcome::QueueFull(1);
        }
        let mut dropped = 0;
        while self.flow.queue.len() >= limit.count || self.flow.queued_bytes + size > limit.bytes {
            let Some(old) = self.pop_queued() else {
                break;
            };
            self.charge.sub(budget::held(&old));
            dropped += 1;
        }
        self.charge.add(budget::held(&h));
        self.flow.queued_bytes += size;
        let cap = self.flow.queue.capacity();
        self.flow.queue.push_back(h);
        if self.flow.queue.capacity() != cap {
            self.charge_room();
        }
        if dropped == 0 {
            Outcome::Queued
        } else {
            Outcome::QueueFull(dropped)
        }
    }

    pub(super) fn pop_queued(&mut self) -> Option<Kept> {
        let h = self.flow.queue.pop_front()?;
        self.flow.queued_bytes -= h.item.msg.size();
        Some(h)
    }

    /// Whether a QoS 1/2 delivery now would wait in the queue.
    #[inline]
    pub(crate) fn would_queue(&self) -> bool {
        self.conn.is_none()
            || !self.flow.queue.is_empty()
            || self.flow.inflight.len() >= usize::from(self.flow.window)
    }

    /// Whether a QoS 1/2 delivery now may stay a while: queued, or behind
    /// many unacked messages.
    #[inline]
    pub(crate) fn holds_long(&self) -> bool {
        self.would_queue() || self.flow.inflight.len() >= SLOW_INFLIGHT
    }

    /// Sends a message the session is charged for, and keeps it until the
    /// client acks it.
    pub(super) fn send_inflight(&mut self, h: Kept) -> Outcome {
        let pkid = self.alloc_pkid();
        let ok = self.encode(&Fields::of(&h.item, pkid, false));
        if !ok {
            self.free_pkid(pkid);
            self.charge.sub(budget::held(&h));
            return Outcome::TooLarge;
        }
        let cap = self.flow.inflight.capacity();
        self.flow.inflight.push_back(Inflight {
            pkid,
            held: h,
            released: false,
        });
        if self.flow.inflight.capacity() != cap {
            self.charge_room();
        }
        Outcome::Sent
    }

    /// A copy of `msg` in the arena, so a message held long does not pin a
    /// read buffer.
    pub(crate) fn copy(&mut self, msg: &Msg) -> Msg {
        let copy = self.flow.arena.copy(msg);
        self.charge_room();
        copy
    }

    /// Charges the slots the queue and window have room for, which they
    /// keep as they empty, and the room left in the arena block. Their room
    /// changes only when a push grows them or a copy starts a block.
    pub(super) fn charge_room(&mut self) {
        let room = self.flow.queue.capacity() * size_of::<Kept>()
            + self.flow.inflight.capacity() * size_of::<Inflight>()
            + self.flow.arena.room();
        if room > self.flow.room {
            self.charge.add(room - self.flow.room);
        } else {
            self.charge.sub(self.flow.room - room);
        }
        self.flow.room = room;
    }

    pub(super) fn alloc_pkid(&mut self) -> u16 {
        if self.flow.in_use.is_none() {
            let id = self.flow.next_pkid.wrapping_add(1).max(1);
            if self.flow.inflight.front().is_none_or(|f| f.pkid != id) {
                self.flow.next_pkid = id;
                return id;
            }
        }
        let inflight = &self.flow.inflight;
        let bits = self.flow.in_use.get_or_insert_with(|| {
            let mut bits = Box::new([0; 1024]);
            for f in inflight {
                bits[usize::from(f.pkid >> 6)] |= 1u64 << (f.pkid & 63);
            }
            bits
        });
        loop {
            self.flow.next_pkid = self.flow.next_pkid.wrapping_add(1).max(1);
            let id = self.flow.next_pkid;
            let (word, bit) = (usize::from(id >> 6), 1u64 << (id & 63));
            if bits[word] & bit == 0 {
                bits[word] |= bit;
                return id;
            }
        }
    }

    fn free_pkid(&mut self, id: u16) {
        if let Some(bits) = &mut self.flow.in_use {
            bits[usize::from(id >> 6)] &= !(1u64 << (id & 63));
        }
    }

    /// Moves queued messages into the window. Returns whether anything was
    /// written.
    pub(crate) fn pump(&mut self) -> bool {
        let mut wrote = false;
        if self.flow.unsent > 0 {
            wrote = self.resend_unsent();
            if self.flow.unsent > 0 {
                return wrote;
            }
        }
        let mut now = None;
        while self.conn.is_some() && self.flow.inflight.len() < usize::from(self.flow.window) {
            let Some(h) = self.pop_queued() else {
                break;
            };
            let expires_at = h.item.msg.expires_at;
            if expires_at.is_some() && expired(expires_at, *now.get_or_insert_with(Instant::now)) {
                self.flow.expired += 1;
                self.charge.sub(budget::held(&h));
                continue;
            }
            match self.send_inflight(h) {
                Outcome::Sent => {
                    self.flow.sent += 1;
                    wrote = true;
                }
                _ => self.flow.too_large += 1,
            }
        }
        wrote
    }

    /// Where the delivery in flight under `pkid` is in the window. IDs go
    /// out in order and skip only IDs in use, so it sits at most `pkid -
    /// front` from the front: look there, then back over acked gaps. A
    /// window that wrapped all IDs, or came from a state file, may need
    /// the rest.
    fn inflight_at(&self, pkid: u16) -> Option<usize> {
        let w = &self.flow.inflight;
        let front = w.front()?.pkid;
        let guess = usize::from(pkid.wrapping_sub(front)).min(w.len() - 1);
        (0..=guess)
            .rev()
            .find(|&i| w[i].pkid == pkid)
            .or_else(|| (guess + 1..w.len()).find(|&i| w[i].pkid == pkid))
    }

    fn take_inflight(&mut self, i: usize) {
        if i >= self.flow.inflight.len() - self.flow.unsent {
            self.flow.unsent -= 1;
        }
        if let Some(f) = self.flow.inflight.remove(i) {
            self.free_pkid(f.pkid);
            self.charge.sub(budget::held(&f.held));
        }
    }

    /// PUBACK from the client. `Err` with a reason code if it acks a QoS 2
    /// delivery.
    pub(crate) fn on_puback(&mut self, pkid: u16) -> Result<bool, u8> {
        let Some(i) = self.inflight_at(pkid) else {
            return Ok(false);
        };
        if self.flow.inflight[i].held.item.qos != QoS::AtLeastOnce {
            return Err(zzq_proto::reason::PROTOCOL_ERROR);
        }
        self.take_inflight(i);
        Ok(self.pump())
    }

    /// PUBREC from the client. Answers with PUBREL. `Err` with a reason
    /// code if it acks a QoS 1 delivery.
    pub(crate) fn on_pubrec(&mut self, pkid: u16, reason: u8) -> Result<bool, u8> {
        let Some(i) = self.inflight_at(pkid) else {
            self.ack(
                PacketType::PubRel,
                pkid,
                zzq_proto::reason::PACKET_IDENTIFIER_NOT_FOUND,
            );
            return Ok(true);
        };
        let f = &mut self.flow.inflight[i];
        if f.held.item.qos != QoS::ExactlyOnce {
            return Err(zzq_proto::reason::PROTOCOL_ERROR);
        }
        if reason >= 0x80 {
            self.take_inflight(i);
            return Ok(self.pump());
        }
        f.released = true;
        self.ack(PacketType::PubRel, pkid, 0);
        Ok(true)
    }

    /// PUBCOMP from the client. `Err` with a reason code if it comes before
    /// PUBREC, or for a QoS 1 delivery.
    pub(crate) fn on_pubcomp(&mut self, pkid: u16) -> Result<bool, u8> {
        let Some(i) = self.inflight_at(pkid) else {
            return Ok(false);
        };
        if !self.flow.inflight[i].released {
            return Err(zzq_proto::reason::PROTOCOL_ERROR);
        }
        self.take_inflight(i);
        Ok(self.pump())
    }

    /// Resends the inflight window after a reconnect, as far as the
    /// client's Receive Maximum allows [MQTT-3.3.4-9]. Acks send the rest.
    pub(crate) fn resend(&mut self) {
        self.flow.unsent = self.flow.inflight.len();
        self.pump();
    }

    /// Resends held back inflight messages while the window has room.
    fn resend_unsent(&mut self) -> bool {
        let window = usize::from(self.flow.window);
        let mut inflight = std::mem::take(&mut self.flow.inflight);
        let mut wrote = false;
        while self.flow.unsent > 0
            && self.conn.is_some()
            && inflight.len() - self.flow.unsent < window
        {
            let at = inflight.len() - self.flow.unsent;
            self.flow.unsent -= 1;
            let f = &inflight[at];
            if f.released {
                self.ack(PacketType::PubRel, f.pkid, 0);
            } else if !self.encode(&Fields::of(&f.held.item, f.pkid, true)) {
                // Larger than the client now takes, so it could never be
                // acked: it gives up its slot.
                if let Some(f) = inflight.remove(at) {
                    self.free_pkid(f.pkid);
                    self.charge.sub(budget::held(&f.held));
                    self.flow.too_large += 1;
                }
                continue;
            }
            wrote = true;
        }
        self.flow.inflight = inflight;
        wrote
    }
}

#[cfg(test)]
mod tests {
    use zzq_proto::{QoS, Version};

    use super::super::{Inflight, Kept, Queued, Session};
    use crate::budget::LaneTotal;
    use crate::index::SubIds;
    use crate::msg::Msg;

    fn inflight(pkid: u16) -> Inflight {
        let item = Queued {
            msg: Msg::new("t", b"x", &[], QoS::AtLeastOnce, false),
            qos: QoS::AtLeastOnce,
            retain: false,
            sub_id: SubIds::default(),
        };
        Inflight {
            pkid,
            held: Kept::own(item),
            released: false,
        }
    }

    #[test]
    fn acked_deliveries_are_found_past_gaps_and_the_id_wrap() {
        let mut s = Session::new("c".into(), Version::V5, LaneTotal::default());
        assert_eq!(s.inflight_at(1), None);
        // Acks out of order left gaps, and IDs wrapped past 65535 to 1.
        let ids = [65533, 65535, 1, 4];
        for id in ids {
            s.flow.inflight.push_back(inflight(id));
        }
        for (i, id) in ids.into_iter().enumerate() {
            assert_eq!(s.inflight_at(id), Some(i));
        }
        assert_eq!(s.inflight_at(2), None);
        // Out of order, as a state file may hold it: before where it sits.
        s.flow.inflight.push_back(inflight(65534));
        assert_eq!(s.inflight_at(65534), Some(4));
    }
}

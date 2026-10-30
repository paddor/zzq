//! Requests to the broker: packet IDs, the Receive Maximum, resends
//! after a reconnect, and their acks.

use tokio::sync::oneshot;
use zzq_proto::encode;
use zzq_proto::packet::fixed_header;
use zzq_proto::property::{self, Property};
use zzq_proto::{PacketType, QoS, ReasonCode, SubscriptionOptions};

use crate::submit::Op;
use crate::submit::QUEUED;
use crate::{Cmd, Error, Result};

use super::{Change, Conn, Pending};

impl Conn<'_> {
    /// After a reconnect: resends unacked requests and, if the broker lost
    /// the session, the subscriptions.
    pub(super) fn restore(&mut self, session_present: bool) {
        // Restores from an earlier connection would only add duplicates.
        let stale: Vec<u16> = self
            .s
            .requests
            .pending
            .iter()
            .filter(|(_, p)| matches!(p, Pending::Restore))
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            self.s.requests.pending.remove(&id);
            self.s.requests.unacked.remove(&id);
        }
        if !session_present {
            self.finish_released();
        }
        let order = self.unacked_in_order();
        if !session_present {
            self.s.inbound.awaiting_rel.clear();
            self.s.inbound.unconsumed.clear();
            self.s.inbox.forget_acks();
            self.s.scratch.taken.clear();
            self.restore_subscriptions();
        }
        self.resend_unacked(order);
        // Finished flows may have made room for waiting requests.
        self.send_waiting();
    }

    /// Packet IDs of the unacked requests, in the order they were sent.
    fn unacked_in_order(&self) -> Vec<u16> {
        let mut order: Vec<(u64, u16)> = self
            .s
            .requests
            .unacked
            .iter()
            .map(|(&pkid, (seq, _))| (*seq, pkid))
            .collect();
        order.sort_unstable();
        order.into_iter().map(|(_, pkid)| pkid).collect()
    }

    /// Subscribes again, one SUBSCRIBE per Subscription Identifier.
    fn restore_subscriptions(&mut self) {
        let mut by_id: Vec<(u32, Vec<(String, SubscriptionOptions)>)> = Vec::new();
        for (f, o, id) in &self.s.subs {
            match by_id.iter_mut().find(|(i, _)| i == id) {
                Some((_, list)) => list.push((f.clone(), *o)),
                None => by_id.push((*id, vec![(f.clone(), *o)])),
            }
        }
        for (id, filters) in by_id {
            let Some(pkid) = self.pkid() else {
                // Every packet ID is taken: send it once one is free.
                self.s.requests.waiting.push_front(Cmd::Subscribe {
                    filters,
                    sub_id: id,
                    done: oneshot::channel().0,
                });
                continue;
            };
            let packet = self.subscribe_packet(pkid, &filters, id);
            self.send_tracked(pkid, packet, Pending::Restore);
        }
    }

    /// Sends the requests in `order` again: PUBREL and SUBSCRIBE packets at
    /// once, PUBLISH packets as the broker's Receive Maximum allows, which
    /// counts the released flows too. The other PUBLISH packets wait in
    /// `resend`.
    fn resend_unacked(&mut self, order: Vec<u16>) {
        self.s.requests.resend.clear();
        self.s.requests.resent_rel.clear();
        let released = self
            .s
            .requests
            .unacked
            .values()
            .filter(|(_, p)| is(p, 6))
            .count();
        let mut room = self.s.requests.quota.saturating_sub(released);
        for pkid in order {
            let publish = self
                .s
                .requests
                .unacked
                .get(&pkid)
                .is_some_and(|(_, p)| is(p, 3));
            if !publish {
                self.resend_one(pkid);
            } else if room > 0 {
                room -= 1;
                self.resend_one(pkid);
            } else {
                self.s.requests.resend.push_back(pkid);
            }
        }
    }

    /// Sends an unacked request again, a PUBLISH with DUP.
    fn resend_one(&mut self, pkid: u16) {
        if let Some((_, packet)) = self.s.requests.unacked.get(&pkid) {
            let dup = if is(packet, 3) { 0x08 } else { 0 };
            if is(packet, 6) {
                self.s.requests.resent_rel.insert(pkid);
            }
            self.out.push(|b| {
                let at = b.len();
                b.extend_from_slice(packet);
                b[at] |= dup;
            });
        }
    }

    /// Finishes the QoS 2 publishes that got PUBREC, for a broker that lost
    /// the session and so the flows: PUBREC said it had the message.
    fn finish_released(&mut self) {
        let released: Vec<u16> = self
            .s
            .requests
            .unacked
            .iter()
            .filter(|(_, (_, p))| is(p, 6))
            .map(|(id, _)| *id)
            .collect();
        for pkid in released {
            self.s.requests.unacked.remove(&pkid);
            if let Some(Pending::Publish(slot)) = self.s.requests.pending.remove(&pkid) {
                self.s.scratch.finished.push((slot, Ok(())));
                self.s.requests.inflight = self.s.requests.inflight.saturating_sub(1);
            }
        }
    }

    /// A free packet ID, if there is one.
    fn pkid(&mut self) -> Option<u16> {
        if !self.ids_left() {
            return None;
        }
        for _ in 0..u16::MAX {
            self.s.requests.next_pkid = self.s.requests.next_pkid.wrapping_add(1).max(1);
            if !self
                .s
                .requests
                .pending
                .contains_key(&self.s.requests.next_pkid)
            {
                return Some(self.s.requests.next_pkid);
            }
        }
        None
    }

    fn ids_left(&self) -> bool {
        self.s.requests.pending.len() < usize::from(u16::MAX)
    }

    /// Whether `cmd` has to wait: for the broker's Receive Maximum, or for
    /// a free packet ID.
    fn blocked(&self, cmd: &Cmd) -> bool {
        match cmd {
            Cmd::Publish { .. } => {
                self.s.requests.inflight >= self.s.requests.quota || !self.ids_left()
            }
            Cmd::Subscribe { .. } | Cmd::Unsubscribe { .. } => !self.ids_left(),
            Cmd::Disconnect { .. } => false,
        }
    }

    /// Whether the session takes more requests: the writer has room, and
    /// not too much waits for the broker's Receive Maximum.
    pub(super) fn takes_requests(&self) -> bool {
        self.out.has_room() && self.s.requests.waiting_bytes < QUEUED
    }

    /// Sends what waits for room, in order: publishes to resend after a
    /// reconnect, then waiting commands.
    pub(super) fn send_waiting(&mut self) {
        while let Some(&pkid) = self.s.requests.resend.front()
            && self
                .s
                .requests
                .inflight
                .saturating_sub(self.s.requests.resend.len())
                < self.s.requests.quota
        {
            self.s.requests.resend.pop_front();
            self.resend_one(pkid);
        }
        while let Some(cmd) = self.s.requests.waiting.front()
            && !self.blocked(cmd)
        {
            if let Some(cmd) = self.s.requests.waiting.pop_front() {
                if let Cmd::Publish { packet, .. } = &cmd {
                    self.s.requests.waiting_bytes -= packet.len();
                }
                self.command(cmd);
            }
        }
    }

    /// Sends `packet` and keeps it for resending until acked.
    fn send_tracked(&mut self, pkid: u16, packet: Vec<u8>, pending: Pending) {
        self.out.push(|b| b.extend_from_slice(&packet));
        self.s.requests.sent += 1;
        self.s
            .requests
            .unacked
            .insert(pkid, (self.s.requests.sent, packet));
        self.s.requests.pending.insert(pkid, pending);
    }

    fn subscribe_packet(
        &self,
        pkid: u16,
        filters: &[(String, SubscriptionOptions)],
        sub_id: u32,
    ) -> Vec<u8> {
        let list: Vec<(&str, _)> = filters.iter().map(|(f, o)| (f.as_str(), *o)).collect();
        let props: Vec<Property<'_>> = if sub_id > 0 {
            vec![Property::VarInt(property::SUBSCRIPTION_IDENTIFIER, sub_id)]
        } else {
            Vec::new()
        };
        let mut b = Vec::new();
        encode::subscribe(self.s.opts.version, pkid, &props, &list, &mut b);
        b
    }

    fn command(&mut self, cmd: Cmd) {
        if self.blocked(&cmd) {
            if let Cmd::Publish { packet, .. } = &cmd {
                self.s.requests.waiting_bytes += packet.len();
            }
            self.s.requests.waiting.push_back(cmd);
            return;
        }
        let v = self.s.opts.version;
        // Not blocked, so a packet ID is free.
        match cmd {
            Cmd::Publish { mut packet, slot } => {
                let Some(pkid) = self.pkid() else {
                    self.s.scratch.finished.push((slot, Err(Error::Closed)));
                    return;
                };
                self.s.requests.inflight += 1;
                set_pkid(&mut packet, pkid);
                self.send_tracked(pkid, packet, Pending::Publish(slot));
            }
            Cmd::Subscribe {
                filters,
                sub_id,
                done,
            } => {
                let Some(pkid) = self.pkid() else { return };
                let packet = self.subscribe_packet(pkid, &filters, sub_id);
                self.send_tracked(
                    pkid,
                    packet,
                    Pending::Sub(done, Change::Add(filters, sub_id)),
                );
            }
            Cmd::Unsubscribe { filters, done } => {
                let Some(pkid) = self.pkid() else { return };
                let list: Vec<&str> = filters.iter().map(String::as_str).collect();
                let mut b = Vec::new();
                encode::unsubscribe(v, pkid, &[], &list, &mut b);
                self.send_tracked(pkid, b, Pending::Sub(done, Change::Remove(filters)));
            }
            Cmd::Disconnect { .. } => {}
        }
    }

    /// Runs the queued requests. Returns false once the session ends:
    /// DISCONNECT went out or the handle is gone.
    pub(super) fn requests(&mut self) -> bool {
        let mut batch = std::mem::take(&mut self.s.scratch.batch);
        let dropped = self.s.submit.take(&mut batch);
        let mut at = 0;
        let mut open = true;
        while let Some(op) = batch.ops.pop_front() {
            match op {
                Op::Publish { len, qos, slot } => {
                    let packet = &batch.bytes[at..at + len];
                    at += len;
                    if qos == QoS::AtMostOnce {
                        self.out.push(|b| b.extend_from_slice(packet));
                    } else {
                        let packet = packet.to_vec();
                        self.command(Cmd::Publish { packet, slot });
                    }
                }
                Op::Cmd(Cmd::Disconnect { reason, done }) => {
                    self.say_bye(reason, done);
                    // The requests after it fail with the session.
                    open = false;
                    break;
                }
                Op::Cmd(cmd) => self.command(cmd),
            }
        }
        if open {
            batch.clear();
        }
        self.s.scratch.batch = batch;
        self.finish_acks();
        if !open || dropped {
            self.s.ended = true;
            return false;
        }
        true
    }

    /// Queues DISCONNECT, and answers `done` once it is written.
    pub(super) fn say_bye(&mut self, reason: ReasonCode, done: oneshot::Sender<()>) {
        let v = self.s.opts.version;
        self.out.push(|b| {
            encode::reason_only(PacketType::Disconnect, v, reason.into(), &[], b);
        });
        *self.out.bye.borrow_mut() = Some(done);
    }

    /// Hands the publish acks gathered so far to the waiting calls.
    pub(super) fn finish_acks(&mut self) {
        if !self.s.scratch.finished.is_empty() {
            self.s.scratch.chained |= self.s.acks.finish(&mut self.s.scratch.finished);
        }
    }

    /// Records granted subscriptions and removed ones for later restores.
    pub(super) fn apply(&mut self, change: Change, codes: &[u8]) {
        let subs = &mut self.s.subs;
        match change {
            Change::Add(filters, sub_id) => {
                for (i, (f, o)) in filters.into_iter().enumerate() {
                    if codes.get(i).is_some_and(|&c| c < 0x80) {
                        subs.retain(|(g, _, _)| *g != f);
                        subs.push((f, o, sub_id));
                    }
                }
            }
            Change::Remove(filters) => subs.retain(|(g, _, _)| !filters.contains(g)),
        }
    }

    /// Finishes the publish that `pkid` acks with `code`. An ack for a
    /// subscribe or unsubscribe breaks the protocol.
    pub(super) fn done(&mut self, pkid: u16, code: u8) -> Result<()> {
        let slot = match self.s.requests.pending.remove(&pkid) {
            Some(Pending::Publish(slot)) => slot,
            Some(other) => {
                // Kept for the next connection to send again.
                self.s.requests.pending.insert(pkid, other);
                return Err(Error::Protocol("ack of the wrong kind"));
            }
            None => return Ok(()),
        };
        self.s.requests.unacked.remove(&pkid);
        let res = if code >= 0x80 {
            Err(Error::Rejected(code.into()))
        } else {
            Ok(())
        };
        self.s.scratch.finished.push((slot, res));
        self.s.requests.inflight = self.s.requests.inflight.saturating_sub(1);
        self.send_waiting();
        Ok(())
    }
}

/// Whether `packet` is of control packet type `kind`.
fn is(packet: &[u8], kind: u8) -> bool {
    packet.first().is_some_and(|b| b >> 4 == kind)
}

/// Sets the packet ID of an encoded QoS 1/2 PUBLISH.
fn set_pkid(packet: &mut [u8], pkid: u16) {
    let Ok(Some(h)) = fixed_header(packet) else {
        return;
    };
    let at = h.header_len;
    let Some(&[a, b]) = packet.get(at..at + 2) else {
        return;
    };
    let at = at + 2 + usize::from(u16::from_be_bytes([a, b]));
    if let Some(id) = packet.get_mut(at..at + 2) {
        id.copy_from_slice(&pkid.to_be_bytes());
    }
}

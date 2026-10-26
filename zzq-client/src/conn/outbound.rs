//! Requests to the broker: packet IDs, the Receive Maximum, resends
//! after a reconnect, and their acks.

use tokio::sync::oneshot;
use zzq_proto::encode;
use zzq_proto::packet::fixed_header;
use zzq_proto::property::{self, Property};
use zzq_proto::{PacketType, QoS, SubscriptionOptions};

use crate::submit::Op;
use crate::{Cmd, Error};

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
        let mut resend: Vec<(u64, Vec<u8>)> = self.s.requests.unacked.values().cloned().collect();
        resend.sort_by_key(|(seq, _)| *seq);
        if !session_present {
            self.s.inbound.awaiting_rel.clear();
            self.s.inbound.unconsumed.clear();
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
        for (_, mut packet) in resend {
            if packet.first().is_some_and(|b| b >> 4 == 3) {
                packet[0] |= 0x08;
            }
            self.out.push(|b| b.extend_from_slice(&packet));
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

    /// Sends waiting commands that may go now, in order.
    pub(super) fn send_waiting(&mut self) {
        while let Some(cmd) = self.s.requests.waiting.front()
            && !self.blocked(cmd)
        {
            if let Some(cmd) = self.s.requests.waiting.pop_front() {
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
                    let v = self.s.opts.version;
                    self.out.push(|b| {
                        encode::reason_only(PacketType::Disconnect, v, reason.into(), &[], b);
                    });
                    *self.out.bye.borrow_mut() = Some(done);
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

    pub(super) fn done(&mut self, pkid: u16, code: u8) {
        self.s.requests.unacked.remove(&pkid);
        let Some(Pending::Publish(slot)) = self.s.requests.pending.remove(&pkid) else {
            return;
        };
        let res = if code >= 0x80 {
            Err(Error::Rejected(code.into()))
        } else {
            Ok(())
        };
        self.s.scratch.finished.push((slot, res));
        self.s.requests.inflight = self.s.requests.inflight.saturating_sub(1);
        self.send_waiting();
    }
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

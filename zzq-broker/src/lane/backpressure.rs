//! Publisher acks held while a subscriber they went to is congested.

use zzq_proto::packet::Publish;
use zzq_proto::{PacketType, QoS, reason};

use crate::index::SubRef;
use crate::session::{Session, Waiter};
use crate::slab::{Key, Slab};

use super::{Close, Control, Lane};

/// A congested session a QoS 1/2 publish was routed to.
#[derive(Debug, Clone, Copy)]
pub(super) enum Hit {
    Local(Key),
    Remote(SubRef),
}

impl Lane {
    /// Acks a routed QoS 1/2 publish, or holds the ack while a subscriber it
    /// went to is congested. A publisher gets at most Receive Maximum acks
    /// held: a v5 one breaks the protocol past it, a v3.1.1 one, which has no
    /// such limit, gets the rest at once. A publish that needs journal
    /// writes gets its ack once they are done, and then as here.
    pub(super) fn ack_or_hold(&mut self, key: Key, p: &Publish<'_>) -> Result<(), Close> {
        if !self.needs.is_empty() {
            self.hold_publish(key, p);
            return Ok(());
        }
        let kind = match p.qos {
            QoS::AtMostOnce => return Ok(()),
            QoS::AtLeastOnce => PacketType::PubAck,
            QoS::ExactlyOnce => PacketType::PubRec,
        };
        let hit = self.congestion.hit.take();
        self.ack_after(key, hit, (kind, p.pkid, 0))
    }

    /// Sends publisher `key` ack `kind` for `pkid` with `reason`, or holds it
    /// while `hit` is still congested.
    pub(super) fn ack_after(
        &mut self,
        key: Key,
        hit: Option<Hit>,
        (kind, pkid, reason): (PacketType, u16, u8),
    ) -> Result<(), Close> {
        let max = u32::from(self.shared.opts.receive_maximum);
        let hit = hit.filter(|&h| self.still_congested(key, h));
        let Some(s) = self.sessions.get_mut(key) else {
            return Ok(());
        };
        let hit = match hit {
            Some(_) if s.inbound.held_acks >= max && s.version.is_v5() => {
                return Err(Close::Protocol(reason::RECEIVE_MAXIMUM_EXCEEDED));
            }
            Some(_) if s.inbound.held_acks >= max => None,
            hit => hit,
        };
        let Some(hit) = hit else {
            s.ack(kind, pkid, reason);
            self.mark(key);
            return Ok(());
        };
        s.inbound.held_acks += 1;
        let waiter = Waiter {
            publisher: key,
            conn: s.conn.as_ref().map_or(0, |c| c.id),
            kind,
            pkid,
        };
        match hit {
            Hit::Local(sub) => {
                let Some(s) = self.sessions.get_mut(sub) else {
                    return Ok(());
                };
                let mut waiters = std::mem::take(&mut s.backpressure.waiters);
                push_waiter(&self.sessions, &mut waiters, waiter);
                if let Some(s) = self.sessions.get_mut(sub) {
                    s.backpressure.waiters = waiters;
                }
            }
            Hit::Remote(sub) => {
                let waiters = self.congestion.waiters.entry(sub).or_default();
                push_waiter(&self.sessions, waiters, waiter);
            }
        }
        Ok(())
    }

    /// Whether `hit` still holds the acks of publisher `key`: a congested
    /// session other than the publisher.
    fn still_congested(&self, key: Key, hit: Hit) -> bool {
        match hit {
            Hit::Local(sub) => {
                sub != key
                    && self
                        .sessions
                        .get(sub)
                        .is_some_and(|s| s.backpressure.congested)
            }
            Hit::Remote(sub) => self.congestion.remote.contains(&sub),
        }
    }

    /// The session `hit` names, as other lanes name it.
    pub(super) const fn sub_of(&self, hit: Hit) -> SubRef {
        match hit {
            Hit::Local(key) => self.sub_ref(key),
            Hit::Remote(sub) => sub,
        }
    }

    /// What session `sub` is to this lane.
    pub(super) const fn hit_of(&self, sub: SubRef) -> Hit {
        if sub.lane == self.id {
            Hit::Local(sub.key)
        } else {
            Hit::Remote(sub)
        }
    }

    /// Sends held acks once a congested session drained or went away.
    pub(super) fn release_waiters(&mut self, key: Key) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let low = self.shared.opts.qos_queue_high / 2;
        let drained = s.conn.is_none() || s.queued() <= low;
        if !s.backpressure.congested || !drained {
            return;
        }
        s.backpressure.congested = false;
        let waiters = std::mem::take(&mut s.backpressure.waiters);
        let sub = self.sub_ref(key);
        self.broadcast(&|| Control::Congested(sub, false));
        self.send_acks(&waiters);
    }

    pub(super) fn send_acks(&mut self, waiters: &[Waiter]) {
        for w in waiters {
            // A publisher that reconnected sends the publish again.
            if let Some(p) = self.sessions.get_mut(w.publisher)
                && p.has_conn(w.conn)
            {
                p.ack(w.kind, w.pkid, 0);
                p.inbound.held_acks = p.inbound.held_acks.saturating_sub(1);
                self.mark(w.publisher);
            }
        }
    }
}

/// Adds `w` to the acks held for a congested session. A full list first
/// drops those of publisher connections that ended, which a reconnecting
/// publisher would otherwise pile up until the session drains, and keeps
/// room for as many again, so this costs O(1) per ack.
fn push_waiter(sessions: &Slab<Session>, waiters: &mut Vec<Waiter>, w: Waiter) {
    if waiters.len() == waiters.capacity() {
        waiters.retain(|w| {
            sessions
                .get(w.publisher)
                .is_some_and(|p| p.has_conn(w.conn))
        });
        waiters.reserve(waiters.len());
    }
    waiters.push(w);
}

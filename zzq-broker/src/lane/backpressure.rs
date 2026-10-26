//! Publisher acks held while a subscriber they went to is congested.

use zzq_proto::packet::Publish;
use zzq_proto::{PacketType, QoS};

use crate::index::SubRef;
use crate::session::Waiter;
use crate::slab::Key;

use super::{Control, Lane};

/// A congested session a QoS 1/2 publish was routed to.
#[derive(Debug, Clone, Copy)]
pub(super) enum Hit {
    Local(Key),
    Remote(SubRef),
}

impl Lane {
    /// Acks a routed QoS 1/2 publish, or holds the ack while a subscriber it
    /// went to is congested.
    pub(super) fn ack_or_hold(&mut self, key: Key, p: &Publish<'_>) {
        let kind = match p.qos {
            QoS::AtMostOnce => return,
            QoS::AtLeastOnce => PacketType::PubAck,
            QoS::ExactlyOnce => PacketType::PubRec,
        };
        let waiter = Waiter {
            publisher: key,
            kind,
            pkid: p.pkid,
        };
        match self.congestion.hit.take() {
            Some(Hit::Local(sub)) if sub != key && self.sessions.get(sub).is_some() => {
                if let Some(s) = self.sessions.get_mut(sub) {
                    s.waiters.push(waiter);
                }
            }
            Some(Hit::Remote(sub)) => self.congestion.waiters.entry(sub).or_default().push(waiter),
            _ => {
                if let Some(s) = self.sessions.get_mut(key) {
                    s.ack(kind, p.pkid, 0);
                }
                self.mark(key);
            }
        }
    }

    /// Sends held acks once a congested session drained or went away.
    pub(super) fn release_waiters(&mut self, key: Key) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let low = self.shared.opts.qos_queue_high / 2;
        let drained = s.conn.is_none() || s.queued() <= low;
        if !s.congested || !drained {
            return;
        }
        s.congested = false;
        let waiters = std::mem::take(&mut s.waiters);
        let sub = self.sub_ref(key);
        self.broadcast(&|| Control::Congested(sub, false));
        self.send_acks(&waiters);
    }

    pub(super) fn send_acks(&mut self, waiters: &[Waiter]) {
        for w in waiters {
            if let Some(p) = self.sessions.get_mut(w.publisher) {
                p.ack(w.kind, w.pkid, 0);
                self.mark(w.publisher);
            }
        }
    }
}

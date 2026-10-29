//! Acks held for the disk: the connection reads on once the journal
//! writes behind them finished.

use zzq_proto::packet::Publish;
use zzq_proto::{PacketType, QoS, reason};

use crate::durable::{Journaled, Wait};
use crate::session::AckHold;
use crate::slab::Key;

use super::{Close, Lane};

/// The journal writes the publish being handled needs before its ack.
#[derive(Debug, Default)]
pub(crate) struct Needs {
    waits: Vec<Wait>,
    failed: bool,
    /// Records waiting for the disk passed their cap.
    crowded: bool,
}

impl Needs {
    /// Starts over for the next publish.
    pub(super) fn clear(&mut self) {
        self.waits.clear();
        self.failed = false;
        self.crowded = false;
    }

    /// Adds what became of one change.
    pub(super) fn note(&mut self, j: Option<Journaled>) {
        match j {
            Some(Journaled::At { wait, crowded }) => {
                self.crowded |= crowded;
                self.wait(wait);
            }
            Some(Journaled::Failed) => self.failed = true,
            None => {}
        }
    }

    /// Adds record `w`.
    pub(super) fn wait(&mut self, w: Wait) {
        match self.waits.iter_mut().find(|x| x.journal == w.journal) {
            Some(x) => x.n = x.n.max(w.n),
            None => self.waits.push(w),
        }
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.waits.is_empty() && !self.failed
    }
}

impl Lane {
    /// Holds the ack of publish `p` until the journal writes it needs are
    /// done, and the connection's later acks behind it. A QoS 0 publish
    /// has no ack: its connection waits only while records pile up for
    /// the disk.
    pub(super) fn hold_publish(&mut self, key: Key, p: &Publish<'_>) {
        let needs = std::mem::take(&mut self.needs);
        let kind = match p.qos {
            QoS::AtMostOnce => {
                if needs.crowded
                    && let Some(h) = self.hold(key)
                {
                    for &w in &needs.waits {
                        h.wait(w);
                    }
                }
                self.needs = needs;
                return;
            }
            QoS::AtLeastOnce => PacketType::PubAck,
            QoS::ExactlyOnce => PacketType::PubRec,
        };
        // The ack waits for the disk, then for a congested subscriber.
        let congested = self.congestion.hit.take().map(|hit| self.sub_of(hit));
        if let Some(h) = self.hold(key) {
            for &w in &needs.waits {
                h.wait(w);
            }
            h.failed |= needs.failed;
            h.push_publish(kind, p.pkid, congested);
        }
        self.needs = needs;
    }

    /// Holds ack `kind` for `pkid` of session `key` until the write of `j`
    /// is done.
    pub(super) fn hold_ack(&mut self, key: Key, kind: PacketType, pkid: u16, j: Journaled) {
        if let Some(h) = self.hold(key) {
            h.note(j);
            h.push_durable(kind, pkid);
        }
    }

    pub(super) fn hold(&mut self, key: Key) -> Option<&mut AckHold> {
        let s = self.sessions.get_mut(key)?;
        let conn = s.conn.as_ref()?.id;
        Some(
            s.inbound
                .hold
                .get_or_insert_with(|| Box::new(AckHold::new(conn))),
        )
    }

    /// The records the held acks of connection `conn` wait for. `None` if
    /// it has none held.
    pub(crate) fn hold_waits(&mut self, key: Key, conn: u64) -> Option<Vec<Wait>> {
        let h = self.sessions.get_mut(key)?.inbound.hold.as_mut()?;
        (h.conn == conn).then(|| h.take_waits())
    }

    /// Sends the held acks of connection `conn`, once the records they
    /// wait for are `written` or failed. A publish that went to a congested
    /// subscriber keeps its ack held until that drains. If not written, a
    /// v5 publisher gets Unspecified error for each, and a v3.1.1 one,
    /// which has no such code, is disconnected, as is a client whose PUBREL
    /// or PUBCOMP waited.
    pub(crate) fn release_acks(&mut self, key: Key, conn: u64, written: bool) -> Result<(), Close> {
        let Some(s) = self.sessions.get_mut(key) else {
            return Ok(());
        };
        let Some(hold) = s.inbound.hold.take_if(|h| h.conn == conn) else {
            return Ok(());
        };
        let ok = written && !hold.failed;
        let mut result = Ok(());
        for a in &hold.acks {
            if ok || !a.durable {
                let hit = a.congested.map(|sub| self.hit_of(sub));
                if let Err(close) = self.ack_after(key, hit, (a.kind, a.pkid, a.reason)) {
                    result = Err(close);
                    break;
                }
                continue;
            }
            let Some(s) = self.sessions.get_mut(key) else {
                break;
            };
            if !s.version.is_v5() || !matches!(a.kind, PacketType::PubAck | PacketType::PubRec) {
                result = Err(Close::Protocol(reason::UNSPECIFIED_ERROR));
                break;
            }
            // A PUBREC with an error ends the QoS 2 flow.
            if a.kind == PacketType::PubRec {
                s.inbound.awaiting_rel.remove(&a.pkid);
            }
            s.ack(a.kind, a.pkid, reason::UNSPECIFIED_ERROR);
        }
        self.mark(key);
        self.flush();
        result
    }
}

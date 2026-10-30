//! Acks held for the disk: the connection reads on once the journal
//! writes behind them finished.

use zzq_proto::packet::Publish;
use zzq_proto::{PacketType, QoS, reason};

use crate::durable::{Journaled, Wait};
use crate::session::{AckHold, HeldAck};
use crate::slab::Key;

use super::{Close, Lane};

/// The journal writes the publish being handled needs before its ack.
#[derive(Debug, Default)]
pub(crate) struct Needs {
    pub(super) handoff: Option<std::sync::Arc<crate::state::handoff::Pending>>,
    waits: Vec<Wait>,
    failed: bool,
    /// Records waiting for the disk passed their cap.
    crowded: bool,
    /// The publish reached the lane's state whatever becomes of the write.
    pub(super) applied: bool,
}

impl Needs {
    pub(super) fn waits(&self) -> Vec<Wait> {
        self.waits.clone()
    }
    /// Starts over for the next publish.
    pub(super) fn clear(&mut self) {
        self.handoff = None;
        self.waits.clear();
        self.failed = false;
        self.crowded = false;
        self.applied = false;
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
        let mut needs = std::mem::take(&mut self.needs);
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
            if let Some(ack) = h.acks.last_mut() {
                ack.handoff = needs.handoff.take();
                ack.applied = needs.applied;
            }
        }
        self.needs = needs;
    }

    /// Holds the PUBCOMP for `pkid` until the write of `j` is done, and until
    /// the destinations of its publish, if that went through `handoff`, have
    /// it: that frees the publisher's slot, which bounds what is on its way.
    pub(super) fn hold_pubcomp(
        &mut self,
        key: Key,
        pkid: u16,
        j: Option<Journaled>,
        handoff: Option<std::sync::Arc<crate::state::handoff::Pending>>,
    ) {
        let waits = handoff
            .as_ref()
            .and_then(|p| self.handoff_waits_of(p.seq))
            .unwrap_or_default();
        if let Some(h) = self.hold(key) {
            if let Some(j) = j {
                h.note(j);
            }
            for w in waits {
                h.wait(w);
            }
            h.push_durable(PacketType::PubComp, pkid);
            if let Some(ack) = h.acks.last_mut() {
                ack.handoff = handoff;
            }
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
            if !ok && a.durable {
                let refused = self.refuse_ack(key, a, result.is_ok());
                result = result.and(refused);
            } else if result.is_ok() {
                let sub = a
                    .congested
                    .or_else(|| a.handoff.as_ref()?.congested.get().copied());
                let hit = sub.map(|sub| self.hit_of(sub));
                result = self.ack_after(key, hit, (a.kind, a.pkid, a.reason));
            }
        }
        self.mark(key);
        for p in hold.acks.iter().filter_map(|a| a.handoff.as_ref()) {
            self.reap_handoff(p.seq);
        }
        self.flush();
        result
    }

    /// Held ack `a` answers for a change that did not reach a journal. With
    /// `tell`, a v5 publisher gets Unspecified error. Whoever cannot be told
    /// so gets an error, for the connection to close. A publish in the
    /// lane's state stays, as the journal gets all of that state again: the
    /// connection closes, and the client's retry finds it.
    fn refuse_ack(&mut self, key: Key, a: &HeldAck, tell: bool) -> Result<(), Close> {
        if a.applied {
            return Err(Close::Protocol(reason::UNSPECIFIED_ERROR));
        }
        let Some(s) = self.sessions.get_mut(key) else {
            return Ok(());
        };
        let can_tell =
            s.version.is_v5() && matches!(a.kind, PacketType::PubAck | PacketType::PubRec);
        // The QoS 2 flow ends here, also for a client that learns of it by
        // the close: its PUBLISH is a new one when it comes again.
        if a.kind == PacketType::PubRec {
            s.inbound.awaiting_rel.remove(&a.pkid);
            if let Some(flows) = s.inbound.flows.as_mut() {
                flows.by_pkid.remove(&a.pkid);
            }
        }
        if can_tell && tell {
            s.ack(a.kind, a.pkid, reason::UNSPECIFIED_ERROR);
        }
        if a.kind == PacketType::PubRec {
            let j = self.journal_inbound(key, a.pkid, true);
            if let Some(s) = self.sessions.get_mut(key) {
                // The sender may reuse this ID as soon as it sees the
                // rejection, including after the broker restarts.
                s.out.due = s.out.due.then(j);
            }
        }
        if can_tell {
            Ok(())
        } else {
            Err(Close::Protocol(reason::UNSPECIFIED_ERROR))
        }
    }
}

//! Acks held until the journal writes they depend on finish.

use zzq_proto::PacketType;

use crate::durable::{Journaled, Wait};
use crate::index::SubRef;

/// Acks of one connection held for journal writes, in the order they were
/// due, so they leave in the order of the packets they answer.
#[derive(Debug)]
pub(crate) struct AckHold {
    /// The connection the acks are for.
    pub(crate) conn: u64,
    pub(crate) acks: Vec<HeldAck>,
    /// The last record to wait for in each journal.
    waits: Vec<Wait>,
    /// A change of a durable ack never reached a journal.
    pub(crate) failed: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HeldAck {
    pub(crate) kind: PacketType,
    pub(crate) pkid: u16,
    pub(crate) reason: u8,
    /// It answers for a change that must be on disk first, not only for
    /// its place in line.
    pub(crate) durable: bool,
    /// A congested subscriber the publish went to: once on disk, the ack
    /// waits for it to drain too.
    pub(crate) congested: Option<SubRef>,
}

impl AckHold {
    pub(crate) const fn new(conn: u64) -> AckHold {
        AckHold {
            conn,
            acks: Vec::new(),
            waits: Vec::new(),
            failed: false,
        }
    }

    /// Holds an ack until the writes of its change, which the hold waits
    /// for, are done.
    pub(crate) fn push_durable(&mut self, kind: PacketType, pkid: u16) {
        self.push_publish(kind, pkid, None);
    }

    /// Holds the ack of a publish until the writes of its change are done,
    /// then while `congested`, a subscriber it went to, is congested.
    pub(crate) fn push_publish(&mut self, kind: PacketType, pkid: u16, congested: Option<SubRef>) {
        self.acks.push(HeldAck {
            kind,
            pkid,
            reason: 0,
            durable: true,
            congested,
        });
    }

    /// Holds an ack behind the others.
    pub(crate) fn push_now(&mut self, kind: PacketType, pkid: u16, reason: u8) {
        self.acks.push(HeldAck {
            kind,
            pkid,
            reason,
            durable: false,
            congested: None,
        });
    }

    /// Waits for what became of a change too.
    pub(crate) fn note(&mut self, j: Journaled) {
        match j {
            Journaled::At { wait, .. } => self.wait(wait),
            Journaled::Failed => self.failed = true,
        }
    }

    /// Waits for record `w` too.
    pub(crate) fn wait(&mut self, w: Wait) {
        match self.waits.iter_mut().find(|x| x.journal == w.journal) {
            Some(x) => x.n = x.n.max(w.n),
            None => self.waits.push(w),
        }
    }

    /// The records to wait for, taken out.
    pub(crate) fn take_waits(&mut self) -> Vec<Wait> {
        std::mem::take(&mut self.waits)
    }
}

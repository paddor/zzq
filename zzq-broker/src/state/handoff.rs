//! Recoverable QoS 2 routing between independently committed lane journals.

use std::collections::BTreeMap;
use std::sync::Arc;

use zzq_proto::QoS;

use crate::index::SubIds;
use crate::msg::Msg;

use super::codec::{Decoder, put_bytes, put_count, put_msg, put_u16, put_u64};
use super::{Clock, SessionState};

/// A lane's sessions and the handoffs that connect its journal to others.
#[derive(Debug, Default)]
pub(crate) struct LaneState {
    pub(crate) sessions: Vec<SessionState>,
    pub(crate) handoffs: Handoffs,
}

/// Sequence numbers are never reused, including after session expiry.
#[derive(Debug, Clone, Default)]
pub(crate) struct Handoffs {
    pub(crate) serial: u64,
    /// Highest completed handoff from each source lane.
    pub(crate) received: Vec<u64>,
    pub(crate) pending: BTreeMap<u64, Arc<Pending>>,
}

/// A routing decision kept until every destination committed it.
#[derive(Debug)]
pub(crate) struct Pending {
    pub(crate) seq: u64,
    /// Runtime preparation; the journal contains the decision, not this flag.
    pub(crate) ready: std::sync::atomic::AtomicBool,
    /// A local subscriber that became congested while applying this message.
    pub(crate) congested: std::sync::OnceLock<crate::index::SubRef>,
    pub(crate) publisher: Box<str>,
    pub(crate) birth: u64,
    pub(crate) pkid: u16,
    pub(crate) msg: Msg,
    pub(crate) targets: Vec<Target>,
}

/// The selected session and grant, including a shared subscription's choice.
#[derive(Debug, Clone)]
pub(crate) struct Target {
    pub(crate) lane: u16,
    pub(crate) client: Arc<str>,
    /// Distinguishes a later clean session using the same client ID.
    pub(crate) birth: u64,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
    pub(crate) sub_id: SubIds,
}

impl Handoffs {
    pub(crate) const fn next(&mut self) -> u64 {
        self.serial += 1;
        self.serial
    }

    pub(crate) fn received(&self, from: u16) -> u64 {
        self.received.get(usize::from(from)).copied().unwrap_or(0)
    }

    pub(crate) fn receive(&mut self, from: u16, sequence: u64) {
        let from = usize::from(from);
        self.received.resize(self.received.len().max(from + 1), 0);
        self.received[from] = self.received[from].max(sequence);
    }
}

impl Pending {
    /// Conservative accounting, including the routing decision's allocations.
    pub(crate) fn bytes(&self) -> usize {
        self.msg.size()
            + self.publisher.len()
            + 128
            + self
                .targets
                .iter()
                .map(|t| t.client.len() + 96)
                .sum::<usize>()
    }
}

/// Finishes routing in offline state before a lane-count migration. The
/// caller writes the resulting sessions to a new, atomically installed set
/// of journals; a crash before installation retries against the old set.
pub(crate) fn materialize(
    lanes: &mut [LaneState],
    retained: &crate::retain::Retained,
) -> std::io::Result<()> {
    for from in 0..lanes.len() {
        let source = u16::try_from(from).map_err(|_| std::io::ErrorKind::InvalidData)?;
        let pending = std::mem::take(&mut lanes[from].handoffs.pending);
        for (seq, p) in pending {
            if p.msg.retain
                && matches!(
                    retained.set_once(source, seq, &p.msg, false),
                    Some(crate::durable::Journaled::Failed)
                )
            {
                return Err(std::io::Error::other(
                    "retained handoff could not be written",
                ));
            }
            for (to, lane) in lanes.iter_mut().enumerate() {
                if lane.handoffs.received(source) >= seq {
                    continue;
                }
                for target in &p.targets {
                    if usize::from(target.lane) != to || target.qos == QoS::AtMostOnce {
                        continue;
                    }
                    if let Some(s) = lane.sessions.iter_mut().find(|s| {
                        s.client_id.as_ref() == &*target.client && s.birth == target.birth
                    }) {
                        s.last_seq += 1;
                        s.seqs.push(s.last_seq);
                        s.queue.push(crate::session::Queued {
                            msg: p.msg.clone(),
                            qos: target.qos,
                            retain: target.retain,
                            sub_id: target.sub_id,
                        });
                    }
                }
                lane.handoffs.receive(source, seq);
            }
        }
    }
    retained.sync()
}

pub(crate) fn put_pending(b: &mut Vec<u8>, p: &Pending, clock: Clock) {
    put_u64(b, p.seq);
    put_bytes(b, p.publisher.as_bytes());
    put_u64(b, p.birth);
    put_u16(b, p.pkid);
    put_msg(b, &p.msg, clock);
    put_count(b, p.targets.len());
    for t in &p.targets {
        put_u16(b, t.lane);
        put_bytes(b, t.client.as_bytes());
        put_u64(b, t.birth);
        b.extend_from_slice(&[t.qos as u8, u8::from(t.retain)]);
        for id in t.sub_id.slots() {
            super::codec::put_u32(b, id);
        }
    }
}

pub(super) fn read_pending(d: &mut Decoder<'_>, clock: Clock) -> Result<Pending, &'static str> {
    let seq = d.u64()?;
    let publisher = d.str()?.into();
    let birth = d.u64()?;
    let pkid = d.u16()?;
    if seq == 0 || seq == u64::MAX || pkid == 0 {
        return Err("invalid handoff identifier");
    }
    let msg = d.msg(clock)?;
    if msg.qos != QoS::ExactlyOnce {
        return Err("handoff is not QoS 2");
    }
    let count = d.count(2 + 4 + 8 + 2 + 16)?;
    let mut targets = Vec::with_capacity(count);
    for _ in 0..count {
        let lane = d.u16()?;
        let client = d.str()?.into();
        let birth = d.u64()?;
        let qos = d.qos()?;
        let retain = d.flag()?;
        let mut sub_id = SubIds::default();
        for _ in 0..4 {
            sub_id.add(d.u32()?);
        }
        targets.push(Target {
            lane,
            client,
            birth,
            qos,
            retain,
            sub_id,
        });
    }
    Ok(Pending {
        seq,
        ready: std::sync::atomic::AtomicBool::new(false),
        congested: std::sync::OnceLock::new(),
        publisher,
        birth,
        pkid,
        msg,
        targets,
    })
}

pub(super) fn put_meta(b: &mut Vec<u8>, h: &Handoffs) {
    put_u64(b, h.serial);
    put_count(b, h.received.len());
    for &seq in &h.received {
        put_u64(b, seq);
    }
}

pub(super) fn read_meta(d: &mut Decoder<'_>, h: &mut Handoffs) -> Result<(), &'static str> {
    let serial = d.u64()?;
    if serial == u64::MAX {
        return Err("invalid handoff counter");
    }
    h.serial = h.serial.max(serial);
    let count = d.count(8)?;
    if count > usize::from(u16::MAX) + 1 {
        return Err("too many handoff lanes");
    }
    h.received.clear();
    for _ in 0..count {
        h.received.push(d.u64()?);
    }
    Ok(())
}

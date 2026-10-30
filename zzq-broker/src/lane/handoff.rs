//! Journaled routing decisions and idempotent delivery to other lanes.

use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;
use zzq_proto::packet::Publish;

use crate::broker::Shared;
use crate::budget::{Charge, LaneTotal};
use crate::durable::{Due, JournalId, Journaled, Wait};
use crate::index::Matches;
use crate::msg::Publication;
use crate::slab::Key;
use crate::state::handoff::{Handoffs, Pending, Target};

use super::route::{Grant, Owned};
use super::{Lane, LaneRef, Remote};

/// Handoffs a sweep looks at for those that finished.
const REAP: usize = 64;

/// Handoffs, or bytes of them, [`prepare`] handles before it ships them and
/// lets the lane's connections run.
const PREPARE_RUN: (usize, usize) = (64, 1 << 20);

#[derive(Debug)]
pub(super) struct Routing {
    pub(super) state: Handoffs,
    /// Decisions on their way to their journals, oldest first.
    ready: VecDeque<Ready>,
    /// Marked on the push that made `ready` non-empty.
    pub(super) wake: Rc<Notify>,
    /// What each handoff waits for: the writes of its source, which its
    /// PUBREC needs, and its destinations, which its PUBCOMP needs.
    waits: BTreeMap<u64, Vec<Wait>>,
    charge: Charge,
    /// Where the next sweep looks for finished handoffs.
    reap_from: u64,
    /// A QoS 2 publish of the connection being read has its delivery still
    /// ahead. See [`Lane::handle_chunk`].
    pub(super) in_read: bool,
    /// Handoffs from other lanes this lane's journal could not take yet,
    /// oldest first: they go on in order once it can.
    later: VecDeque<Arriving>,
}

/// A handoff that came to this lane.
#[derive(Debug)]
pub(super) struct Arriving {
    pub(super) pending: Arc<Pending>,
    pub(super) seq: u64,
    pub(super) from: u16,
    /// Its number among the handoffs of `from` to this lane.
    pub(super) sequence: u64,
}

/// A decision and the writes it waits for: its own record, and its retained
/// update.
#[derive(Debug)]
struct Ready {
    pending: Arc<Pending>,
    writes: [Due; 2],
}

impl Routing {
    pub(super) fn new(total: LaneTotal) -> Routing {
        Routing {
            state: Handoffs::default(),
            ready: VecDeque::new(),
            wake: Rc::default(),
            waits: BTreeMap::new(),
            charge: Charge::new(total, 0),
            reap_from: 0,
            in_read: false,
            later: VecDeque::new(),
        }
    }
}

impl Lane {
    /// Keeps both the message and the exact subscription choices before
    /// exposing any delivery. Persistent recipients remember this sequence.
    /// A publish behind one that waits for its writes goes the same way, as
    /// must a publish that may reach another journal or the retained store.
    pub(super) fn route_handoff(&mut self, key: Key, pkid: u16, p: &Publication<'_>) -> bool {
        if !self.pairs_on() || !self.journals() {
            return false;
        }
        if !p.retain && !self.handoffs.in_read {
            let m = self.publisher_matches(key, p.topic);
            if m.kept_on(self.id) {
                self.route_local(key, pkid, p, &m);
                return true;
            }
        }
        let Some(s) = self.sessions.get(key) else {
            return true;
        };
        let publisher = s.client_id.clone();
        let birth = s.birth;
        let seq = self.handoffs.state.next();
        let msg = p.to_msg();
        // Before the match, as for any publish: a SUBSCRIBE either is matched
        // or finds the message in the store.
        let retained = if p.retain {
            let full = self.budget.full;
            self.shared.retained.set_once(self.id, seq, &msg, full)
        } else {
            None
        };
        let targets = self.handoff_targets(key, p);
        if let Some(s) = self.sessions.get_mut(key) {
            let flows = s.inbound.flows.get_or_insert_default();
            flows.by_pkid.insert(pkid, seq);
            flows.last = seq;
        }
        let pending = Arc::new(Pending {
            seq,
            ready: AtomicBool::new(false),
            congested: std::sync::OnceLock::new(),
            publisher,
            birth,
            pkid,
            msg,
            targets,
        });
        self.handoffs.charge.add(pending.bytes());
        self.handoffs.state.pending.insert(seq, pending.clone());
        let j = self.journal_handoff(&pending);
        self.needs.note(j);
        self.needs.note(retained);
        self.queue_handoff(&pending, [Due::Now.then(j), Due::Now.then(retained)]);
        self.needs.handoff = Some(pending);
        self.handoffs.in_read = true;
        self.stats.publishes += 1;
        true
    }

    /// Routes a publish that only sessions of this lane's journal get: one
    /// record holds its packet ID and what those sessions make of it, so a
    /// crash leaves all of that or none. They see it after that record.
    fn route_local(&mut self, key: Key, pkid: u16, p: &Publication<'_>, m: &Matches) {
        if !self.begin_handoff() {
            self.needs.note(Some(Journaled::Failed));
            return;
        }
        self.journal_inbound(key, pkid, false);
        self.congestion.hit = None;
        self.route_matches(Some(key), m, p, &mut Owned::default());
        let j = self.end_transaction();
        self.needs.note(j);
        let Some(j) = j else {
            return;
        };
        self.needs.applied = true;
        for e in m.entries() {
            if let Some(s) = self.sessions.get_mut(e.sub.key) {
                s.out.due = s.out.due.then(Some(j));
            }
        }
        if j == Journaled::Failed {
            self.rewrite_journal();
        }
    }

    /// The sessions a publish from `key` goes to, a shared subscription's
    /// choice among them.
    fn handoff_targets(&mut self, key: Key, p: &Publication<'_>) -> Vec<Target> {
        let source = self.sub_ref(key);
        let m = self.publisher_matches(key, p.topic);
        self.congestion.hit = None;
        m.direct
            .iter()
            .chain(m.groups.iter().map(crate::index::Group::pick))
            .filter(|e| !(e.opts.no_local && e.sub == source))
            .inspect(|e| {
                if self.congestion.remote.contains(&e.sub) {
                    self.congestion.hit.get_or_insert(super::Hit::Remote(e.sub));
                }
            })
            .map(|e| Target {
                lane: e.sub.lane,
                client: e.client.clone(),
                birth: e.birth,
                qos: e.opts.qos.min(p.qos),
                retain: e.opts.retain_as_published && p.retain,
                sub_id: e.sub_id,
            })
            .collect()
    }

    fn queue_handoff(&mut self, p: &Arc<Pending>, writes: [Due; 2]) {
        // Beyond 64 lanes, a lane may get one without targets for it.
        let mut lanes = 0u64;
        for t in &p.targets {
            lanes |= 1 << (t.lane & 63);
        }
        let mut waits = self.needs.waits();
        for to in 0..self.out.remote.len() {
            if lanes & (1 << (to & 63)) != 0
                && let Some(w) = self.pairs.sent_handoff(self.id, to)
            {
                waits.push(w);
            }
        }
        self.handoffs.waits.insert(p.seq, waits);
        if self.handoffs.ready.is_empty() {
            self.handoffs.wake.notify_one();
        }
        self.handoffs.ready.push_back(Ready {
            pending: p.clone(),
            writes,
        });
    }

    /// The retry must wait for the writes of the first attempt too, which may
    /// be on their way. Only a flow the session still has can be retried: a
    /// publish that reuses the ID of a finished one is new, whatever that
    /// left behind.
    pub(super) fn retry_handoff(&mut self, key: Key, p: &Publish<'_>) -> bool {
        if !self.pairs_on() {
            return false;
        }
        let Some(s) = self
            .sessions
            .get_mut(key)
            .filter(|s| s.inbound.awaiting_rel.contains(&p.pkid))
        else {
            return false;
        };
        let flows = s.inbound.flows.as_mut();
        let seq = flows.and_then(|f| f.by_pkid.get(&p.pkid).copied());
        let pending = seq.and_then(|seq| self.handoffs.state.pending.get(&seq).cloned());
        self.needs.clear();
        if let Some(pending) = pending {
            let waits = self.handoffs.waits.get(&pending.seq).into_iter().flatten();
            for w in waits.filter(|w| !matches!(w.journal, JournalId::Handoff { .. })) {
                self.needs.wait(*w);
            }
            if let Some(flows) = s.inbound.flows.as_mut() {
                flows.last = flows.last.max(pending.seq);
            }
            self.needs.handoff = Some(pending);
            self.handoffs.in_read = true;
        } else {
            self.needs.wait(Wait {
                journal: JournalId::Lane(usize::from(self.id)),
                n: self.pairs.appended,
            });
            self.needs.applied = true;
        }
        self.hold_publish(key, p);
        true
    }

    /// The handoff of session `key`'s flow `pkid`, which its PUBREL ends.
    pub(super) fn take_flow(&mut self, key: Key, pkid: u16) -> Option<Arc<Pending>> {
        let flows = self.sessions.get_mut(key)?.inbound.flows.as_mut()?;
        let seq = flows.by_pkid.remove(&pkid)?;
        self.handoffs.state.pending.get(&seq).cloned()
    }

    /// What handoff `seq` waits for.
    pub(super) fn handoff_waits_of(&self, seq: u64) -> Option<Vec<Wait>> {
        self.handoffs.waits.get(&seq).cloned()
    }

    /// Whether the last handoff of session `key` is still on its way to its
    /// destinations.
    #[inline]
    pub(super) fn handoff_ahead(&mut self, key: Key) -> bool {
        let Some(flows) = self
            .sessions
            .get_mut(key)
            .and_then(|s| s.inbound.flows.as_mut())
        else {
            return false;
        };
        if flows.last == 0 {
            return false;
        }
        let shared = &self.shared;
        let done = self.handoffs.waits.get(&flows.last).is_none_or(|waits| {
            waits
                .iter()
                .all(|w| shared.cursor(w.journal).check(w.n).is_some())
        });
        if done {
            flows.last = 0;
        }
        !done
    }

    /// What the last handoff of session `key` waits for.
    pub(crate) fn handoff_waits(&self, key: Key) -> Vec<Wait> {
        self.sessions
            .get(key)
            .and_then(|s| s.inbound.flows.as_ref())
            .and_then(|f| self.handoff_waits_of(f.last))
            .unwrap_or_default()
    }

    pub(super) fn restore_handoffs(&mut self, mut state: Handoffs) {
        for pending in state.pending.values() {
            self.handoffs.charge.add(pending.bytes());
        }
        // A crash can leave the store with a handoff its source journal lost.
        // Its number must not come again: the store would take it for a repeat.
        state.serial = state.serial.max(self.shared.retained.receipt(self.id));
        self.handoffs.state = state;
    }

    /// Called after all sessions and their subscriptions are installed.
    pub(crate) fn resume_handoffs(&mut self) {
        let pending: Vec<_> = self.handoffs.state.pending.values().cloned().collect();
        for p in pending {
            self.needs.clear();
            if let Some(&key) = self.by_client.get(&*p.publisher)
                && let Some(s) = self
                    .sessions
                    .get_mut(key)
                    .filter(|s| s.birth == p.birth && s.inbound.awaiting_rel.contains(&p.pkid))
            {
                let flows = s.inbound.flows.get_or_insert_default();
                flows.by_pkid.insert(p.pkid, p.seq);
                flows.last = flows.last.max(p.seq);
            }
            let retained = if p.msg.retain {
                let full = self.budget.full;
                self.shared.retained.set_once(self.id, p.seq, &p.msg, full)
            } else {
                None
            };
            // Its record stays until the store has it.
            self.needs.note(retained);
            self.queue_handoff(&p, [Due::Now, Due::Now.then(retained)]);
        }
        self.needs.clear();
    }

    /// Looks at the next [`REAP`] handoffs for those that finished, in turn,
    /// so those stuck on a slow lane do not hide the others. Handoffs whose
    /// publisher is still there go when their acks do: this finds the rest.
    pub(super) fn reap_handoffs(&mut self) {
        let from = self.handoffs.reap_from;
        let seqs: Vec<u64> = self
            .handoffs
            .waits
            .range(from..)
            .take(REAP)
            .map(|(&seq, _)| seq)
            .collect();
        self.handoffs.reap_from = match seqs.last() {
            Some(last) if seqs.len() == REAP => last + 1,
            _ => 0,
        };
        for seq in seqs {
            self.reap_handoff(seq);
        }
    }

    /// Forgets handoff `seq` if every destination has it, or if it never
    /// reached disk, so that nothing delivers it. A written one a destination
    /// failed on stays for the next start: its publisher has a PUBREC.
    pub(super) fn reap_handoff(&mut self, seq: u64) {
        let Some(waits) = self.handoffs.waits.get(&seq) else {
            return;
        };
        let mut written = true;
        for w in waits {
            match self.shared.cursor(w.journal).check(w.n) {
                Some(ok) => written &= ok,
                None => return,
            }
        }
        let refused = |p: &Arc<Pending>| !p.ready.load(Ordering::Relaxed);
        if !written && !self.handoffs.state.pending.get(&seq).is_none_or(refused) {
            return;
        }
        self.handoffs.waits.remove(&seq);
        if let Some(p) = self.handoffs.state.pending.remove(&seq) {
            self.handoffs.charge.sub(p.bytes());
            self.journal_handed(seq);
        }
    }

    /// Takes a handoff, behind those the journal could not take yet.
    pub(super) fn take_handoff(&mut self, h: Arriving) {
        if !self.handoffs.later.is_empty() || !self.receive_handoff(&h) {
            self.handoffs.later.push_back(h);
        }
    }

    /// Takes the handoffs the journal could not take before, while it can.
    pub(super) fn retry_handoffs(&mut self) {
        while let Some(h) = self.handoffs.later.pop_front() {
            if !self.receive_handoff(&h) {
                self.handoffs.later.push_front(h);
                return;
            }
        }
    }

    /// False if the journal cannot take handoff `h` yet. Otherwise its source
    /// learns how it went.
    fn receive_handoff(&mut self, h: &Arriving) -> bool {
        let Some(written) = self.apply_handoff(h.from, h.seq, &h.pending) else {
            return false;
        };
        self.got_handoff_from(h.from, h.sequence, written);
        true
    }

    /// Whether the journal has what handoff `seq` made of the lane's sessions,
    /// or will: `None` if it cannot have it yet, which changed nothing.
    fn apply_handoff(&mut self, from: u16, seq: u64, p: &Pending) -> Option<bool> {
        // The mailbox publishes preparation before transferring ownership.
        if !p.ready.load(Ordering::Relaxed) {
            return Some(false);
        }
        if self.handoffs.state.received(from) >= seq {
            return Some(true);
        }
        if !self.begin_handoff() {
            return None;
        }
        let publication = Publication::of(&p.msg);
        let mut owned = Owned::of(p.msg.clone());
        self.congestion.hit = None;
        for t in &p.targets {
            if t.lane != self.id {
                continue;
            }
            let Some(&key) = self.by_client.get(&*t.client) else {
                continue;
            };
            if self.sessions.get(key).is_none_or(|s| s.birth != t.birth) {
                continue;
            }
            self.deliver(
                key,
                &publication,
                &mut owned,
                Grant {
                    qos: t.qos,
                    retain: t.retain,
                    sub_id: t.sub_id,
                    last: false,
                },
            );
        }
        let j = self.end_handoff(from, seq);
        if from == self.id
            && let Some(hit) = self.congestion.hit.take()
        {
            let _ = p.congested.set(self.sub_of(hit));
        }
        self.handoffs.state.receive(from, seq);
        // What they make of it may have taken its place from the record's
        // first part. A session in RAM has nothing there to wait for.
        for target in p.targets.iter().filter(|t| t.lane == self.id) {
            if let Some(&key) = self.by_client.get(&*target.client)
                && let Some(s) = self
                    .sessions
                    .get_mut(key)
                    .filter(|s| s.birth == target.birth && (s.journaled || j == Journaled::Failed))
            {
                s.out.due = s.out.due.then(Some(j));
            }
        }
        if j == Journaled::Failed {
            self.rewrite_journal();
            return Some(false);
        }
        Some(true)
    }

    /// Puts handoff `r` in the batches of its destinations. Its destinations
    /// refuse it unless `written`, which fails the ack that waits for them.
    fn send_handoff(&mut self, r: &Ready, written: bool, epoch: u64) {
        if self.epoch != epoch {
            return;
        }
        // Relaxed: the mailbox publishes it with the batch.
        r.pending.ready.store(written, Ordering::Relaxed);
        let seq = r.pending.seq;
        for w in self.handoffs.waits.get(&seq).into_iter().flatten() {
            if let JournalId::Handoff { to, .. } = w.journal {
                self.out.remote[to].push(Remote::Handoff {
                    pending: r.pending.clone(),
                    seq,
                    from: self.id,
                    epoch: self.epoch,
                    sequence: w.n,
                });
            }
        }
    }
}

#[cfg(test)]
impl Lane {
    /// Leaves a handoff of session `client` for packet ID `pkid`, done once
    /// `waits` are. Returns its number.
    pub(crate) fn leave_handoff(&mut self, client: &str, pkid: u16, waits: Vec<Wait>) -> u64 {
        let birth = self
            .by_client
            .get(client)
            .map_or(0, |&key| self.sessions.get(key).map_or(0, |s| s.birth));
        let seq = self.handoffs.state.next();
        let pending = Pending {
            seq,
            ready: AtomicBool::new(true),
            congested: std::sync::OnceLock::new(),
            publisher: client.into(),
            birth,
            pkid,
            msg: crate::msg::Msg::new("left", b"x", &[], zzq_proto::QoS::ExactlyOnce, false),
            targets: Vec::new(),
        };
        if let Some(s) = self
            .by_client
            .get(client)
            .and_then(|&key| self.sessions.get_mut(key))
        {
            let flows = s.inbound.flows.get_or_insert_default();
            flows.by_pkid.insert(pkid, seq);
        }
        self.handoffs.charge.add(pending.bytes());
        self.handoffs.state.pending.insert(seq, Arc::new(pending));
        self.handoffs.waits.insert(seq, waits);
        seq
    }

    /// Forgets handoff `seq` if it is done, then sends what the lane's work
    /// asked to, as when its acks go out.
    pub(crate) fn finish_handoff(&mut self, seq: u64) {
        self.reap_handoff(seq);
        self.flush();
    }

    /// Handoff `seq` never reached disk, so nothing delivers it.
    pub(crate) fn refuse_handoff(&self, seq: u64) {
        if let Some(p) = self.handoffs.state.pending.get(&seq) {
            p.ready.store(false, Ordering::Relaxed);
        }
    }

    pub(crate) fn has_handoff(&self, seq: u64) -> bool {
        self.handoffs.state.pending.contains_key(&seq)
    }

    pub(crate) fn by_client_key(&self, client: &str) -> Option<Key> {
        self.by_client.get(client).copied()
    }
}

/// Sends the lane's handoffs on as their records are written, in the order
/// they were routed: a source commits before all its destinations do. Only
/// this task waits for those writes, so nothing else on the lane depends on
/// the disk, and it holds no lock meanwhile.
pub(crate) async fn prepare(lane: LaneRef) {
    let (shared, wake) = {
        let l = lane.borrow();
        (l.shared.clone(), l.handoffs.wake.clone())
    };
    let mut count = 0;
    let mut bytes = 0;
    loop {
        let next = {
            let mut l = lane.borrow_mut();
            let epoch = l.epoch;
            l.handoffs.ready.pop_front().map(|r| (r, epoch))
        };
        let Some((r, epoch)) = next else {
            crate::broker::ship(&lane).await;
            wake.notified().await;
            continue;
        };
        let mut written = true;
        for due in r.writes {
            written &= write_done(&lane, &shared, due).await;
        }
        lane.borrow_mut().send_handoff(&r, written, epoch);
        count += 1;
        bytes += r.pending.bytes();
        if count >= PREPARE_RUN.0 || bytes >= PREPARE_RUN.1 {
            crate::broker::ship(&lane).await;
            tokio::task::yield_now().await;
            count = 0;
            bytes = 0;
        }
    }
}

/// Whether the write `due` went well. What is ready goes to its lanes first
/// if it has to wait.
async fn write_done(lane: &LaneRef, shared: &Shared, due: Due) -> bool {
    let w = match due {
        Due::Now => return true,
        Due::Failed => return false,
        Due::After(w) => w,
    };
    let cursor = shared.cursor(w.journal);
    if let Some(ok) = cursor.check(w.n) {
        return ok;
    }
    crate::broker::ship(lane).await;
    cursor.wait(w.n).await
}

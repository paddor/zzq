//! The replica task: one per broker, on a lane. It owns the node, turns
//! the journals' groups into ops while it leads and their commits into
//! written records, and keeps a follower's state until the node leads.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use zzq_cluster::OpNumber;
use zzq_cluster::node::{Event, Node, Outgoing, ProposeError, Role};

use super::app::Accumulators;
use super::image::{self, Cut};
use super::log::{Commits, Group, Journal};
use crate::broker::Shared;

mod exit;
mod lead;

use exit::Exiting;

/// Polls of the node without a deadline wait at most this long.
const IDLE: Duration = Duration::from_millis(50);

/// An op this leader proposed and the records it carries.
#[derive(Debug)]
struct Proposed {
    op: OpNumber,
    journal: Journal,
    first: u64,
    last: u64,
}

/// Where the broker's state lives.
#[derive(Debug)]
enum App {
    /// Rebuilt from replicated ops; no lane serves it.
    Follower(Accumulators),
    /// In the lanes, which serve clients.
    Leader,
}

/// The replica's ways in and out.
#[derive(Debug)]
pub(crate) struct Links {
    pub(crate) outbound: mpsc::Sender<Outgoing>,
    pub(crate) inbound: mpsc::Receiver<[Bytes; 3]>,
    /// Asks for the exit dump.
    pub(crate) exits: mpsc::Receiver<super::ExitReply>,
}

pub(crate) struct Replica {
    shared: Arc<Shared>,
    node: Node,
    links: Links,
    /// Groups not proposed yet: the pipeline was full.
    held: VecDeque<Group>,
    /// Ops proposed and not committed yet, oldest first.
    proposed: VecDeque<Proposed>,
    /// The last record of each journal taken from its groups, so a group
    /// knows its first.
    taken: FxHashMap<Journal, u64>,
    app: App,
    /// The image a leader cut, until its groups are proposed.
    cut: Option<Cut>,
    exiting: Option<Exiting>,
    /// The last op that failed to apply.
    failed: Option<OpNumber>,
    /// Since when a leader is without its lease, still the leader of its
    /// view: it keeps its state and waits.
    suspended: Option<Duration>,
    /// A forced epoch's history starts empty: the first leadership writes
    /// the whole state into it.
    rewrite: bool,
    origin: tokio::time::Instant,
}

impl std::fmt::Debug for Replica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Replica")
            .field("role", &self.node.role())
            .finish_non_exhaustive()
    }
}

impl Replica {
    /// The replica of a node as it booted.
    pub(crate) fn new(shared: Arc<Shared>, booted: super::Booted, links: Links) -> Replica {
        let super::Booted { node, app, rewrite } = booted;
        Replica {
            shared,
            node,
            rewrite,
            links,
            held: VecDeque::new(),
            proposed: VecDeque::new(),
            taken: FxHashMap::default(),
            app: App::Follower(app),
            cut: None,
            exiting: None,
            failed: None,
            suspended: None,
            origin: tokio::time::Instant::now(),
        }
    }

    fn now(&self) -> Duration {
        self.origin.elapsed()
    }

    /// Runs until the broker stops or the node exits: input, node,
    /// output, events.
    pub(crate) async fn run(mut self) {
        let groups = self.shared.cluster_groups();
        loop {
            let now = self.now();
            let wait = self
                .node
                .next_deadline()
                .map_or(IDLE, |d| d.saturating_sub(now))
                .min(IDLE);
            // Each arm takes one item or none: losing one to another is fine.
            tokio::select! {
                biased;
                frames = self.links.inbound.recv() => match frames {
                    Some(frames) => {
                        let _ = self.node.receive(&frames, self.now());
                    }
                    None => return,
                },
                Some(reply) = self.links.exits.recv(), if self.exiting.is_none() => {
                    self.begin_exit(reply).await;
                }
                () = groups.ready.ready() => self.held.extend(groups.take()),
                () = tokio::time::sleep(wait) => {}
            }
            if self.step().await {
                return;
            }
        }
    }

    /// The node's work after an input, then the exit if it is due. True
    /// once the node exited.
    async fn step(&mut self) -> bool {
        let groups = self.shared.cluster_groups();
        let now = self.now();
        // Before the poll, which sends what was proposed.
        self.propose(now);
        if self.node.poll(now).is_err() {
            // The core faulted: this node serves nothing until restart.
            self.fail_proposed();
        }
        while let Some(out) = self.node.next_outgoing() {
            // A full link drops; the node sends again.
            let _ = self.links.outbound.try_send(out);
        }
        while let Some(event) = self.node.next_event() {
            self.event(event).await;
        }
        self.check_suspended(now).await;
        // The leader's log holds its own view's history: what committed
        // there is its own, also what committed while it had no lease.
        if matches!(self.app, App::Leader) && self.node.leads_view() {
            self.commit_through(self.node.committed().op);
        }
        self.offer_cut();
        *groups
            .status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(self.node.status());
        self.poll_exit(now).await
    }

    /// Proposes the held groups while leading, as far as the pipeline
    /// takes them. Not leading, they fail.
    fn propose(&mut self, now: Duration) {
        while let Some(g) = self.held.front() {
            match self.node.propose(g.body.clone(), now) {
                Ok(op) => {
                    let Some(g) = self.held.pop_front() else {
                        break;
                    };
                    let (first, last) = self.span(&g);
                    if let Some(cut) = &mut self.cut {
                        cut.proposed(g.journal, last, op.op);
                    }
                    self.proposed.push_back(Proposed {
                        op: op.op,
                        journal: g.journal,
                        first,
                        last,
                    });
                }
                Err(ProposeError::Full) => break,
                // Still the leader of its view, without its lease for now:
                // they wait.
                Err(_) if self.node.leads_view() => break,
                // Not leading: these records never commit here.
                Err(_) => {
                    self.fail_held();
                    break;
                }
            }
        }
    }

    fn fail_held(&mut self) {
        for g in std::mem::take(&mut self.held) {
            let (first, last) = self.span(&g);
            if let Some(c) = commits(&self.shared, g.journal) {
                c.fail(first, last);
            }
        }
    }

    /// The records of `g`: after the last its journal handed over.
    fn span(&mut self, g: &Group) -> (u64, u64) {
        let taken = self.taken.entry(g.journal).or_insert(0);
        let first = (*taken + 1).min(g.last);
        *taken = g.last.max(*taken);
        (first, g.last)
    }

    /// Fails every op proposed that did not commit: the leadership went.
    fn fail_proposed(&mut self) {
        for p in std::mem::take(&mut self.proposed) {
            if let Some(c) = commits(&self.shared, p.journal) {
                c.fail(p.first, p.last);
            }
        }
    }

    async fn event(&mut self, event: Event) {
        match event {
            Event::Committed(through) => self.commit_through(through),
            Event::Apply { op, body } => self.apply(op.op, &body),
            Event::CutImage => self.cut_image().await,
            Event::Image { bytes, .. } => self.install(&bytes),
            Event::Role(Role::Leader) => self.lead().await,
            Event::Role(_) => self.step_down().await,
            Event::Reset => self.reset().await,
            _ => {}
        }
    }

    /// Applies a committed op to a follower's state. State that does not
    /// take it went wrong here: it is rebuilt, once per op. Failing again,
    /// the node applies no more and serves nothing.
    fn apply(&mut self, op: OpNumber, body: &[u8]) {
        let App::Follower(acc) = &mut self.app else {
            return;
        };
        if acc.apply(op, body).is_ok() {
            let _ = self.node.applied(op);
        } else if rebuild_after(&mut self.failed, op) {
            self.app = App::Follower(self.empty());
            self.node.reset_application();
        }
    }

    fn empty(&self) -> Accumulators {
        Accumulators::new(self.shared.lanes.len(), self.shared.opts.max_topic_levels)
    }

    /// A follower's state becomes the image's. One that does not fit is
    /// dropped, and the state comes again.
    fn install(&mut self, bytes: &Bytes) {
        let (lanes, levels) = (self.shared.lanes.len(), self.shared.opts.max_topic_levels);
        let acc = image::decode(bytes).and_then(|p| Accumulators::from_image(lanes, levels, &p));
        let Ok(acc) = acc else {
            self.app = App::Follower(self.empty());
            self.node.reset_application();
            return;
        };
        self.app = App::Follower(acc);
    }

    /// Cuts an image to offer: a follower its state at once; a leader asks
    /// each lane for its part, cuts the stripes here, and offers once the
    /// groups they sealed are proposed.
    async fn cut_image(&mut self) {
        let App::Follower(acc) = &self.app else {
            self.cut = self.cut_lanes().await;
            return;
        };
        let applied = self.node.status().applied;
        let parts = acc.image(applied);
        let _ = self
            .node
            .offer_image(applied, applied, image::encode(&parts));
    }

    async fn cut_lanes(&self) -> Option<Cut> {
        let mut asked = Vec::new();
        for lane in &self.shared.lanes {
            let (tx, rx) = tokio::sync::oneshot::channel();
            lane.control.push(crate::lane::Control::CutImage(tx)).ok()?;
            asked.push(rx);
        }
        let mut cut = Cut::default();
        for (j, rx) in asked.into_iter().enumerate() {
            let (last, body) = rx.await.ok()?;
            cut.add(Journal::Lane(u16::try_from(j).ok()?), last, body);
        }
        let retained = &self.shared.retained;
        for s in 0..retained.stripes() {
            let (last, body) = retained.cut_image(s)?;
            cut.add(Journal::Retained(u8::try_from(s).ok()?), last, body);
        }
        Some(cut)
    }

    /// Offers the leader's image once its groups are proposed.
    fn offer_cut(&mut self) {
        let Some(cut) = &self.cut else {
            return;
        };
        let taken = &self.taken;
        let tip = self.node.status().log_last;
        if let Some((predecessor, position, bytes)) =
            cut.finish(|j| taken.get(&j).copied().unwrap_or(0), tip)
        {
            self.cut = None;
            let _ = self.node.offer_image(predecessor, position, bytes);
        }
    }
}

/// Whether a follower whose state did not take op `op` rebuilds it:
/// once per op, as it fails again from an image when the op is at fault.
fn rebuild_after(failed: &mut Option<OpNumber>, op: OpNumber) -> bool {
    if *failed == Some(op) {
        return false;
    }
    *failed = Some(op);
    true
}

/// Every journal of the broker.
fn journals(shared: &Shared) -> impl Iterator<Item = Journal> {
    let lanes = (0..shared.lanes.len()).filter_map(|j| u16::try_from(j).ok().map(Journal::Lane));
    let stripes =
        (0..shared.retained.stripes()).filter_map(|s| u8::try_from(s).ok().map(Journal::Retained));
    lanes.chain(stripes)
}

/// The number of the last record `journal` appended.
fn appended(shared: &Shared, journal: Journal) -> Option<u64> {
    match journal {
        Journal::Lane(i) => shared.lane_logs.get(usize::from(i))?.cluster_appended(),
        Journal::Retained(s) => shared.retained.cluster_appended(usize::from(s)),
    }
}

/// The commits of `journal`, through the broker's journals.
pub(crate) fn commits(shared: &Shared, journal: Journal) -> Option<Arc<Commits>> {
    match journal {
        Journal::Lane(i) => shared.lane_logs.get(usize::from(i))?.commits(),
        Journal::Retained(s) => shared.retained.commits(usize::from(s)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_follower_rebuilds_once_per_op_its_state_does_not_take() {
        let mut failed = None;
        assert!(rebuild_after(&mut failed, OpNumber(4)));
        assert!(!rebuild_after(&mut failed, OpNumber(4)));
        assert!(rebuild_after(&mut failed, OpNumber(9)));
    }
}

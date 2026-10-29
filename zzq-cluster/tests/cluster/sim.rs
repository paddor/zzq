//! Three nodes on a simulated network, driven from one seed: virtual time,
//! delays, drops and splits, and an application per node that records
//! what it holds.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

use bytes::Bytes;
use zzq_cluster::driver::Timing;
use zzq_cluster::node::{Event, LogLimits, Node, NodeConfig, Role};
use zzq_cluster::wire::WireLimits;
use zzq_cluster::{Configuration, Digest, GroupId, NodeId, OpNumber, PipelineLimits};

pub(crate) const TICK: Duration = Duration::from_millis(1);

/// A small seeded generator, so a seed replays a run.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) const fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub(crate) const fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// True with chance `percent` in 100.
    pub(crate) const fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

pub(crate) const fn node_id(i: usize) -> NodeId {
    NodeId::from_bytes([i as u8 + 1; 16])
}

pub(crate) fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [0, 1, 2].map(node_id),
    )
    .unwrap()
}

pub(crate) const fn timing() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(10),
        primary_timeout: Duration::from_millis(100),
        retransmit: Duration::from_millis(20),
        election_timeout: Duration::from_millis(150),
        max_election_timeout: Duration::from_secs(1),
    }
}

pub(crate) fn node_config(i: usize, log: LogLimits) -> NodeConfig {
    NodeConfig {
        configuration: configuration(),
        local: node_id(i),
        timing: timing(),
        pipeline: PipelineLimits {
            max_operations: 64,
            max_body_bytes: 1 << 20,
        },
        log,
        wire: WireLimits::for_transfer(64, 1 << 20).unwrap(),
        fingerprint: 4,
    }
}

/// What one node's application holds: the bodies of the ops it applied
/// or proposed, in op order.
#[derive(Debug, Default)]
pub(crate) struct App {
    pub(crate) ops: Vec<Bytes>,
    /// The last of its own ops that committed.
    pub(crate) committed: OpNumber,
    pub(crate) role: Option<Role>,
}

/// Network behavior.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Faults {
    /// Chance in 100 that a message is lost.
    pub(crate) drop: u64,
    /// Most extra delay, in ticks.
    pub(crate) jitter: u64,
}

struct Flight {
    due: Duration,
    seq: u64,
    to: usize,
    frames: [Bytes; 3],
}

impl PartialEq for Flight {
    fn eq(&self, other: &Self) -> bool {
        (self.due, self.seq) == (other.due, other.seq)
    }
}
impl Eq for Flight {}
impl PartialOrd for Flight {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Flight {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.due, self.seq).cmp(&(other.due, other.seq))
    }
}

pub(crate) struct Sim {
    pub(crate) now: Duration,
    pub(crate) rng: Rng,
    pub(crate) nodes: [Option<Node>; 3],
    pub(crate) apps: [App; 3],
    pub(crate) faults: Faults,
    /// `links[a][b]`: messages from a reach b.
    pub(crate) links: [[bool; 3]; 3],
    net: BinaryHeap<Reverse<Flight>>,
    seq: u64,
    /// Every op some leader saw committed: (op, body).
    pub(crate) acked: Vec<(OpNumber, Bytes)>,
    /// Resets each application got.
    pub(crate) resets: [usize; 3],
    /// Images each application took.
    pub(crate) images: [usize; 3],
    /// The log limits of every node.
    log: LogLimits,
    /// Exit dumps and the application ops dumped with them.
    dumps: [Option<(Bytes, Vec<Bytes>)>; 3],
    /// Record each message's opcode in `traced`.
    pub(crate) trace: bool,
    pub(crate) traced: Vec<(usize, usize, u8)>,
    /// Role changes, for failure reports.
    pub(crate) timeline: Vec<String>,
    /// Inputs a node rejected: when, which node, why.
    pub(crate) errors: Vec<(Duration, usize, zzq_cluster::node::NodeError)>,
}

impl Sim {
    pub(crate) fn new(seed: u64, faults: Faults) -> Sim {
        Sim::with_log(seed, faults, LogLimits::default())
    }

    /// A sim whose nodes keep at most `log` ops.
    pub(crate) fn with_log(seed: u64, faults: Faults, log: LogLimits) -> Sim {
        let now = Duration::ZERO;
        let mut sim = Sim {
            now,
            rng: Rng::new(seed),
            nodes: [0, 1, 2].map(|i| Some(Node::bootstrap(node_config(i, log), now).unwrap())),
            apps: Default::default(),
            faults,
            links: [[true; 3]; 3],
            net: BinaryHeap::new(),
            seq: 0,
            acked: Vec::new(),
            resets: [0; 3],
            images: [0; 3],
            errors: Vec::new(),
            timeline: Vec::new(),
            dumps: Default::default(),
            trace: false,
            traced: Vec::new(),
            log,
        };
        for i in 0..3 {
            for j in (0..3).filter(|&j| j != i) {
                sim.node(i).connected(node_id(j)).unwrap();
            }
        }
        sim.drain();
        sim
    }

    pub(crate) const fn node(&mut self, i: usize) -> &mut Node {
        self.nodes[i].as_mut().unwrap()
    }

    /// The node that leads, if exactly one does.
    pub(crate) fn leader(&self) -> Option<usize> {
        let mut leaders = (0..3).filter(|&i| {
            self.nodes[i]
                .as_ref()
                .is_some_and(|n| n.role() == Role::Leader)
        });
        let first = leaders.next()?;
        leaders.next().is_none().then_some(first)
    }

    /// Node `i`'s process dies: its RAM and its application's state are
    /// gone, and so is what was on its way to it.
    pub(crate) fn crash(&mut self, i: usize) {
        self.timeline.push(format!("{:?} crash node {i}", self.now));
        self.nodes[i] = None;
        self.apps[i] = App::default();
    }

    /// Node `i` exits cleanly: its dump and its application's ops are
    /// kept for [`Sim::start`]. False if it had nothing to dump.
    pub(crate) fn stop(&mut self, i: usize) -> bool {
        self.timeline.push(format!("{:?} stop node {i}", self.now));
        let dumped = self.nodes[i].as_ref().and_then(Node::export);
        self.nodes[i] = None;
        let app = std::mem::take(&mut self.apps[i]);
        match dumped {
            Some((bytes, through)) => {
                let mut ops = app.ops;
                ops.truncate(through.0 as usize);
                self.dumps[i] = Some((bytes, ops));
                true
            }
            None => false,
        }
    }

    /// Starts node `i` from its dump, which is gone after, or without
    /// state if it has none.
    pub(crate) fn start(&mut self, i: usize) {
        let Some((bytes, ops)) = self.dumps[i].take() else {
            self.restart(i);
            return;
        };
        self.timeline.push(format!("{:?} start node {i}", self.now));
        let node = Node::restore(node_config(i, self.log), &bytes, self.now).unwrap();
        self.nodes[i] = Some(node);
        self.apps[i].ops = ops;
        for j in (0..3).filter(|&j| j != i) {
            self.node(i).connected(node_id(j)).unwrap();
        }
    }

    /// Starts node `i` again without state.
    pub(crate) fn restart(&mut self, i: usize) {
        self.timeline
            .push(format!("{:?} restart node {i}", self.now));
        let node = Node::recover(node_config(i, self.log), self.now).unwrap();
        self.nodes[i] = Some(node);
        for j in (0..3).filter(|&j| j != i) {
            self.node(i).connected(node_id(j)).unwrap();
        }
    }

    /// Splits node `i` off from the others, or heals it.
    pub(crate) fn isolate(&mut self, i: usize, cut: bool) {
        if cut {
            self.timeline.push(format!("{:?} cut node {i}", self.now));
        }
        for j in 0..3 {
            if j != i {
                self.links[i][j] = !cut;
                self.links[j][i] = !cut;
            }
        }
    }

    /// Leader `i` proposes `body`. False if it would not take it.
    pub(crate) fn propose(&mut self, i: usize, body: Bytes) -> bool {
        let now = self.now;
        match self.node(i).propose(body.clone(), now) {
            Ok(op) => {
                let app = &mut self.apps[i];
                assert_eq!(
                    app.ops.len() as u64 + 1,
                    op.op.0,
                    "node {i} proposes after what it holds"
                );
                app.ops.push(body);
                true
            }
            Err(_) => false,
        }
    }

    /// One tick: deliver what is due, poll every node, route what they
    /// send and hand their events to their applications.
    pub(crate) fn step(&mut self) {
        self.now += TICK;
        while self.net.peek().is_some_and(|f| f.0.due <= self.now) {
            let Reverse(f) = self.net.pop().unwrap();
            if let Some(node) = self.nodes[f.to].as_mut()
                && let Err(e) = node.receive(&f.frames, self.now)
            {
                self.errors.push((self.now, f.to, e));
            }
        }
        let now = self.now;
        for i in 0..3 {
            let Some(node) = self.nodes[i].as_mut() else {
                continue;
            };
            if let Err(e) = node.poll(now) {
                let st: Vec<_> = self
                    .nodes
                    .iter()
                    .map(|n| n.as_ref().map(Node::status))
                    .collect();
                panic!("node {i} poll at {now:?}: {e}\n{st:?}");
            }
        }
        self.drain();
        let leaders = self
            .nodes
            .iter()
            .flatten()
            .filter(|n| n.role() == Role::Leader)
            .count();
        if leaders > 1 {
            let st: Vec<_> = self
                .nodes
                .iter()
                .map(|n| n.as_ref().map(Node::status))
                .collect();
            let recent = &self.timeline[self.timeline.len().saturating_sub(12)..];
            panic!(
                "{leaders} leaders at {:?}: {st:?} links {:?}\n{}",
                self.now,
                self.links,
                recent.join("\n")
            );
        }
    }

    pub(crate) fn run(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    /// Steps until `done` holds, at most `ticks` times. True if it held.
    pub(crate) fn run_until(&mut self, ticks: u64, mut done: impl FnMut(&Sim) -> bool) -> bool {
        for _ in 0..ticks {
            if done(self) {
                return true;
            }
            self.step();
        }
        done(self)
    }

    fn drain(&mut self) {
        for i in 0..3 {
            let Some(node) = self.nodes[i].as_mut() else {
                continue;
            };
            let mut out = Vec::new();
            while let Some(o) = node.next_outgoing() {
                out.push(o);
            }
            let mut events = Vec::new();
            while let Some(e) = node.next_event() {
                events.push(e);
            }
            for o in out {
                let to = (0..3).find(|&j| node_id(j) == o.to).unwrap();
                if !self.links[i][to] || self.rng.chance(self.faults.drop) {
                    continue;
                }
                if self.trace {
                    self.traced.push((i, to, o.frames[0][5]));
                }
                let delay = TICK * (1 + self.rng.below(self.faults.jitter + 1) as u32);
                self.seq += 1;
                self.net.push(Reverse(Flight {
                    due: self.now + delay,
                    seq: self.seq,
                    to,
                    frames: o.frames,
                }));
            }
            for e in events {
                self.event(i, e);
            }
        }
    }

    fn event(&mut self, i: usize, e: Event) {
        match e {
            Event::Apply { op, body } => {
                let app = &mut self.apps[i];
                assert_eq!(
                    app.ops.len() as u64 + 1,
                    op.op.0,
                    "node {i} applies op {} out of order",
                    op.op.0
                );
                app.ops.push(body);
                self.node(i).applied(op.op).unwrap();
            }
            Event::Committed(op) => {
                let app = &mut self.apps[i];
                assert!(op > app.committed, "commits only move forward");
                for n in app.committed.0 + 1..=op.0 {
                    let body = app.ops[n as usize - 1].clone();
                    self.acked.push((OpNumber(n), body));
                }
                app.committed = op;
            }
            Event::Role(role) => {
                self.apps[i].role = Some(role);
                let view = self.nodes[i].as_ref().map_or(0, Node::view);
                self.timeline
                    .push(format!("{:?} node {i} {role:?} view {view}", self.now));
            }
            Event::Reset => {
                self.apps[i].ops.clear();
                self.resets[i] += 1;
            }
            Event::CutImage => {
                let ops = &self.apps[i].ops;
                let n = OpNumber(ops.len() as u64);
                let image = encode(ops);
                // A cut asked for before a reset no longer fits: refused.
                let _ = self.node(i).offer_image(n, n, image);
            }
            Event::Image {
                predecessor,
                position,
                bytes,
            } => {
                assert_eq!(predecessor, position, "one journal: no ops to skip");
                let ops = decode(&bytes);
                assert_eq!(ops.len() as u64, position.0);
                self.apps[i].ops = ops;
                self.images[i] += 1;
            }
            _ => {}
        }
    }

    /// Every pair of applications agrees on the ops both hold, and every
    /// acked op is in each that got that far.
    pub(crate) fn check(&self) {
        for a in 0..3 {
            for b in a + 1..3 {
                let (x, y) = (&self.apps[a].ops, &self.apps[b].ops);
                let n = x.len().min(y.len());
                // A leader's uncommitted tail may differ from what another
                // view committed; compare only what both applied as committed.
                let n = n.min(self.settled(a)).min(self.settled(b));
                assert_eq!(x[..n], y[..n], "nodes {a} and {b} disagree");
            }
        }
        for (op, body) in &self.acked {
            for (i, app) in self.apps.iter().enumerate() {
                if let Some(held) = app.ops.get(op.0 as usize - 1)
                    && (op.0 as usize) <= self.settled(i)
                {
                    assert_eq!(held, body, "node {i} lost acked op {}", op.0);
                }
            }
        }
    }

    /// How many of node `i`'s ops are committed.
    fn settled(&self, i: usize) -> usize {
        self.nodes[i]
            .as_ref()
            .map_or(0, |n| n.committed().op.0 as usize)
            .min(self.apps[i].ops.len())
    }
}

/// An application's ops as image bytes: each length, then its body.
fn encode(ops: &[Bytes]) -> Bytes {
    let mut out = Vec::new();
    for op in ops {
        out.extend_from_slice(&(op.len() as u32).to_be_bytes());
        out.extend_from_slice(op);
    }
    Bytes::from(out)
}

fn decode(mut bytes: &[u8]) -> Vec<Bytes> {
    let mut ops = Vec::new();
    while let Some((len, rest)) = bytes.split_first_chunk::<4>() {
        let len = u32::from_be_bytes(*len) as usize;
        ops.push(Bytes::copy_from_slice(&rest[..len]));
        bytes = &rest[len..];
    }
    ops
}

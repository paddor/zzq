//! A seeded workload against a simulated cluster: publishers and
//! persistent subscribers that move to whichever node takes them, while
//! one node at a time is cut off, crashes or stops. The oracle: every
//! QoS 1 message a publisher got a PUBACK for reaches every subscriber,
//! in order per publisher and topic.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use zzq_client::{Client, ClientOptions};
use zzq_proto::{QoS, Version};

use zzq_cluster::node::Role;

use super::sim::{Cluster, NODES};

pub(crate) const PUBLISHERS: usize = 2;
pub(crate) const SUBSCRIBERS: usize = 3;
const TOPICS: u64 = 4;
/// Messages per publisher.
const MESSAGES: u64 = 100;
/// Virtual time the faults run.
const FAULTS_FOR: Duration = Duration::from_secs(15);

/// A small seeded generator, so a seed names a run.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) const fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    const fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn millis(&mut self, from: u64, to: u64) -> Duration {
        Duration::from_millis(from + self.below(to - from))
    }
}

/// What the clients saw.
#[derive(Debug, Default)]
pub(crate) struct Oracle {
    /// Messages a PUBACK came for: publisher, number, topic.
    pub(crate) acked: Vec<(usize, u64, u64)>,
    /// Payloads each subscriber got, in order, duplicates included.
    pub(crate) delivered: [Vec<Bytes>; SUBSCRIBERS],
    pub(crate) published: usize,
    /// Faults, role changes and acks, for a failure report.
    pub(crate) log: String,
    /// When the run began.
    pub(crate) start: Option<tokio::time::Instant>,
    /// Each node's role and view, as last noted.
    seen: [Option<(Role, u64)>; NODES],
}

impl Oracle {
    fn note(&mut self, what: std::fmt::Arguments<'_>) {
        let at = self.start.map(|s| s.elapsed()).unwrap_or_default();
        let _ = writeln!(self.log, "{at:?}: {what}");
    }

    /// Notes the nodes whose role or view changed.
    fn watch(&mut self, c: &Cluster) {
        for i in 0..NODES {
            let status = c.status(i);
            let now = status.map(|s| (s.role, s.view));
            if now != self.seen[i] {
                self.seen[i] = now;
                match status {
                    Some(s) => self.note(format_args!(
                        "node {i} {:?} view {} epoch {} committed {} applied {} log {}..{}",
                        s.role,
                        s.view,
                        s.epoch,
                        s.committed.0,
                        s.applied.0,
                        s.log_base.0,
                        s.log_last.0
                    )),
                    None => self.note(format_args!("node {i} down")),
                }
            }
        }
    }
}

type Shared = Rc<RefCell<Oracle>>;

fn payload(publisher: usize, n: u64) -> String {
    format!("p{publisher}-{n}")
}

fn parse(payload: &[u8]) -> Option<(usize, u64)> {
    let s = std::str::from_utf8(payload).ok()?.strip_prefix('p')?;
    let (p, n) = s.split_once('-')?;
    Some((p.parse().ok()?, n.parse().ok()?))
}

/// A client of whichever node takes it, trying them in turn.
async fn connect(ctx: &zzq_broker::Context, id: &str, clean: bool, from: usize) -> (usize, Client) {
    let mut i = from;
    loop {
        let opts = ClientOptions {
            version: Version::V5,
            clean_start: clean,
            session_expiry: 3600,
            ..ClientOptions::new(id)
        };
        let uri = format!("inproc://node-{}", i % NODES);
        if let Ok(Ok(c)) =
            tokio::time::timeout(Duration::from_secs(1), Client::connect(ctx, &uri, opts)).await
        {
            return (i % NODES, c);
        }
        i += 1;
        if i % NODES == from % NODES {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Publishes [`MESSAGES`] messages, each again until a PUBACK comes for
/// it, over as many connections as it takes.
async fn publish(ctx: zzq_broker::Context, p: usize, mut rng: Rng, oracle: Shared) {
    let id = format!("pub{p}");
    let mut client = None;
    for n in 0..MESSAGES {
        let topic = rng.below(TOPICS);
        let name = format!("t/{topic}");
        loop {
            let c = match client.take() {
                Some(c) => c,
                None => connect(&ctx, &id, true, p).await.1,
            };
            let sent = c.publish(&name, payload(p, n), QoS::AtLeastOnce);
            if let Ok(Ok(())) = tokio::time::timeout(Duration::from_secs(5), sent).await {
                let mut o = oracle.borrow_mut();
                o.acked.push((p, n, topic));
                o.note(format_args!("acked p{p}-{n}"));
                client = Some(c);
                break;
            }
        }
        // Spread over the time the faults run.
        tokio::time::sleep(rng.millis(0, 300)).await;
    }
    oracle.borrow_mut().published += 1;
}

/// Takes every message of subscriber `s`'s session, again after each
/// lost connection.
async fn subscribe(ctx: zzq_broker::Context, s: usize, oracle: Shared) {
    let id = format!("sub{s}");
    loop {
        let (node, mut c) = connect(&ctx, &id, false, s).await;
        let present = c.connected().session_present;
        oracle.borrow_mut().note(format_args!(
            "sub{s} on node {node}, session present {present}"
        ));
        while let Some(m) = c.recv().await {
            oracle.borrow_mut().delivered[s].push(m.payload);
        }
        oracle
            .borrow_mut()
            .note(format_args!("sub{s} lost node {node}"));
    }
}

/// Waits `d`, noting role changes as they come.
async fn pause(c: &Cluster, d: Duration, oracle: &Shared) {
    let end = tokio::time::Instant::now() + d;
    while tokio::time::Instant::now() < end {
        oracle.borrow_mut().watch(c);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Cuts off, crashes or stops one node at a time, then brings it back
/// and waits until it follows again.
async fn faults(c: &mut Cluster, mut rng: Rng, oracle: &Shared) -> Result<(), String> {
    let start = tokio::time::Instant::now();
    let end = start + FAULTS_FOR;
    while tokio::time::Instant::now() < end {
        pause(c, rng.millis(100, 1000), oracle).await;
        let i = rng.below(NODES as u64) as usize;
        let down = rng.millis(100, 2000);
        let kind = rng.below(4);
        oracle
            .borrow_mut()
            .note(format_args!("fault {kind} on node {i} for {down:?}"));
        match kind {
            0 => {
                c.isolate(i, true);
                pause(c, down, oracle).await;
                c.isolate(i, false);
            }
            1 => {
                c.crash(i);
                pause(c, down, oracle).await;
                c.restart(i).await;
            }
            2 => {
                c.stop(i).await;
                pause(c, down, oracle).await;
                c.restart(i).await;
            }
            _ => {}
        }
        let settled = c.until(10, |c| {
            oracle.borrow_mut().watch(c);
            settled(c)
        });
        if !settled.await {
            let nodes: Vec<_> = (0..NODES).map(|i| c.status(i)).collect();
            return Err(format!("node {i} did not come back\n{nodes:#?}"));
        }
    }
    Ok(())
}

/// Every node follows or leads, one leads, and none recovers.
fn settled(c: &Cluster) -> bool {
    c.leader().is_some()
        && (0..NODES).all(|i| {
            c.status(i)
                .is_some_and(|s| !s.recovering && matches!(s.role, Role::Leader | Role::Follower))
        })
}

/// Runs the workload from `seed`. Returns what went wrong, if anything.
pub(crate) async fn run(seed: u64) -> Result<(), String> {
    let mut c = Cluster::new().await;
    if !c.until(2, |c| c.leader().is_some()).await {
        return Err("no first leader".into());
    }
    for s in 0..SUBSCRIBERS {
        let (_, sub) = connect(&c.ctx, &format!("sub{s}"), true, 0).await;
        sub.subscribe("t/#", QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("subscribe: {e}"))?;
        sub.disconnect().await;
    }
    let oracle = Shared::default();
    oracle.borrow_mut().start = Some(tokio::time::Instant::now());
    let mut rng = Rng::new(seed);
    for p in 0..PUBLISHERS {
        let r = Rng::new(rng.next());
        tokio::task::spawn_local(publish(c.ctx.clone(), p, r, oracle.clone()));
    }
    for s in 0..SUBSCRIBERS {
        tokio::task::spawn_local(subscribe(c.ctx.clone(), s, oracle.clone()));
    }
    let mut net = Rng::new(rng.next());
    c.lossy(net.below(10), net.below(20));
    let faulted = faults(&mut c, Rng::new(rng.next()), &oracle).await;
    c.lossy(0, 0);
    let published = c
        .until(60, |_| oracle.borrow().published == PUBLISHERS)
        .await;
    let delivered = c
        .until(60, |c| {
            let mut o = oracle.borrow_mut();
            o.watch(c);
            missing(&o).is_empty()
        })
        .await;
    let o = oracle.borrow();
    if let Err(e) = faulted {
        return Err(format!("{e}\n{}", o.log));
    }
    if !published {
        let nodes: Vec<_> = (0..NODES).map(|i| c.status(i)).collect();
        return Err(format!(
            "publishers stuck at {} acks\n{}{nodes:#?}",
            o.acked.len(),
            o.log
        ));
    }
    if !delivered {
        let m = missing(&o);
        return Err(format!("{} acked messages lost: {m:?}\n{}", m.len(), o.log));
    }
    out_of_order(&o).map_or(Ok(()), |e| Err(format!("{e}\n{}", o.log)))
}

/// Acked messages some subscriber lacks: subscriber, publisher, number.
fn missing(o: &Oracle) -> Vec<(usize, usize, u64)> {
    let mut out = Vec::new();
    for (s, got) in o.delivered.iter().enumerate() {
        let got: HashSet<(usize, u64)> = got.iter().filter_map(|p| parse(p)).collect();
        for &(p, n, _) in &o.acked {
            if !got.contains(&(p, n)) {
                out.push((s, p, n));
            }
        }
    }
    out
}

/// A subscriber that first got an acked message after a later one of
/// its publisher and topic.
fn out_of_order(o: &Oracle) -> Option<String> {
    let topic_of: HashMap<(usize, u64), u64> =
        o.acked.iter().map(|&(p, n, t)| ((p, n), t)).collect();
    for (s, got) in o.delivered.iter().enumerate() {
        let mut seen = HashSet::new();
        let mut last: HashMap<(usize, u64), u64> = HashMap::default();
        for (p, n) in got.iter().filter_map(|p| parse(p)) {
            let Some(&t) = topic_of.get(&(p, n)) else {
                continue;
            };
            if !seen.insert((p, n)) {
                continue;
            }
            if let Some(&before) = last.get(&(p, t))
                && before > n
            {
                return Some(format!("sub{s} got p{p}-{n} after p{p}-{before} on t/{t}"));
            }
            last.insert((p, t), n);
        }
    }
    None
}

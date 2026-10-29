//! Fixed-load test: many connections, each publishing on a schedule.
//!
//! Each publisher sends its share of the offered rate on a fixed schedule
//! with its own phase, and the payload carries the send time. A step runs
//! for `warmup`, then is measured for `measure`, on the same connections.
//! It fails under 95% delivered, on a closed connection or at a median
//! latency over 100 ms, and no further steps run.
//!
//! Latency is kept per second of the window. A second with host CPU steal
//! (the hypervisor ran something else on one of this VM's CPUs) is left
//! out, unless all are.

mod client;
mod hist;
mod steal;
mod trace;

use std::fmt;
use std::io;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;

use crate::cpu::{CpuUse, CpuWindow};
use hist::Hist;
use steal::Steal;

/// Generator threads. Each runs its own current-thread runtime with its
/// share of the connections, so a wake never crosses threads.
const THREADS: usize = 2;

/// Latency is kept per part of the window this long, in ns, so a part
/// with host CPU steal can be left out.
const PART: u64 = 1_000_000_000;

/// Steal in a part, or up to `STEAL_AFTER` ns after it, that leaves it
/// out, in `USER_HZ` ticks (10 ms) on any CPU.
const STEAL_TICKS: u64 = 1;
const STEAL_AFTER: u64 = 100_000_000;

/// When the first step starts, in ns after the clock does.
const LEAD: u64 = 1_000_000_000;

/// How long after its window a step's latency is read, in ns. Shorter than
/// any warmup, so the next window has not started.
const GRACE: u64 = 1_000_000_000;

/// How long all clients may take to connect and subscribe.
const CONNECT_LIMIT: Duration = Duration::from_secs(120);

/// Width of a trace timeline slot, in ns.
const SLOT: u64 = 10_000_000;

/// Traffic shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// `pubs` publishers, each to its own topic with its own subscriber.
    P2p,
    /// `pubs` publishers on distinct topics, `subs` subscribers in one
    /// shared subscription group, so each message is delivered once.
    Fanin,
    /// `pubs` publishers, `subs` subscribers to all of their topics.
    Fanout,
}

impl Scenario {
    /// Subscriber connections for `cfg`.
    pub const fn subscribers(self, cfg: &Fixed) -> usize {
        match self {
            Scenario::P2p => cfg.pubs,
            Scenario::Fanin | Scenario::Fanout => cfg.subs,
        }
    }

    /// Deliveries per published message.
    const fn fan(self, cfg: &Fixed) -> usize {
        match self {
            Scenario::Fanout => cfg.subs,
            Scenario::P2p | Scenario::Fanin => 1,
        }
    }
}

impl FromStr for Scenario {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "p2p" => Ok(Scenario::P2p),
            "fanin" => Ok(Scenario::Fanin),
            "fanout" => Ok(Scenario::Fanout),
            _ => Err(format!("unknown scenario {s}")),
        }
    }
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scenario::P2p => "p2p",
            Scenario::Fanin => "fanin",
            Scenario::Fanout => "fanout",
        })
    }
}

/// Fixed-load settings.
#[derive(Debug, Clone)]
pub struct Fixed {
    /// Broker port.
    pub port: u16,
    /// Connections go to 127.0.0.1 through 127.0.0.`addrs`, so more of
    /// them fit than one source port range holds.
    pub addrs: u8,
    /// Speak v5 instead of v3.1.1.
    pub v5: bool,
    /// Publish and subscribe QoS, 0 or 1.
    pub qos: u8,
    /// Payload size in bytes, at least 8 for the send time.
    pub size: usize,
    /// Publisher connections.
    pub pubs: usize,
    /// Subscriber connections for fan-in and fan-out.
    pub subs: usize,
    /// Offered deliveries per second, one step each.
    pub rates: Vec<f64>,
    /// Time each step runs before it is measured, over 1 s.
    pub warmup: Duration,
    /// Time each step is measured.
    pub measure: Duration,
    /// Connections opened per second.
    pub connect_rate: f64,
    /// The PIDs of the broker's processes, for its CPU and memory.
    pub broker_pids: Vec<u32>,
    /// Also report send lateness, run queue waits, steal, and the slowest
    /// deliveries in time.
    pub trace: bool,
}

impl Default for Fixed {
    fn default() -> Self {
        Fixed {
            port: 1883,
            addrs: 8,
            v5: true,
            qos: 1,
            size: 16,
            pubs: 10_000,
            subs: 0,
            rates: vec![10_000.0, 20_000.0, 50_000.0, 100_000.0, 200_000.0],
            warmup: Duration::from_secs(2),
            measure: Duration::from_secs(8),
            connect_rate: 5000.0,
            broker_pids: Vec::new(),
            trace: false,
        }
    }
}

/// How the clients connected.
#[derive(Debug, Clone, Default)]
pub struct Connect {
    /// Clients to connect.
    pub clients: usize,
    /// Clients connected and subscribed.
    pub connected: usize,
    /// Connection attempts the broker refused, then retried.
    pub retries: usize,
    /// Time from the first connect until all were done.
    pub took: Duration,
    /// Not all clients connected within the time limit.
    pub timed_out: bool,
    /// Why the first client that failed did.
    pub error: Option<String>,
}

impl Connect {
    /// All clients connected.
    pub const fn ok(&self) -> bool {
        self.connected == self.clients
    }
}

/// One step's result.
#[derive(Debug, Clone, Default)]
pub struct Step {
    /// Offered deliveries per second.
    pub offered: f64,
    /// Deliveries per second in the window.
    pub delivered: f64,
    /// Percent of expected deliveries missing.
    pub loss: f64,
    /// Connections the broker closed in the window. On a failed connect,
    /// the clients that did not connect.
    pub closed: u64,
    /// Broker and generator CPU, and broker memory.
    pub cpu: CpuUse,
    /// Latency in microseconds: p50, p90, p99, p99.9 and max.
    pub latency: [f64; 5],
    /// Deliveries the latency counts.
    pub samples: u64,
    /// Parts of the window left out for steal, and all parts.
    pub steal: Option<(usize, usize)>,
    /// Under 95% delivered, a closed connection, or a median over 100 ms.
    pub failed: bool,
    /// With [`Fixed::trace`]: lateness, waits and the slowest deliveries.
    pub trace: Option<String>,
}

impl fmt::Display for Step {
    /// The result line from the offered rate on.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [p50, p90, p99, p999, max] = self.latency;
        write!(
            f,
            "offered {:.0}/s  delivered {:.0}/s  loss {:.1}%  closed {}  {}  p50 {p50:.1}us  \
             p90 {p90:.1}us  p99 {p99:.1}us  p99.9 {p999:.1}us  max {max:.1}us  samples {}  ",
            self.offered, self.delivered, self.loss, self.closed, self.cpu, self.samples,
        )?;
        if let Some((dropped, parts)) = self.steal {
            write!(f, "steal {dropped}/{parts}s  ")?;
        }
        f.write_str(if self.failed { "failed" } else { "ok" })
    }
}

/// What [`run`] reports as it goes.
#[derive(Debug)]
pub enum Report<'a> {
    /// The clients connected, or not all did. Then a failed step follows
    /// and the run ends.
    Connect(&'a Connect),
    /// A step was measured.
    Step(&'a Step),
}

/// What all connections of a run share.
#[derive(Debug)]
struct Plan {
    cfg: Fixed,
    scenario: Scenario,
    /// Topics and client IDs carry it, so runs do not share them.
    pid: u32,
    connected: AtomicUsize,
    failed: AtomicUsize,
    retries: AtomicUsize,
    timed_out: AtomicBool,
    error: Mutex<Option<String>>,
}

impl Plan {
    fn new(scenario: Scenario, cfg: &Fixed) -> Plan {
        Plan {
            cfg: cfg.clone(),
            scenario,
            pid: std::process::id(),
            connected: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            retries: AtomicUsize::new(0),
            timed_out: AtomicBool::new(false),
            error: Mutex::new(None),
        }
    }
}

/// Step times of a run, in ns since `epoch`.
#[derive(Debug)]
struct Clock {
    epoch: Instant,
    /// Start and per-publisher send period of each step.
    steps: Vec<(u64, u64)>,
    end: u64,
}

impl Clock {
    /// Steps start `LEAD` from now, back to back.
    fn new(plan: &Plan) -> Clock {
        let cfg = &plan.cfg;
        let len = step_len(cfg);
        let pubs = (plan.scenario.fan(cfg) * cfg.pubs) as f64;
        let steps: Vec<(u64, u64)> = (0..)
            .zip(&cfg.rates)
            .map(|(k, &rate)| (LEAD + k * len, (1e9 * pubs / rate) as u64))
            .collect();
        let end = steps.last().map_or(0, |s| s.0 + len);
        Clock {
            epoch: Instant::now(),
            steps,
            end,
        }
    }

    fn now(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    fn at(&self, ns: u64) -> Instant {
        self.epoch + Duration::from_nanos(ns)
    }
}

/// Latency of the deliveries sent in the window, by send time.
#[derive(Debug)]
struct Latencies {
    /// One histogram per `PART`.
    parts: Box<[Hist]>,
    /// With trace: how late sends were written, and the slowest delivery
    /// and latest send per `SLOT`.
    late: Hist,
    slots: Box<[AtomicU64]>,
    late_slots: Box<[AtomicU64]>,
}

impl Latencies {
    fn new(measure: Duration) -> Latencies {
        let ns = measure.as_nanos() as u64;
        let slots = || (0..=ns / SLOT).map(|_| AtomicU64::new(0)).collect();
        Latencies {
            parts: (0..ns.div_ceil(PART)).map(|_| Hist::new()).collect(),
            late: Hist::new(),
            slots: slots(),
            late_slots: slots(),
        }
    }

    fn clear(&self) {
        for h in self.parts.iter().chain([&self.late]) {
            h.clear();
        }
        for s in self.slots.iter().chain(&self.late_slots) {
            s.store(0, Relaxed);
        }
    }
}

/// State shared by all connections of a run. Counters and the window
/// bounds are statistics read after the fact, so `Relaxed` is enough.
#[derive(Debug)]
struct Shared {
    plan: Arc<Plan>,
    clock: Clock,
    sent: AtomicU64,
    recv: AtomicU64,
    closed: AtomicU64,
    /// Send times (ns) whose latency is recorded: from, until.
    window: (AtomicU64, AtomicU64),
    lat: Latencies,
    steal: Steal,
    stop: AtomicBool,
}

impl Shared {
    fn new(plan: Arc<Plan>) -> Shared {
        Shared {
            clock: Clock::new(&plan),
            lat: Latencies::new(plan.cfg.measure),
            plan,
            sent: AtomicU64::new(0),
            recv: AtomicU64::new(0),
            closed: AtomicU64::new(0),
            window: (AtomicU64::new(u64::MAX), AtomicU64::new(u64::MAX)),
            steal: Steal::default(),
            stop: AtomicBool::new(false),
        }
    }

    fn window(&self) -> (u64, u64) {
        (self.window.0.load(Relaxed), self.window.1.load(Relaxed))
    }

    /// Counts a connection the broker closed, unless the run is over.
    fn lost(&self) {
        if !self.stop.load(Relaxed) {
            self.closed.fetch_add(1, Relaxed);
        }
    }

    fn counters(&self) -> [u64; 3] {
        [&self.sent, &self.recv, &self.closed].map(|c| c.load(Relaxed))
    }
}

fn step_len(cfg: &Fixed) -> u64 {
    (cfg.warmup + cfg.measure).as_nanos() as u64
}

/// Connects the clients, then runs the steps, reporting each as it ends.
///
/// # Errors
///
/// On settings out of range, or if a generator thread cannot start.
pub fn run(scenario: Scenario, cfg: &Fixed, mut report: impl FnMut(Report<'_>)) -> io::Result<()> {
    check(scenario, cfg)?;
    let plan = Arc::new(Plan::new(scenario, cfg));
    let runtimes = (0..THREADS)
        .map(|_| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
        })
        .collect::<io::Result<Vec<Runtime>>>()?;
    let started = Instant::now();
    thread::scope(|s| {
        let (done_tx, done_rx) = mpsc::channel();
        let mut handoff = Vec::new();
        for (t, rt) in runtimes.into_iter().enumerate() {
            let (tx, rx) = mpsc::channel();
            handoff.push(tx);
            let (plan, done) = (plan.clone(), done_tx.clone());
            thread::Builder::new()
                .name(format!("zl-{t}"))
                .spawn_scoped(s, move || client::worker(t, &rt, &plan, done, &rx))?;
        }
        drop(done_tx);
        // Each worker reports once its share connected or gave up.
        let all = done_rx.iter().take(THREADS).count() == THREADS;
        let connect = connect_result(&plan, started);
        report(Report::Connect(&connect));
        if !all || !connect.ok() {
            report(Report::Step(&Step {
                offered: cfg.rates.first().copied().unwrap_or(0.0),
                loss: 100.0,
                closed: (connect.clients - connect.connected) as u64,
                failed: true,
                ..Step::default()
            }));
            return Ok(());
        }
        let shared = Arc::new(Shared::new(plan.clone()));
        for tx in handoff {
            let _ = tx.send(shared.clone());
        }
        let sampler = shared.clone();
        thread::Builder::new()
            .name("zl-steal".into())
            .spawn_scoped(s, move || {
                steal::sample(&sampler.steal, &sampler.clock, &sampler.stop);
            })
            .inspect_err(|_| shared.stop.store(true, Relaxed))?;
        drive(&shared, &mut report);
        shared.stop.store(true, Relaxed);
        Ok(())
    })
}

fn check(scenario: Scenario, cfg: &Fixed) -> io::Result<()> {
    let bad = if cfg.size < 8 {
        "the payload carries an 8-byte send time"
    } else if cfg.qos > 1 {
        "QoS 0 and 1 only"
    } else if cfg.warmup.as_nanos() <= u128::from(GRACE) {
        "the warmup takes over 1 s"
    } else if cfg.measure.is_zero() || cfg.rates.is_empty() || cfg.rates.iter().any(|&r| r <= 0.0) {
        "a step needs a measured window and a rate"
    } else if cfg.pubs == 0 || scenario.subscribers(cfg) == 0 || cfg.addrs == 0 {
        "publishers, subscribers and addresses"
    } else if cfg.connect_rate <= 0.0 {
        "a connect rate"
    } else {
        return Ok(());
    };
    Err(io::Error::new(io::ErrorKind::InvalidInput, bad))
}

fn connect_result(plan: &Plan, started: Instant) -> Connect {
    Connect {
        clients: plan.scenario.subscribers(&plan.cfg) + plan.cfg.pubs,
        connected: plan.connected.load(Relaxed),
        retries: plan.retries.load(Relaxed),
        took: started.elapsed(),
        timed_out: plan.timed_out.load(Relaxed),
        error: plan
            .error
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
    }
}

/// Runs the steps until one fails.
fn drive(sh: &Shared, report: &mut impl FnMut(Report<'_>)) {
    for (k, &rate) in sh.plan.cfg.rates.iter().enumerate() {
        let step = measure(sh, k, rate);
        report(Report::Step(&step));
        if step.failed {
            return;
        }
    }
}

/// Measures step `k`'s window.
fn measure(sh: &Shared, k: usize, rate: f64) -> Step {
    let cfg = &sh.plan.cfg;
    let until = |ns: u64| thread::sleep(sh.clock.at(ns).saturating_duration_since(Instant::now()));
    let begin = sh.clock.steps.get(k).map_or(0, |s| s.0);
    let start = begin + cfg.warmup.as_nanos() as u64;
    let stop = begin + step_len(cfg);
    until(start);
    sh.lat.clear();
    sh.window.0.store(start, Relaxed);
    sh.window.1.store(stop, Relaxed);
    let [sent0, recv0, closed0] = sh.counters();
    let cpu = CpuWindow::start(&cfg.broker_pids);
    let sched0 = cfg.trace.then(trace::Sched::now);
    until(stop);
    let [sent1, recv1, closed1] = sh.counters();
    let sched1 = cfg.trace.then(trace::Sched::now);
    let cpu = cpu.finish();
    // Late deliveries of this window come in during the next warmup.
    until(stop + GRACE);
    let (counts, dropped) = kept_counts(sh, start);
    let (latency, samples) = hist::quantiles(&counts, [0.5, 0.9, 0.99, 0.999, 1.0]);
    let got = (recv1 - recv0) as f64;
    let delivered = got / cfg.measure.as_secs_f64();
    let expected = (sent1 - sent0) as f64 * sh.plan.scenario.fan(cfg) as f64;
    let loss = if expected > 0.0 {
        100.0 * (1.0 - got / expected).max(0.0)
    } else {
        0.0
    };
    let trace = sched0.zip(sched1).map(|(a, b)| trace::report(sh, &a, &b));
    Step {
        offered: rate,
        delivered,
        loss,
        closed: closed1 - closed0,
        cpu,
        latency,
        samples,
        steal: Some((dropped, sh.lat.parts.len())),
        failed: delivered < 0.95 * rate || closed1 > closed0 || latency[0] > 100_000.0,
        trace,
    }
}

/// Latency counts of the window from `lo`, without the parts that had
/// steal, and how many those were. With steal in every part, all count.
fn kept_counts(sh: &Shared, lo: u64) -> (Vec<u64>, usize) {
    let parts = &sh.lat.parts;
    let stolen: Vec<bool> = (0..parts.len() as u64)
        .map(|i| {
            let (from, to) = (lo + i * PART, lo + (i + 1) * PART + STEAL_AFTER);
            sh.steal.worst(from, to) >= STEAL_TICKS
        })
        .collect();
    let all = stolen.iter().all(|&s| s);
    let mut counts = vec![0; sh.lat.late.len()];
    for (h, _) in parts.iter().zip(&stolen).filter(|&(_, &s)| all || !s) {
        for (c, n) in counts.iter_mut().zip(h.counts()) {
            *c += n;
        }
    }
    (counts, stolen.iter().filter(|&&s| s).count())
}

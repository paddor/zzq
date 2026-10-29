//! Throughput and latency scenarios.

use std::io::{self, ErrorKind, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use crate::cpu::{CpuUse, CpuWindow};
use crate::wire::{self, Head, PUBACK, PUBCOMP, PUBREC, PUBREL, Reader};

/// Time stopped publishers have to finish their last write before the run
/// shuts the sockets down.
#[cfg(not(test))]
const STALL: Duration = Duration::from_secs(10);
#[cfg(test)]
const STALL: Duration = Duration::from_millis(200);

/// Longest wait for deliveries after the publishers stop, so a broker that
/// trickles out a backlog cannot hold up a run.
#[cfg(not(test))]
const DRAIN: Duration = Duration::from_secs(90);
#[cfg(test)]
const DRAIN: Duration = Duration::from_millis(300);

/// Connection notes a run keeps.
const NOTES: usize = 32;

/// Traffic shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// `pubs` publishers, each with its own topic and subscriber.
    Pairs,
    /// One publisher, `subs` subscribers on one topic.
    Fanout,
    /// `pubs` publishers on distinct topics, one wildcard subscriber.
    Fanin,
}

/// Load settings.
#[derive(Debug, Clone)]
pub struct Load {
    /// Broker address, `host:port`.
    pub addr: String,
    /// Speak v5 instead of v3.1.1.
    pub v5: bool,
    /// Publish QoS, 0 to 2.
    pub qos: u8,
    /// Payload size in bytes.
    pub size: usize,
    /// Publisher connections.
    pub pubs: usize,
    /// Subscriber connections.
    pub subs: usize,
    /// Time to run before measuring.
    pub warmup: Duration,
    /// Time to measure.
    pub measure: Duration,
    /// QoS 1/2 messages in flight per publisher.
    pub window: usize,
    /// Lanes of the zzq broker under test. Client IDs are chosen so pair `i`
    /// lands on lane `i % lanes`. Other brokers ignore this.
    pub lanes: usize,
    /// Pairs: put each publisher on the lane after its subscriber's.
    pub cross: bool,
    /// Topics per publisher, cycled message by message. Above 1,
    /// subscribers use `/#` filters.
    pub topics: usize,
    /// What the broker has to keep.
    pub keep: Keep,
    /// The PIDs of the broker's processes. Their CPU and memory, with
    /// their descendants', go into the results.
    pub broker_pids: Vec<u32>,
}

impl Default for Load {
    fn default() -> Self {
        Load {
            addr: "127.0.0.1:1883".to_owned(),
            v5: true,
            qos: 0,
            size: 16,
            pubs: 1,
            subs: 1,
            warmup: Duration::from_secs(1),
            measure: Duration::from_secs(3),
            window: 100,
            lanes: 1,
            cross: false,
            topics: 1,
            keep: Keep::default(),
            broker_pids: Vec::new(),
        }
    }
}

/// What the broker has to keep: the state a data directory puts on disk.
#[derive(Debug, Clone, Copy, Default)]
pub struct Keep {
    /// Publish with the retain flag.
    pub retain: bool,
    /// Subscribers have sessions that outlive their connection.
    pub sessions: bool,
}

/// Throughput result.
#[derive(Debug, Clone, Default)]
pub struct Throughput {
    /// Publishes per second in the window.
    pub published: f64,
    /// Deliveries per second in the window, summed over subscribers.
    pub delivered: f64,
    /// Percent of expected deliveries missing after the drain.
    pub loss: f64,
    /// Connections the broker closed.
    pub closed: u64,
    /// QoS 1/2 acks with a v5 failure reason code.
    pub nacks: u64,
    /// A publisher was still blocked in a write 10 s after the publishers
    /// were told to stop, so the run shut its sockets down.
    pub stalled: bool,
    /// Closed and disconnected connections, as `client ID: reason`. The
    /// first 32.
    pub notes: Vec<String>,
    /// Deliveries per second for each second of the run.
    pub per_second: Vec<f64>,
    /// Broker and load generator CPU over the measured window.
    pub cpu: CpuUse,
}

#[derive(Default, Debug)]
struct Run {
    sent: AtomicU64,
    recv: AtomicU64,
    closed: AtomicU64,
    nacks: AtomicU64,
    pub_stop: AtomicBool,
    sub_stop: AtomicBool,
    sockets: Mutex<Vec<TcpStream>>,
    notes: Mutex<Vec<String>>,
}

impl Run {
    /// Keeps a note about connection `id`, up to [`NOTES`].
    fn note(&self, id: &str, what: &str) {
        let mut notes = self.notes.lock().unwrap_or_else(PoisonError::into_inner);
        if notes.len() < NOTES {
            notes.push(format!("{id}: {what}"));
        }
    }

    /// Counts a connection the broker closed or failed.
    fn lost(&self, id: &str, e: &io::Error) {
        self.closed.fetch_add(1, Relaxed);
        if e.kind() == ErrorKind::UnexpectedEof {
            self.note(id, "broker closed");
        } else {
            self.note(id, &e.to_string());
        }
    }

    fn register(&self, s: &TcpStream) -> io::Result<()> {
        let clone = s.try_clone()?;
        self.sockets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(clone);
        Ok(())
    }

    fn shutdown_all(&self) {
        for s in self
            .sockets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
        {
            let _ = s.shutdown(Shutdown::Both);
        }
    }
}

/// Topics and connection counts of one run.
struct Shape {
    scenario: Scenario,
    pid: u32,
    pubs: usize,
    subs: usize,
    /// Deliveries per publish.
    fan: u64,
    topics: usize,
}

impl Shape {
    fn new(scenario: Scenario, load: &Load) -> Shape {
        let (pubs, subs, fan) = match scenario {
            Scenario::Pairs => (load.pubs, load.pubs, 1),
            Scenario::Fanout => (1, load.subs, load.subs as u64),
            Scenario::Fanin => (load.pubs, 1, 1),
        };
        Shape {
            scenario,
            pid: std::process::id(),
            pubs,
            subs,
            fan,
            topics: load.topics.max(1),
        }
    }

    fn topic(&self, i: usize) -> String {
        let pid = self.pid;
        match self.scenario {
            Scenario::Pairs => format!("zb/{pid}/p/{i}"),
            Scenario::Fanout => format!("zb/{pid}/f"),
            Scenario::Fanin => format!("zb/{pid}/i/{i}"),
        }
    }

    fn filter(&self, i: usize) -> String {
        match (self.scenario, self.topics > 1) {
            (Scenario::Fanin, false) => format!("zb/{}/i/+", self.pid),
            (Scenario::Fanin, true) => format!("zb/{}/i/#", self.pid),
            (_, false) => self.topic(i),
            (_, true) => format!("{}/#", self.topic(i)),
        }
    }
}

/// A connected client.
struct Conn {
    id: String,
    s: TcpStream,
    rd: Reader,
    /// The broker's Receive Maximum.
    rmax: Option<u16>,
}

impl Conn {
    fn open(addr: &str, id: String, v5: bool, persistent: bool) -> io::Result<Conn> {
        let (s, rd, rmax) = if persistent {
            wire::connect_persistent(addr, &id, v5)?
        } else {
            wire::connect(addr, &id, v5)?
        };
        Ok(Conn { id, s, rd, rmax })
    }
}

/// Subscribers, and publishers with their topics.
type Conns = (Vec<Conn>, Vec<(String, Conn)>);

/// Connects all subscribers, then all publishers.
fn connect_all(shape: &Shape, load: &Load, run: &Run) -> io::Result<Conns> {
    let pid = shape.pid;
    let mut subs = Vec::new();
    for i in 0..shape.subs {
        let id = client_id(&format!("zb-{pid}-s{i}"), i, load.lanes);
        let mut c = Conn::open(&load.addr, id, load.v5, load.keep.sessions)?;
        wire::subscribe(&mut c.s, &mut c.rd, &shape.filter(i), load.qos, load.v5)?;
        c.s.set_read_timeout(Some(Duration::from_millis(100)))?;
        run.register(&c.s)?;
        subs.push(c);
    }
    let mut pubs = Vec::new();
    for i in 0..shape.pubs {
        let id = client_id(
            &format!("zb-{pid}-p{i}"),
            i + usize::from(load.cross),
            load.lanes,
        );
        let c = Conn::open(&load.addr, id, load.v5, false)?;
        run.register(&c.s)?;
        pubs.push((shape.topic(i), c));
    }
    Ok((subs, pubs))
}

/// Runs a throughput scenario.
///
/// # Errors
///
/// If a connection fails, the broker closes one, or a reply is malformed.
pub fn throughput(scenario: Scenario, load: &Load) -> io::Result<Throughput> {
    let shape = Shape::new(scenario, load);
    let run = Arc::new(Run::default());
    let (subs, pubs) = connect_all(&shape, load, &run)?;
    let subs: Vec<_> = subs
        .into_iter()
        .map(|c| {
            let run = run.clone();
            thread::spawn(move || subscriber(c, &run))
        })
        .collect();
    let pubs: Vec<_> = pubs
        .into_iter()
        .map(|(topic, c)| {
            let (load, run) = (load.clone(), run.clone());
            thread::spawn(move || publisher(&load, &topic, c, &run))
        })
        .collect();
    let (samples, cpu) = sample(&run, load);
    let stalled = stop_publishers(&run, pubs);
    let expected = run.sent.load(Relaxed) * shape.fan;
    drain(&run, expected);
    run.sub_stop.store(true, Relaxed);
    run.shutdown_all();
    for t in subs {
        let _ = t.join();
    }
    Ok(Throughput {
        stalled,
        cpu,
        ..summarize(&run, &samples, expected)
    })
}

/// Stops the publishers and waits up to [`STALL`] for them. Returns whether
/// one was still blocked, after shutting all sockets down to free it.
fn stop_publishers(run: &Run, pubs: Vec<thread::JoinHandle<()>>) -> bool {
    run.pub_stop.store(true, Relaxed);
    let deadline = Instant::now() + STALL;
    let mut stalled = false;
    for p in pubs {
        while !p.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if !p.is_finished() {
            stalled = true;
            run.shutdown_all();
        }
        let _ = p.join();
    }
    stalled
}

fn summarize(run: &Run, samples: &[(u64, u64, f64)], expected: u64) -> Throughput {
    let recv = run.recv.load(Relaxed);
    let first = samples.first().copied().unwrap_or_default();
    let last = samples.last().copied().unwrap_or_default();
    let dt = (last.2 - first.2).max(1e-9);
    let per_second = samples
        .windows(2)
        .filter(|w| w[1].2 > w[0].2)
        .map(|w| (w[1].1 - w[0].1) as f64 / (w[1].2 - w[0].2))
        .collect();
    Throughput {
        published: (last.0 - first.0) as f64 / dt,
        delivered: (last.1 - first.1) as f64 / dt,
        loss: if expected > 0 {
            100.0 * (1.0 - recv as f64 / expected as f64)
        } else {
            0.0
        },
        closed: run.closed.load(Relaxed),
        nacks: run.nacks.load(Relaxed),
        stalled: false,
        notes: run
            .notes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone(),
        per_second,
        cpu: CpuUse::default(),
    }
}

/// A client ID starting with `prefix` that a zzq broker with `lanes` lanes
/// places on lane `lane % lanes`.
fn client_id(prefix: &str, lane: usize, lanes: usize) -> String {
    if lanes <= 1 {
        return prefix.to_owned();
    }
    (0..1_000_000)
        .map(|n| format!("{prefix}-{n}"))
        .find(|id| zzq_broker::lane_for(id, lanes) == lane % lanes)
        .unwrap_or_else(|| prefix.to_owned())
}

/// (sent, received, seconds) at the window start, each second, and the end,
/// and the CPU over the window.
fn sample(run: &Run, load: &Load) -> (Vec<(u64, u64, f64)>, CpuUse) {
    let start = Instant::now();
    thread::sleep(load.warmup);
    let window = CpuWindow::start(&load.broker_pids);
    let snap = || {
        (
            run.sent.load(Relaxed),
            run.recv.load(Relaxed),
            start.elapsed().as_secs_f64(),
        )
    };
    let mut samples = vec![snap()];
    let end = start + load.warmup + load.measure;
    loop {
        let next = (Instant::now() + Duration::from_secs(1)).min(end);
        thread::sleep(next.saturating_duration_since(Instant::now()));
        samples.push(snap());
        if Instant::now() >= end {
            return (samples, window.finish());
        }
    }
}

/// Waits until every expected delivery came, 2 s pass without one, or
/// [`DRAIN`] is up.
fn drain(run: &Run, expected: u64) {
    let start = Instant::now();
    let mut idle_since = start;
    let mut prev = run.recv.load(Relaxed);
    while run.recv.load(Relaxed) < expected
        && idle_since.elapsed() < Duration::from_secs(2)
        && start.elapsed() < DRAIN
    {
        thread::sleep(Duration::from_millis(20));
        let now = run.recv.load(Relaxed);
        if now != prev {
            prev = now;
            idle_since = Instant::now();
        }
    }
}

fn timed_out(e: &io::Error) -> bool {
    matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
}

/// `DISCONNECT 0x8e` for the body of a DISCONNECT.
fn disconnect(body: &[u8]) -> String {
    format!("DISCONNECT {:#04x}", body.first().copied().unwrap_or(0))
}

fn subscriber(mut c: Conn, run: &Run) {
    if let Err(e) = deliveries(&mut c, run)
        && !run.sub_stop.load(Relaxed)
    {
        run.lost(&c.id, &e);
    }
}

/// Reads and acks deliveries until the run stops.
fn deliveries(c: &mut Conn, run: &Run) -> io::Result<()> {
    let mut out = Vec::new();
    loop {
        match c.rd.fill(&mut c.s) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(_) => {}
            Err(e) if timed_out(&e) => {
                if run.sub_stop.load(Relaxed) {
                    return Ok(());
                }
                continue;
            }
            Err(e) => return Err(e),
        }
        let mut k = 0;
        while let Some((h, body)) = c.rd.next_packet() {
            match h >> 4 {
                3 => {
                    k += 1;
                    match (h >> 1) & 3 {
                        0 => {}
                        1 => wire::ack(&mut out, PUBACK, &wire::publish_pkid(body)),
                        _ => wire::ack(&mut out, PUBREC, &wire::publish_pkid(body)),
                    }
                }
                6 => wire::ack(&mut out, PUBCOMP, body),
                14 => run.note(&c.id, &disconnect(body)),
                _ => {}
            }
        }
        if k > 0 {
            run.recv.fetch_add(k, Relaxed);
        }
        if !out.is_empty() {
            c.s.write_all(&out)?;
            out.clear();
        }
    }
}

fn publisher(load: &Load, topic: &str, mut c: Conn, run: &Run) {
    let payload = vec![b'x'; load.size];
    let topics: Vec<String> = if load.topics > 1 {
        (0..load.topics).map(|j| format!("{topic}/{j}")).collect()
    } else {
        vec![topic.to_owned()]
    };
    let result = if load.qos == 0 {
        publish_qos0(&mut c.s, &topics, &payload, load, run)
    } else {
        let window = load.window.min(usize::from(c.rmax.unwrap_or(u16::MAX)));
        publish_acked(&mut c, (&topics, &payload), load, window, run)
    };
    if let Err(e) = result
        && !run.pub_stop.load(Relaxed)
    {
        run.lost(&c.id, &e);
    }
}

fn publish_qos0(
    s: &mut TcpStream,
    topics: &[String],
    payload: &[u8],
    load: &Load,
    run: &Run,
) -> io::Result<()> {
    // One packet per topic, repeated to about 64 KiB.
    let mut ring = Vec::new();
    for topic in topics {
        let head = Head {
            topic,
            qos: 0,
            pkid: 0,
            v5: load.v5,
            retain: load.keep.retain,
        };
        wire::publish(&mut ring, head, payload);
    }
    let reps = (65_536 / ring.len()).clamp(1, 1024);
    let k = reps * topics.len();
    let chunk = ring.repeat(reps);
    while !run.pub_stop.load(Relaxed) {
        s.write_all(&chunk)?;
        run.sent.fetch_add(k as u64, Relaxed);
    }
    Ok(())
}

fn publish_acked(
    c: &mut Conn,
    (topics, payload): (&[String], &[u8]),
    load: &Load,
    window: usize,
    run: &Run,
) -> io::Result<()> {
    c.s.set_read_timeout(Some(Duration::from_millis(100)))?;
    let mut out = Vec::new();
    let mut outstanding = 0;
    let mut pkid: u16 = 0;
    let mut seq = 0usize;
    let mut stop_at = None;
    loop {
        if !run.pub_stop.load(Relaxed) {
            let mut n = 0;
            while outstanding < window {
                pkid = pkid.wrapping_add(1).max(1);
                seq += 1;
                let head = Head {
                    topic: &topics[seq % topics.len()],
                    qos: load.qos,
                    pkid,
                    v5: load.v5,
                    retain: load.keep.retain,
                };
                wire::publish(&mut out, head, payload);
                outstanding += 1;
                n += 1;
            }
            run.sent.fetch_add(n, Relaxed);
        } else if outstanding == 0
            || stop_at.get_or_insert_with(Instant::now).elapsed() > Duration::from_secs(2)
        {
            return Ok(());
        }
        if !out.is_empty() {
            c.s.write_all(&out)?;
            out.clear();
        }
        match c.rd.fill(&mut c.s) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(_) => {}
            Err(e) if timed_out(&e) => continue,
            Err(e) => return Err(e),
        }
        outstanding -= take_acks(c, &mut out, load.v5, run);
    }
}

/// Handles the broker's acks and returns how many flows they ended. A v5
/// ack with a failure reason code is a nack, and a failed PUBREC ends its
/// flow without a PUBREL.
fn take_acks(c: &mut Conn, out: &mut Vec<u8>, v5: bool, run: &Run) -> usize {
    let (mut done, mut nacks) = (0, 0);
    while let Some((h, body)) = c.rd.next_packet() {
        let kind = h >> 4;
        let failed = v5 && matches!(kind, 4 | 5 | 7) && body.get(2).is_some_and(|&r| r >= 0x80);
        nacks += u64::from(failed);
        match kind {
            4 | 7 => done += 1,
            5 if failed => done += 1,
            5 => wire::ack(out, PUBREL, body),
            14 => run.note(&c.id, &disconnect(body)),
            _ => {}
        }
    }
    if nacks > 0 {
        run.nacks.fetch_add(nacks, Relaxed);
    }
    done
}

/// Latency result in microseconds.
#[derive(Debug, Clone, Default)]
pub struct Latency {
    /// Median.
    pub p50: f64,
    /// 90th percentile.
    pub p90: f64,
    /// 99th percentile.
    pub p99: f64,
    /// 99.9th percentile.
    pub p999: f64,
    /// Slowest round trip.
    pub max: f64,
    /// Messages not delivered within one second.
    pub lost: u64,
    /// Round trips measured, after the warmup.
    pub rounds: u64,
    /// Broker and load generator CPU over the measured round trips.
    pub cpu: CpuUse,
}

/// What a latency run times from the publish.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Until {
    /// The subscriber has the message.
    #[default]
    Delivered,
    /// The publisher has its PUBACK. A broker that keeps the message on
    /// disk sends it once the message is there.
    Acked,
}

/// One message in flight between two connections of one thread, QoS 0 or 1,
/// timed until `until`. The payload carries a sequence number, so `size`
/// must be at least 8. With [`Load::lanes`], the subscriber is on lane 0
/// and the publisher too, or on lane 1 with [`Load::cross`].
///
/// # Errors
///
/// If `size` is below 8, `qos` above 1, or `qos` 0 with [`Until::Acked`],
/// if a connection fails, the broker closes one, a reply is malformed, or
/// a v5 PUBACK carries a failure reason code.
pub fn latency(load: &Load, iters: usize, until: Until) -> io::Result<Latency> {
    if load.size < 8 || load.qos > 1 || (until == Until::Acked && load.qos == 0) {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "latency needs size >= 8 and QoS 0 or 1, and QoS 1 to time acks",
        ));
    }
    let mut rtt = Rtt::connect(load)?;
    let mut rounds = Rounds::new(iters / 10, iters, Instant::now());
    let mut rtts = Vec::with_capacity(iters);
    let mut lost = 0;
    let mut window = CpuWindow::start(&load.broker_pids);
    let mut seq = 0;
    while let Some(measured) = rounds.next(Instant::now()) {
        if measured && rounds.done == 1 {
            window = CpuWindow::start(&load.broker_pids);
        }
        match rtt.round(seq, load, until)? {
            None => lost += 1,
            Some(d) if measured => rtts.push(d),
            Some(_) => {}
        }
        seq += 1;
    }
    Ok(Latency {
        rounds: rounds.done as u64,
        cpu: window.finish(),
        ..percentiles(rtts, lost)
    })
}

/// Longest warmup of a latency run.
const RTT_WARM: Duration = Duration::from_secs(10);
/// Longest measurement of a latency run, so a slow broker cannot hold up a
/// suite.
const RTT_TIME: Duration = Duration::from_secs(60);

/// The round trips of a latency run: `warm` unmeasured ones, for at most
/// [`RTT_WARM`], then `iters` measured ones, for at most [`RTT_TIME`].
struct Rounds {
    warm: usize,
    iters: usize,
    start: Instant,
    warmed: usize,
    measuring: Option<Instant>,
    /// Measured rounds so far.
    done: usize,
}

impl Rounds {
    const fn new(warm: usize, iters: usize, start: Instant) -> Rounds {
        Rounds {
            warm,
            iters,
            start,
            warmed: 0,
            measuring: None,
            done: 0,
        }
    }

    /// Whether the next round is measured, or `None` to stop.
    fn next(&mut self, now: Instant) -> Option<bool> {
        if self.measuring.is_none()
            && (self.warmed >= self.warm || now.duration_since(self.start) >= RTT_WARM)
        {
            self.measuring = Some(now);
        }
        match self.measuring {
            None => {
                self.warmed += 1;
                Some(false)
            }
            Some(from) if self.done < self.iters && now.duration_since(from) < RTT_TIME => {
                self.done += 1;
                Some(true)
            }
            Some(_) => None,
        }
    }
}

/// The clients and buffers of a latency run.
struct Rtt {
    sub: Conn,
    publisher: Conn,
    topic: String,
    payload: Vec<u8>,
    out: Vec<u8>,
    acks: Vec<u8>,
    pkid: u16,
}

impl Rtt {
    fn connect(load: &Load) -> io::Result<Rtt> {
        let pid = std::process::id();
        let topic = format!("zb/{pid}/rtt");
        let id = client_id(&format!("zb-{pid}-rs"), 0, load.lanes);
        let mut sub = Conn::open(&load.addr, id, load.v5, load.keep.sessions)?;
        wire::subscribe(&mut sub.s, &mut sub.rd, &topic, load.qos, load.v5)?;
        sub.s.set_read_timeout(Some(Duration::from_secs(1)))?;
        let id = client_id(&format!("zb-{pid}-rp"), usize::from(load.cross), load.lanes);
        let publisher = Conn::open(&load.addr, id, load.v5, false)?;
        publisher.s.set_read_timeout(Some(Duration::from_secs(1)))?;
        Ok(Rtt {
            sub,
            publisher,
            topic,
            payload: vec![b'x'; load.size],
            out: Vec::new(),
            acks: Vec::new(),
            pkid: 0,
        })
    }

    /// Publishes message `seq` and times it until `until`. `None` if it was
    /// not delivered, or not acked, within the read timeout.
    fn round(&mut self, seq: u64, load: &Load, until: Until) -> io::Result<Option<Duration>> {
        self.pkid = self.pkid.wrapping_add(1).max(1);
        self.payload[..8].copy_from_slice(&seq.to_le_bytes());
        self.out.clear();
        let head = Head {
            topic: &self.topic,
            qos: load.qos,
            pkid: self.pkid,
            v5: load.v5,
            retain: load.keep.retain,
        };
        wire::publish(&mut self.out, head, &self.payload);
        let t0 = Instant::now();
        self.publisher.s.write_all(&self.out)?;
        let acked = until == Until::Delivered || wait_ack(&mut self.publisher, self.pkid, load.v5)?;
        let acked_at = t0.elapsed();
        let got = wait_for(
            &mut self.sub.s,
            &mut self.sub.rd,
            seq,
            load.v5,
            &mut self.acks,
        )?;
        let rtt = match until {
            Until::Delivered => t0.elapsed(),
            Until::Acked => acked_at,
        };
        if !self.acks.is_empty() {
            self.sub.s.write_all(&self.acks)?;
            self.acks.clear();
        }
        if until == Until::Delivered {
            discard_ready(&mut self.publisher.s, &mut self.publisher.rd)?;
        }
        Ok((acked && got).then_some(rtt))
    }
}

/// Reads the publisher's packets until the PUBACK of `pkid`. `Ok(false)` if
/// it did not come within the read timeout.
fn wait_ack(c: &mut Conn, pkid: u16, v5: bool) -> io::Result<bool> {
    let want = pkid.to_be_bytes();
    loop {
        while let Some((h, body)) = c.rd.next_packet() {
            if h >> 4 != 4 || body.get(..2) != Some(&want[..]) {
                continue;
            }
            return match body.get(2) {
                Some(&r) if v5 && r >= 0x80 => Err(io::Error::other(format!("PUBACK {r:#04x}"))),
                _ => Ok(true),
            };
        }
        match c.rd.fill(&mut c.s) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(_) => {}
            Err(e) if timed_out(&e) => return Ok(false),
            Err(e) => return Err(e),
        }
    }
}

/// Reads and drops whatever the broker sent, without blocking.
fn discard_ready(s: &mut TcpStream, rd: &mut Reader) -> io::Result<()> {
    s.set_nonblocking(true)?;
    while let Ok(k) = rd.fill(s) {
        if k == 0 {
            break;
        }
        while rd.next_packet().is_some() {}
    }
    s.set_nonblocking(false)
}

fn percentiles(mut rtts: Vec<Duration>, lost: u64) -> Latency {
    rtts.sort();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    let at = |q: f64| {
        let i = ((rtts.len() as f64 * q) as usize).min(rtts.len().saturating_sub(1));
        rtts.get(i).map_or(0.0, |d| us(*d))
    };
    Latency {
        p50: at(0.50),
        p90: at(0.90),
        p99: at(0.99),
        p999: at(0.999),
        max: rtts.last().map_or(0.0, |d| us(*d)),
        lost,
        rounds: 0,
        cpu: CpuUse::default(),
    }
}

fn wait_for(
    sub: &mut TcpStream,
    rd: &mut Reader,
    seq: u64,
    v5: bool,
    acks: &mut Vec<u8>,
) -> io::Result<bool> {
    loop {
        match rd.fill(sub) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(_) => {}
            Err(e) if timed_out(&e) => return Ok(false),
            Err(e) => return Err(e),
        }
        while let Some((h, body)) = rd.next_packet() {
            if h >> 4 != 3 {
                continue;
            }
            let qos = (h >> 1) & 3;
            if qos == 1 {
                wire::ack(acks, PUBACK, &wire::publish_pkid(body));
            }
            let payload = wire::publish_payload(body, qos, v5);
            if payload.get(..8) == Some(&seq.to_le_bytes()[..]) {
                return Ok(true);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::TcpListener;

    use super::*;

    /// A client connection named `id` and the server end of it.
    fn pair(id: &str) -> (Conn, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let s = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let c = Conn {
            id: id.to_owned(),
            s,
            rd: Reader::with_capacity(1 << 10),
            rmax: None,
        };
        (c, server)
    }

    #[test]
    fn a_latency_run_stops_at_its_count_or_its_time() {
        let t0 = Instant::now();
        let mut r = Rounds::new(2, 3, t0);
        let seq: Vec<_> = std::iter::from_fn(|| r.next(t0)).collect();
        assert_eq!(seq, [false, false, true, true, true]);
        let mut r = Rounds::new(1000, 1000, t0);
        assert_eq!(r.next(t0), Some(false));
        assert_eq!(r.next(t0 + RTT_WARM), Some(true), "a slow warmup ends");
        assert_eq!(r.next(t0 + RTT_WARM + RTT_TIME), None);
        assert_eq!(r.done, 1);
    }

    #[test]
    fn a_drain_ends_while_deliveries_still_trickle_in() {
        let run = Arc::new(Run::default());
        let trickle = {
            let run = run.clone();
            thread::spawn(move || {
                while !run.sub_stop.load(Relaxed) {
                    run.recv.fetch_add(1, Relaxed);
                    thread::sleep(Duration::from_millis(10));
                }
            })
        };
        let t0 = Instant::now();
        drain(&run, u64::MAX);
        assert!(t0.elapsed() < Duration::from_secs(2), "{:?}", t0.elapsed());
        run.sub_stop.store(true, Relaxed);
        trickle.join().unwrap();
    }

    #[test]
    fn a_publisher_still_blocked_after_the_stop_is_stalled_and_freed() {
        let run = Run::default();
        assert!(!stop_publishers(&run, vec![thread::spawn(|| {})]));
        let (mut c, _server) = pair("p");
        run.register(&c.s).unwrap();
        let blocked = thread::spawn(move || {
            let _ = c.s.read(&mut [0; 1]);
        });
        assert!(stop_publishers(&run, vec![blocked]));
    }

    #[test]
    fn acks_end_flows_and_failed_ones_count_as_nacks() {
        let run = Run::default();
        let (mut c, _server) = pair("p");
        let mut acks: &[u8] = &[
            0x40, 3, 0, 1, 0x87, // PUBACK, not authorized
            0x40, 2, 0, 2, // PUBACK
            0x50, 3, 0, 3, 0x80, // PUBREC, failed
            0x50, 2, 0, 4, // PUBREC
            0x70, 2, 0, 4, // PUBCOMP
            0xe0, 1, 0x8e, // DISCONNECT, session taken over
        ];
        c.rd.fill(&mut acks).unwrap();
        let mut out = Vec::new();
        assert_eq!(take_acks(&mut c, &mut out, true, &run), 4);
        assert_eq!(run.nacks.load(Relaxed), 2);
        assert_eq!(out, [0x62, 2, 0, 4]);
        assert_eq!(*run.notes.lock().unwrap(), ["p: DISCONNECT 0x8e"]);
    }

    #[test]
    fn an_ack_wait_skips_other_packets_and_fails_on_a_nack() {
        let (mut c, mut server) = pair("p");
        c.s.set_read_timeout(Some(Duration::from_millis(50)))
            .unwrap();
        server
            .write_all(&[
                0x40, 2, 0, 1, // PUBACK of another packet
                0x50, 2, 0, 2, // PUBREC
                0x40, 3, 0, 2, 0, // PUBACK, success
            ])
            .unwrap();
        assert!(wait_ack(&mut c, 2, true).unwrap());
        assert!(!wait_ack(&mut c, 3, true).unwrap());
        server.write_all(&[0x40, 3, 0, 3, 0x80]).unwrap();
        let e = wait_ack(&mut c, 3, true).unwrap_err();
        assert_eq!(e.to_string(), "PUBACK 0x80");
        server.write_all(&[0x40, 3, 0, 4, 0x80]).unwrap();
        assert!(wait_ack(&mut c, 4, false).unwrap());
        drop(server);
        let e = wait_ack(&mut c, 5, true).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::UnexpectedEof);
    }

    #[test]
    fn connections_the_broker_closes_are_counted_and_noted() {
        let run = Run::default();
        let (c, mut server) = pair("s");
        server.write_all(&[0xe0, 1, 0x8b]).unwrap();
        drop(server);
        subscriber(c, &run);
        let (c, server) = pair("p");
        drop(server);
        let load = Load {
            qos: 1,
            ..Load::default()
        };
        publisher(&load, "t", c, &run);
        run.lost("x", &io::Error::other("reset"));
        assert_eq!(run.closed.load(Relaxed), 3);
        let notes = run.notes.lock().unwrap();
        assert_eq!(notes[..2], ["s: DISCONNECT 0x8b", "s: broker closed"]);
        assert!(notes[2].starts_with("p: "), "{notes:?}");
        assert_eq!(notes[3], "x: reset");
    }
}

//! Throughput and latency scenarios.

use std::io::{self, ErrorKind, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use crate::wire::{self, Head, PUBACK, PUBCOMP, PUBREC, PUBREL, Reader};

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
        }
    }
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
    /// Deliveries per second for each second of the run.
    pub per_second: Vec<f64>,
}

#[derive(Default, Debug)]
struct Run {
    sent: AtomicU64,
    recv: AtomicU64,
    closed: AtomicU64,
    pub_stop: AtomicBool,
    sub_stop: AtomicBool,
    sockets: Mutex<Vec<TcpStream>>,
}

impl Run {
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

type SubConn = (TcpStream, Reader);
type PubConn = (String, TcpStream, Reader, Option<u16>);

/// Connects all subscribers, then all publishers.
fn connect_all(shape: &Shape, load: &Load, run: &Run) -> io::Result<(Vec<SubConn>, Vec<PubConn>)> {
    let pid = shape.pid;
    let mut subs = Vec::new();
    for i in 0..shape.subs {
        let id = client_id(&format!("zb-{pid}-s{i}"), i, load.lanes);
        let (mut s, mut rd, _) = wire::connect(&load.addr, &id, load.v5)?;
        wire::subscribe(&mut s, &mut rd, &shape.filter(i), load.qos, load.v5)?;
        s.set_read_timeout(Some(Duration::from_millis(100)))?;
        run.register(&s)?;
        subs.push((s, rd));
    }
    let mut pubs = Vec::new();
    for i in 0..shape.pubs {
        let id = client_id(
            &format!("zb-{pid}-p{i}"),
            i + usize::from(load.cross),
            load.lanes,
        );
        let (s, rd, rmax) = wire::connect(&load.addr, &id, load.v5)?;
        run.register(&s)?;
        pubs.push((shape.topic(i), s, rd, rmax));
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
        .map(|(s, rd)| {
            let run = run.clone();
            thread::spawn(move || subscriber(s, rd, &run))
        })
        .collect();
    let pubs: Vec<_> = pubs
        .into_iter()
        .map(|(topic, s, rd, rmax)| {
            let (load, run) = (load.clone(), run.clone());
            thread::spawn(move || publisher(&load, &topic, (s, rd, rmax), &run))
        })
        .collect();
    let samples = sample(&run, load.warmup, load.measure);
    run.pub_stop.store(true, Relaxed);
    let deadline = Instant::now() + Duration::from_secs(10);
    for p in pubs {
        while !p.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if !p.is_finished() {
            run.shutdown_all();
        }
        let _ = p.join();
    }
    let expected = run.sent.load(Relaxed) * shape.fan;
    drain(&run, expected);
    run.sub_stop.store(true, Relaxed);
    run.shutdown_all();
    for t in subs {
        let _ = t.join();
    }
    Ok(summarize(&run, &samples, expected))
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
        per_second,
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

/// (sent, received, seconds) at the window start, each second, and the end.
fn sample(run: &Run, warmup: Duration, measure: Duration) -> Vec<(u64, u64, f64)> {
    let start = Instant::now();
    thread::sleep(warmup);
    let snap = || {
        (
            run.sent.load(Relaxed),
            run.recv.load(Relaxed),
            start.elapsed().as_secs_f64(),
        )
    };
    let mut samples = vec![snap()];
    let end = start + warmup + measure;
    loop {
        let next = (Instant::now() + Duration::from_secs(1)).min(end);
        thread::sleep(next.saturating_duration_since(Instant::now()));
        samples.push(snap());
        if Instant::now() >= end {
            return samples;
        }
    }
}

fn drain(run: &Run, expected: u64) {
    let mut idle_since = Instant::now();
    let mut prev = run.recv.load(Relaxed);
    while run.recv.load(Relaxed) < expected && idle_since.elapsed() < Duration::from_secs(2) {
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

fn subscriber(mut s: TcpStream, mut rd: Reader, run: &Run) {
    let mut out = Vec::new();
    loop {
        match rd.fill(&mut s) {
            Ok(0) => break,
            Ok(_) => {}
            Err(e) if timed_out(&e) => {
                if run.sub_stop.load(Relaxed) {
                    return;
                }
                continue;
            }
            Err(_) => break,
        }
        let mut k = 0;
        while let Some((h, body)) = rd.next_packet() {
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
                _ => {}
            }
        }
        if k > 0 {
            run.recv.fetch_add(k, Relaxed);
        }
        if !out.is_empty() {
            if s.write_all(&out).is_err() {
                break;
            }
            out.clear();
        }
    }
    if !run.sub_stop.load(Relaxed) {
        run.closed.fetch_add(1, Relaxed);
    }
}

fn publisher(load: &Load, topic: &str, conn: (TcpStream, Reader, Option<u16>), run: &Run) {
    let (mut s, mut rd, rmax) = conn;
    let payload = vec![b'x'; load.size];
    let topics: Vec<String> = if load.topics > 1 {
        (0..load.topics).map(|j| format!("{topic}/{j}")).collect()
    } else {
        vec![topic.to_owned()]
    };
    let result = if load.qos == 0 {
        publish_qos0(&mut s, &topics, &payload, load.v5, run)
    } else {
        let window = load.window.min(usize::from(rmax.unwrap_or(u16::MAX)));
        publish_acked((&mut s, &mut rd), (&topics, &payload), load, window, run)
    };
    if result.is_err() && !run.pub_stop.load(Relaxed) {
        run.closed.fetch_add(1, Relaxed);
    }
}

fn publish_qos0(
    s: &mut TcpStream,
    topics: &[String],
    payload: &[u8],
    v5: bool,
    run: &Run,
) -> io::Result<()> {
    // One packet per topic, repeated to about 64 KiB.
    let mut ring = Vec::new();
    for topic in topics {
        let head = Head {
            topic,
            qos: 0,
            pkid: 0,
            v5,
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
    (s, rd): (&mut TcpStream, &mut Reader),
    (topics, payload): (&[String], &[u8]),
    load: &Load,
    window: usize,
    run: &Run,
) -> io::Result<()> {
    s.set_read_timeout(Some(Duration::from_millis(100)))?;
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
            s.write_all(&out)?;
            out.clear();
        }
        match rd.fill(s) {
            Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
            Ok(_) => {}
            Err(e) if timed_out(&e) => continue,
            Err(e) => return Err(e),
        }
        while let Some((h, body)) = rd.next_packet() {
            match h >> 4 {
                4 | 7 => outstanding -= 1,
                5 => wire::ack(&mut out, PUBREL, body),
                _ => {}
            }
        }
    }
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
}

/// One message in flight between two connections of one thread, QoS 0 or 1.
/// The payload carries a sequence number, so `size` must be at least 8.
///
/// # Errors
///
/// If `size` is below 8 or `qos` above 1, if a connection fails, the broker closes one, or a reply is malformed.
pub fn latency(load: &Load, iters: usize) -> io::Result<Latency> {
    if load.size < 8 || load.qos > 1 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "latency needs size >= 8 and QoS 0 or 1",
        ));
    }
    let pid = std::process::id();
    let topic = format!("zb/{pid}/rtt");
    let (mut sub, mut sub_rd, _) = wire::connect(&load.addr, &format!("zb-{pid}-rs"), load.v5)?;
    wire::subscribe(&mut sub, &mut sub_rd, &topic, load.qos, load.v5)?;
    sub.set_read_timeout(Some(Duration::from_secs(1)))?;
    let (mut publisher, mut pub_rd, _) =
        wire::connect(&load.addr, &format!("zb-{pid}-rp"), load.v5)?;
    let mut payload = vec![b'x'; load.size];
    let warm = iters / 10;
    let mut rtts = Vec::with_capacity(iters);
    let mut lost = 0;
    let mut out = Vec::new();
    let mut acks = Vec::new();
    let mut packet_id: u16 = 0;
    for i in 0..(warm + iters) as u64 {
        packet_id = packet_id.wrapping_add(1).max(1);
        payload[..8].copy_from_slice(&i.to_le_bytes());
        out.clear();
        let head = Head {
            topic: &topic,
            qos: load.qos,
            pkid: packet_id,
            v5: load.v5,
        };
        wire::publish(&mut out, head, &payload);
        let t0 = Instant::now();
        publisher.write_all(&out)?;
        let got = wait_for(&mut sub, &mut sub_rd, i, load.v5, &mut acks)?;
        let rtt = t0.elapsed();
        if !got {
            lost += 1;
        } else if i >= warm as u64 {
            rtts.push(rtt);
        }
        if !acks.is_empty() {
            sub.write_all(&acks)?;
            acks.clear();
        }
        discard_ready(&mut publisher, &mut pub_rd)?;
    }
    Ok(percentiles(rtts, lost))
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

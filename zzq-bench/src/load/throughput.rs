//! Throughput: publishers flat out, subscribers counting deliveries.

use std::io::{self, ErrorKind, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use super::{Load, Scenario, client_id, timed_out};
use crate::cpu::{CpuUse, CpuWindow};
use crate::wire::{self, Conn, Head, PUBACK, PUBCOMP, PUBREC, PUBREL, Session};

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

/// Subscribers, and publishers with their topics.
type Conns = (Vec<Conn>, Vec<(String, Conn)>);

/// Connects all subscribers, then all publishers.
fn connect_all(shape: &Shape, load: &Load, run: &Run) -> io::Result<Conns> {
    let pid = shape.pid;
    let mut subs = Vec::new();
    for i in 0..shape.subs {
        let lane = load.local.map_or(i, |local| usize::from(i >= local));
        let id = client_id(&format!("zb-{pid}-s{i}"), lane, load.lanes);
        let mut c = Conn::open(&load.addr, id, load.v5, load.keep.session())?;
        c.subscribe(&shape.filter(i), load.qos)?;
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
        let c = Conn::open(&load.addr, id, load.v5, Session::Clean)?;
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

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::load::tests::pair;
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

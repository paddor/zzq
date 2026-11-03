//! Latency: one publish at a time, timed until it is delivered.

use std::io::{self, ErrorKind, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use super::{Load, client_id, timed_out};
use crate::cpu::{CpuUse, CpuWindow};
use crate::wire::{self, Conn, Head, PUBACK, Reader, Session};

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
        let mut sub = Conn::open(&load.addr, id, load.v5, load.keep.session())?;
        sub.subscribe(&topic, load.qos)?;
        sub.s.set_read_timeout(Some(Duration::from_secs(1)))?;
        let id = client_id(&format!("zb-{pid}-rp"), usize::from(load.cross), load.lanes);
        let publisher = Conn::open(&load.addr, id, load.v5, Session::Clean)?;
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

    use super::*;
    use crate::load::tests::pair;
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
}

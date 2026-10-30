//! Broker memory per piece of state, from the growth of its RSS.
//!
//! Each step adds state to the broker under test, waits for it to settle,
//! and divides the RSS growth by the items added. A zzq broker also
//! publishes its own estimate, the one `max_state_bytes` limits, on
//! `$SYS/broker/state/bytes`.

use std::io::{self, ErrorKind, Write};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, Instant};

use crate::wire::{self, Head, PUBACK, Reader};

/// Filter every offline session subscribes to, besides its own topic.
const ALL: &str = "ram/all";
/// Time for the broker to finish before RSS is read.
const SETTLE: Duration = Duration::from_secs(1);
/// Longest wait for a reply.
const TIMEOUT: Duration = Duration::from_secs(10);
/// QoS 1 messages in flight, at most.
const BURST: usize = 1000;

/// RSS growth over one step.
#[derive(Debug, Clone, Copy)]
pub struct Growth {
    /// Items added.
    pub items: usize,
    /// Broker RSS before, bytes.
    pub rss_before: u64,
    /// Broker RSS after, bytes.
    pub rss_after: u64,
    /// Growth of the broker's estimate, bytes, if it publishes one.
    pub estimate: Option<i64>,
}

impl Growth {
    /// RSS growth per item, bytes.
    #[must_use]
    pub fn per_item(&self) -> f64 {
        (self.rss_after as f64 - self.rss_before as f64) / self.items.max(1) as f64
    }

    /// Estimate growth per item, bytes.
    #[must_use]
    pub fn estimate_per_item(&self) -> Option<f64> {
        self.estimate.map(|e| e as f64 / self.items.max(1) as f64)
    }
}

/// Adds state to a broker and measures what it costs.
#[derive(Debug)]
pub struct Probe {
    addr: String,
    pid: u32,
    v5: bool,
    settle: Duration,
    publisher: TcpStream,
    replies: Reader,
    burst: usize,
    pkid: u16,
    gauge: Option<Gauge>,
    sessions: usize,
    retained: usize,
}

impl Probe {
    /// Connects a publisher to the broker at `addr`, whose process is
    /// `pid`. With `sys`, the interval of its `$SYS` messages, it also
    /// reads the broker's estimate, and each step waits for the next one.
    ///
    /// # Errors
    ///
    /// If a connection fails.
    pub fn new(addr: &str, pid: u32, v5: bool, sys: Option<Duration>) -> io::Result<Probe> {
        let (publisher, replies, rmax) = wire::connect(addr, "ram-pub", v5)?;
        publisher.set_read_timeout(Some(TIMEOUT))?;
        let gauge = match sys {
            Some(every) => Some(Gauge::new(addr, v5, every + SETTLE)?),
            None => None,
        };
        Ok(Probe {
            addr: addr.to_owned(),
            pid,
            v5,
            settle: sys.map_or(SETTLE, |every| every + SETTLE),
            publisher,
            replies,
            burst: usize::from(rmax.unwrap_or(u16::MAX)).min(BURST),
            pkid: 0,
            gauge,
            sessions: 0,
            retained: 0,
        })
    }

    /// Adds `n` offline sessions, each subscribed at QoS 1 to its own topic
    /// and to one all of them share.
    ///
    /// # Errors
    ///
    /// If a connection fails.
    pub fn sessions(&mut self, n: usize) -> io::Result<Growth> {
        let first = self.sessions;
        let g = self.measure(n, |p| {
            for i in first..first + n {
                let (mut s, mut rd, _) =
                    wire::connect_persistent(&p.addr, &format!("ram-{i}"), p.v5)?;
                s.set_read_timeout(Some(TIMEOUT))?;
                wire::subscribe(&mut s, &mut rd, &topic(i), 1, p.v5)?;
                wire::subscribe(&mut s, &mut rd, ALL, 1, p.v5)?;
                wire::disconnect(s, &mut rd)?;
            }
            Ok(())
        })?;
        self.sessions += n;
        Ok(g)
    }

    /// Queues `per` QoS 1 messages of `size` bytes for each offline
    /// session, on its own topic.
    ///
    /// # Errors
    ///
    /// If the publisher's connection fails.
    pub fn queued(&mut self, per: usize, size: usize) -> io::Result<Growth> {
        let n = self.sessions;
        let topics = (0..per).flat_map(|_| (0..n).map(topic));
        self.measure(per * n, |p| p.publish(topics, size, false))
    }

    /// Queues `count` QoS 1 messages of `size` bytes for every offline
    /// session, on the topic they share. Items are queue entries.
    ///
    /// # Errors
    ///
    /// If the publisher's connection fails.
    pub fn fanned_out(&mut self, count: usize, size: usize) -> io::Result<Growth> {
        let topics = (0..count).map(|_| ALL.to_owned());
        self.measure(count * self.sessions, |p| p.publish(topics, size, false))
    }

    /// Retains `count` messages of `size` bytes, each on a topic of its own.
    ///
    /// # Errors
    ///
    /// If the publisher's connection fails.
    pub fn retained(&mut self, count: usize, size: usize) -> io::Result<Growth> {
        let first = self.retained;
        let topics = (first..first + count).map(|i| format!("ram/{i}/state"));
        let g = self.measure(count, |p| p.publish(topics, size, true))?;
        self.retained += count;
        Ok(g)
    }

    fn measure(
        &mut self,
        items: usize,
        work: impl FnOnce(&mut Probe) -> io::Result<()>,
    ) -> io::Result<Growth> {
        let rss_before = rss(self.pid)?;
        let before = self.gauge.as_ref().and_then(|g| g.last);
        work(self)?;
        match &mut self.gauge {
            Some(g) => g.watch(self.settle, false)?,
            None => thread::sleep(self.settle),
        }
        let after = self.gauge.as_ref().and_then(|g| g.last);
        Ok(Growth {
            items,
            rss_before,
            rss_after: rss(self.pid)?,
            estimate: before
                .zip(after)
                .map(|(b, a)| a.cast_signed() - b.cast_signed()),
        })
    }

    /// Publishes at QoS 1, a burst at a time, each acked before the next.
    fn publish(
        &mut self,
        topics: impl Iterator<Item = String>,
        size: usize,
        retain: bool,
    ) -> io::Result<()> {
        let payload = vec![b'x'; size];
        let mut topics = topics.peekable();
        let mut out = Vec::new();
        while topics.peek().is_some() {
            let mut sent = 0;
            for topic in topics.by_ref().take(self.burst) {
                self.pkid = self.pkid.wrapping_add(1).max(1);
                let head = Head {
                    topic: &topic,
                    qos: 1,
                    pkid: self.pkid,
                    v5: self.v5,
                    retain,
                };
                wire::publish(&mut out, head, &payload);
                sent += 1;
            }
            self.publisher.write_all(&out)?;
            out.clear();
            let mut acked = 0;
            while acked < sent {
                if let Some((h, _)) = self.replies.next_packet() {
                    acked += usize::from(h == PUBACK);
                } else if self.replies.fill(&mut self.publisher)? == 0 {
                    return Err(ErrorKind::UnexpectedEof.into());
                }
            }
        }
        Ok(())
    }
}

fn topic(i: usize) -> String {
    format!("ram/{i}/cmd")
}

/// RSS of process `pid`, bytes.
///
/// # Errors
///
/// If `/proc/<pid>/status` cannot be read or lacks `VmRSS`.
pub fn rss(pid: u32) -> io::Result<u64> {
    status_bytes(pid, "VmRSS:")
}

/// The most resident memory of process `pid` so far, in bytes.
///
/// # Errors
///
/// If `/proc/<pid>/status` cannot be read or lacks `VmHWM`.
pub fn peak_rss(pid: u32) -> io::Result<u64> {
    status_bytes(pid, "VmHWM:")
}

fn status_bytes(pid: u32, key: &str) -> io::Result<u64> {
    std::fs::read_to_string(format!("/proc/{pid}/status"))?
        .lines()
        .find_map(|l| l.strip_prefix(key))
        .and_then(|v| v.trim().strip_suffix("kB"))
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(|kb| kb << 10)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, format!("no {key}")))
}

/// The broker's estimate, from `$SYS/broker/state/bytes`.
#[derive(Debug)]
struct Gauge {
    s: TcpStream,
    rd: Reader,
    v5: bool,
    last: Option<u64>,
}

impl Gauge {
    /// Subscribes, then waits up to `wait` for the first value.
    fn new(addr: &str, v5: bool, wait: Duration) -> io::Result<Gauge> {
        let (mut s, mut rd, _) = wire::connect(addr, "ram-sys", v5)?;
        wire::subscribe(&mut s, &mut rd, "$SYS/broker/state/bytes", 0, v5)?;
        let mut g = Gauge {
            s,
            rd,
            v5,
            last: None,
        };
        g.watch(wait, true)?;
        Ok(g)
    }

    /// Reads values for `wait`, keeping the last, or until the first one
    /// with `first`.
    fn watch(&mut self, wait: Duration, first: bool) -> io::Result<()> {
        let deadline = Instant::now() + wait;
        loop {
            while let Some((h, body)) = self.rd.next_packet() {
                if h >> 4 == 3 {
                    let value = wire::publish_payload(body, (h >> 1) & 3, self.v5);
                    let value = std::str::from_utf8(value).ok().and_then(|v| v.parse().ok());
                    self.last = value.or(self.last);
                }
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() || first && self.last.is_some() {
                return Ok(());
            }
            self.s.set_read_timeout(Some(left))?;
            match self.rd.fill(&mut self.s) {
                Ok(0) => return Err(ErrorKind::UnexpectedEof.into()),
                Ok(_) => {}
                Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
                Err(e) => return Err(e),
            }
        }
    }
}

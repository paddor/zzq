//! Generator threads and the connections they run.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, PoisonError, mpsc};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep, sleep_until};

use super::{CONNECT_LIMIT, PART, Plan, SLOT, Scenario, Shared, THREADS};
use crate::wire::{self, Head, PUBACK, Reader};

/// Handshakes in progress at once, per thread.
const HANDSHAKES: usize = 128;

/// QoS 1 publishes a publisher keeps in flight.
const IN_FLIGHT: usize = 1000;

/// Connect attempts per client, 200 ms apart.
const TRIES: usize = 6;

type Conn = (TcpStream, Reader);

/// A client of the run.
#[derive(Debug, Clone, Copy)]
enum Role {
    Sub(usize),
    Pub(usize),
}

impl Plan {
    /// Index of a client among all of them, for its source address.
    const fn index(&self, role: Role) -> usize {
        match role {
            Role::Sub(j) => j,
            Role::Pub(i) => self.scenario.subscribers(&self.cfg) + i,
        }
    }

    fn client_id(&self, role: Role) -> String {
        match role {
            Role::Sub(j) => format!("zl-{}-s{j}", self.pid),
            Role::Pub(i) => format!("zl-{}-p{i}", self.pid),
        }
    }

    fn topic(&self, i: usize) -> String {
        let pid = self.pid;
        match self.scenario {
            Scenario::P2p => format!("zb/{pid}/p/{i}"),
            Scenario::Fanin => format!("zb/{pid}/i/{i}"),
            Scenario::Fanout => format!("zb/{pid}/f/{i}"),
        }
    }

    fn filter(&self, j: usize) -> String {
        let pid = self.pid;
        match self.scenario {
            Scenario::P2p => self.topic(j),
            Scenario::Fanin => format!("$share/zb/zb/{pid}/i/+"),
            Scenario::Fanout => format!("zb/{pid}/f/+"),
        }
    }

    fn fail(&self, e: &io::Error, role: Role) {
        if self.failed.fetch_add(1, Relaxed) == 0 {
            let mut first = self.error.lock().unwrap_or_else(PoisonError::into_inner);
            *first = Some(format!("{}: {e}", self.client_id(role)));
        }
    }
}

/// Connects thread `t`'s share of the clients, says so on `done`, then
/// runs them with the state from `handoff` until the run stops.
pub(super) fn worker(
    t: usize,
    rt: &Runtime,
    plan: &Arc<Plan>,
    done: mpsc::Sender<()>,
    handoff: &mpsc::Receiver<Arc<Shared>>,
) {
    let share = rt.block_on(async {
        let all = tokio::time::timeout(CONNECT_LIMIT, connect_share(t, plan)).await;
        all.unwrap_or_else(|_| {
            plan.timed_out.store(true, Relaxed);
            (Vec::new(), Vec::new())
        })
    });
    let _ = done.send(());
    drop(done);
    let Ok(shared) = handoff.recv() else {
        return;
    };
    let (subs, pubs) = share;
    rt.block_on(async {
        for c in subs {
            tokio::spawn(subscriber(c, shared.clone()));
        }
        for (i, c) in pubs {
            tokio::spawn(publisher(c, i, shared.clone()));
        }
        while !shared.stop.load(Relaxed) {
            sleep(Duration::from_millis(50)).await;
        }
    });
}

/// Connects thread `t`'s subscribers, then its publishers, paced to its
/// part of the connect rate. Clients that failed are left out.
async fn connect_share(t: usize, plan: &Arc<Plan>) -> (Vec<Conn>, Vec<(usize, Conn)>) {
    let gate = Arc::new(Semaphore::new(HANDSHAKES));
    let every = Duration::from_secs_f64(THREADS as f64 / plan.cfg.connect_rate);
    let start = Instant::now();
    let mine = |n: usize| (t..n).step_by(THREADS);
    let subs = mine(plan.scenario.subscribers(&plan.cfg)).map(Role::Sub);
    let pubs = mine(plan.cfg.pubs).map(Role::Pub);
    let handles: Vec<_> = (0u32..)
        .zip(subs.chain(pubs))
        .map(|(nth, role)| {
            let (plan, gate) = (plan.clone(), gate.clone());
            let due = start + every * nth;
            let task = tokio::spawn(async move {
                sleep_until(due).await;
                let _permit = gate.acquire().await.ok()?;
                match open(&plan, role).await {
                    Ok(c) => {
                        plan.connected.fetch_add(1, Relaxed);
                        Some(c)
                    }
                    Err(e) => {
                        plan.fail(&e, role);
                        None
                    }
                }
            });
            (role, task)
        })
        .collect();
    let (mut subs, mut pubs) = (Vec::new(), Vec::new());
    for (role, task) in handles {
        match (role, task.await.ok().flatten()) {
            (Role::Sub(_), Some(c)) => subs.push(c),
            (Role::Pub(i), Some(c)) => pubs.push((i, c)),
            (_, None) => {}
        }
    }
    (subs, pubs)
}

/// Connects a client and subscribes it if it is a subscriber.
async fn open(plan: &Plan, role: Role) -> io::Result<Conn> {
    let cfg = &plan.cfg;
    let host = 1 + (plan.index(role) % usize::from(cfg.addrs)) as u8;
    let addr = SocketAddr::from(([127, 0, 0, host], cfg.port));
    let mut tries = 1;
    let mut s = loop {
        match TcpStream::connect(addr).await {
            Ok(s) => break s,
            Err(_) if tries < TRIES => {
                tries += 1;
                plan.retries.fetch_add(1, Relaxed);
                sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(e),
        }
    };
    s.set_nodelay(true)?;
    let mut out = Vec::new();
    wire::connect_packet(
        &mut out,
        &plan.client_id(role),
        cfg.v5,
        wire::KEEP_ALIVE,
        false,
    );
    s.write_all(&out).await?;
    let mut rd = Reader::with_capacity(4096);
    let body = handshake(&mut s, &mut rd, 0x20).await?;
    if body.get(1) != Some(&0) {
        return Err(io::Error::other(format!("CONNACK {body:x?}")));
    }
    if let Role::Sub(j) = role {
        out.clear();
        wire::subscribe_packet(&mut out, &plan.filter(j), cfg.qos, cfg.v5);
        s.write_all(&out).await?;
        let code = handshake(&mut s, &mut rd, 0x90).await?.last().copied();
        if code.is_none_or(|c| c >= 0x80) {
            return Err(io::Error::other(format!("SUBACK {code:x?}")));
        }
    }
    Ok((s, rd))
}

/// Reads the next packet, which must have fixed header byte `want`, and
/// returns its body.
async fn handshake(s: &mut TcpStream, rd: &mut Reader, want: u8) -> io::Result<Vec<u8>> {
    loop {
        if let Some((h, body)) = rd.next_packet() {
            if h != want {
                let e = format!("unexpected packet {h:#x} during the handshake");
                return Err(io::Error::other(e));
            }
            return Ok(body.to_vec());
        }
        let k = s.read(rd.space()).await?;
        if k == 0 {
            let e = "closed during the handshake";
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, e));
        }
        rd.filled(k);
    }
}

/// Reads deliveries: counts them, records latency, acks QoS 1.
async fn subscriber((mut s, mut rd): Conn, sh: Arc<Shared>) {
    let mut acks = Vec::new();
    let (v5, trace) = (sh.plan.cfg.v5, sh.plan.cfg.trace);
    loop {
        let k = s.read(rd.space()).await.unwrap_or(0);
        if k == 0 {
            sh.lost();
            return;
        }
        rd.filled(k);
        let now = sh.clock.now();
        let (lo, hi) = sh.window();
        let mut n = 0;
        while let Some((h, body)) = rd.next_packet() {
            if h >> 4 != 3 {
                continue;
            }
            n += 1;
            let qos = (h >> 1) & 3;
            if qos == 1 {
                wire::ack(&mut acks, PUBACK, &wire::publish_pkid(body));
            }
            let payload = wire::publish_payload(body, qos, v5);
            let Some(&sent) = payload.first_chunk::<8>() else {
                continue;
            };
            let sent = u64::from_le_bytes(sent);
            if (lo..hi).contains(&sent) {
                let took = now.saturating_sub(sent);
                if let Some(h) = sh.lat.parts.get(((sent - lo) / PART) as usize) {
                    h.record(took);
                }
                if trace && let Some(slot) = sh.lat.slots.get(((sent - lo) / SLOT) as usize) {
                    slot.fetch_max(took, Relaxed);
                }
            }
        }
        sh.recv.fetch_add(n, Relaxed);
        if !acks.is_empty() {
            if s.write_all(&acks).await.is_err() {
                sh.lost();
                return;
            }
            acks.clear();
        }
    }
}

/// A publisher's connection and send state.
struct Publisher {
    s: TcpStream,
    rd: Reader,
    topic: String,
    payload: Vec<u8>,
    out: Vec<u8>,
    pkid: u16,
    in_flight: usize,
    /// Until when the last write waited for the broker to read, in ns.
    blocked_until: u64,
}

/// Publishes on schedule until the last step ends or the connection
/// closes. A send that waits on the broker (for room in the window, or for
/// an earlier write) counts from its due time, any other from when it is
/// written.
async fn publisher((s, rd): Conn, i: usize, sh: Arc<Shared>) {
    let mut p = Publisher {
        s,
        rd,
        topic: sh.plan.topic(i),
        payload: vec![b'x'; sh.plan.cfg.size],
        out: Vec::new(),
        pkid: 0,
        in_flight: 0,
        blocked_until: 0,
    };
    // A fixed phase per publisher, spread over the period.
    let mut due = Due {
        phase: (i as f64 * 0.618_033_988_75).fract(),
        k: 0,
        n: 0,
    };
    while !sh.stop.load(Relaxed)
        && let Some(at) = due.next(&sh)
    {
        let Some(held) = p.wait(&sh, at).await else {
            sh.lost();
            return;
        };
        if p.send(&sh, at, held).await.is_err() {
            sh.lost();
            return;
        }
    }
}

/// A publisher's place in the schedule: send `n` of step `k`.
struct Due {
    phase: f64,
    k: usize,
    n: u64,
}

impl Due {
    /// When the next send is due, in ns, or `None` after the last step.
    fn next(&mut self, sh: &Shared) -> Option<u64> {
        let steps = &sh.clock.steps;
        loop {
            let &(start, period) = steps.get(self.k)?;
            let due = start + (self.phase * period as f64) as u64 + self.n * period;
            let next_step = steps.get(self.k + 1).map_or(sh.clock.end, |s| s.0);
            if due < next_step {
                self.n += 1;
                return Some(due);
            }
            self.k += 1;
            self.n = 0;
        }
    }
}

impl Publisher {
    /// Reads acks until `due` and until the window has room. Returns
    /// whether the send waited for room, or `None` if the connection
    /// closed. Both arms are cancel-safe: a read that loses the race has
    /// read nothing, and the timer is made again.
    async fn wait(&mut self, sh: &Shared, due: u64) -> Option<bool> {
        let at = Instant::from_std(sh.clock.at(due));
        let (mut fired, mut held) = (false, false);
        loop {
            if fired && self.in_flight < IN_FLIGHT {
                return Some(held);
            }
            tokio::select! {
                () = sleep_until(at), if !fired => {
                    fired = true;
                    held = self.in_flight >= IN_FLIGHT;
                }
                r = self.s.read(self.rd.space()) => {
                    let k = r.unwrap_or(0);
                    if k == 0 {
                        return None;
                    }
                    self.rd.filled(k);
                    while let Some((h, _)) = self.rd.next_packet() {
                        if h >> 4 == 4 {
                            self.in_flight = self.in_flight.saturating_sub(1);
                        }
                    }
                }
            }
        }
    }

    /// Writes the PUBLISH due at `due`.
    async fn send(&mut self, sh: &Shared, due: u64, held: bool) -> io::Result<()> {
        let cfg = &sh.plan.cfg;
        let sent_at = if held || due < self.blocked_until {
            due
        } else {
            sh.clock.now()
        };
        self.pkid = self.pkid % u16::MAX + 1;
        if let Some(stamp) = self.payload.first_chunk_mut::<8>() {
            *stamp = sent_at.to_le_bytes();
        }
        self.out.clear();
        let fields = Head {
            topic: &self.topic,
            qos: cfg.qos,
            pkid: self.pkid,
            v5: cfg.v5,
            retain: false,
        };
        wire::publish(&mut self.out, fields, &self.payload);
        let wrote = match self.s.try_write(&self.out) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
            Err(e) => return Err(e),
        };
        if let Some(rest) = self.out.get(wrote..).filter(|r| !r.is_empty()) {
            self.s.write_all(rest).await?;
            self.blocked_until = sh.clock.now();
        }
        if cfg.qos > 0 {
            self.in_flight += 1;
        }
        sh.sent.fetch_add(1, Relaxed);
        let (lo, hi) = sh.window();
        if cfg.trace && (lo..hi).contains(&due) {
            let late = sh.clock.now().saturating_sub(due);
            sh.lat.late.record(late);
            if let Some(slot) = sh.lat.late_slots.get(((due - lo) / SLOT) as usize) {
                slot.fetch_max(late, Relaxed);
            }
        }
        Ok(())
    }
}

//! Failover under load: QoS 1 publishers and persistent subscribers on a
//! cluster of three, each client on whichever node takes it, node 0
//! first. At `at` seconds it runs the `stop` command of the node the
//! publishers use, and reports how long the publishers waited, and what
//! of what was acked the subscribers lack, got twice or out of order.
//!
//! Usage: `zzq_failover uris=URI,URI,URI stop=CMD|CMD|CMD [rate=1000]
//! [pubs=4] [subs=4] [secs=20] [at=8] [offline=0] [v=5]`
//!
//! With `offline=1` the subscribers go away 2 s before the stop and come
//! back 2 s after it, so what is published meanwhile waits in their
//! sessions. `v=4` speaks MQTT 3.1.1, whose sessions do not expire.
//!
//! Prints `failover rate=… offline=… pubs=… subs=… acked=… lost=… dup=… reordered=…
//! down_ms=… stall_ms=… resends=…`: `down_ms` the longest time without
//! any PUBACK, `stall_ms` the longest a message waited for its PUBACK,
//! across reconnects, and `resends` the publishes sent again after a lost
//! connection.

use std::collections::{HashMap, HashSet};
use std::process::{Command, ExitCode};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use zzq_client::{Client, ClientOptions, Context, QoS, Version};

/// How long a publish may take before its connection counts as lost.
const ACK_WAIT: Duration = Duration::from_secs(5);
/// How long the subscribers get for what was acked after the run.
const DRAIN: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct Args {
    uris: Vec<String>,
    stop: Vec<String>,
    rate: u64,
    pubs: usize,
    subs: usize,
    secs: u64,
    at: u64,
    offline: bool,
    version: Version,
}

fn parse(args: &[String]) -> Option<Args> {
    let mut a = Args {
        uris: Vec::new(),
        stop: Vec::new(),
        rate: 1000,
        pubs: 4,
        subs: 4,
        secs: 20,
        at: 8,
        offline: false,
        version: Version::V5,
    };
    for arg in args {
        let (k, v) = arg.split_once('=')?;
        match k {
            "uris" => a.uris = v.split(',').map(str::to_owned).collect(),
            "stop" => a.stop = v.split('|').map(str::to_owned).collect(),
            "rate" => a.rate = v.parse().ok()?,
            "pubs" => a.pubs = v.parse().ok()?,
            "subs" => a.subs = v.parse().ok()?,
            "secs" => a.secs = v.parse().ok()?,
            "at" => a.at = v.parse().ok()?,
            "offline" => a.offline = v == "1",
            "v" => {
                a.version = match v {
                    "4" => Version::V311,
                    "5" => Version::V5,
                    _ => return None,
                }
            }
            _ => return None,
        }
    }
    let ok = !a.uris.is_empty()
        && a.stop.len() == a.uris.len()
        && a.pubs > 0
        && a.rate > 0
        && (!a.offline || a.at >= 2);
    ok.then_some(a)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// What the clients saw.
#[derive(Debug, Default)]
struct Seen {
    /// Messages a PUBACK came for: publisher, number.
    acked: Vec<(usize, u64)>,
    /// Each subscriber's deliveries in order.
    delivered: Vec<Vec<(usize, u64)>>,
    stall: Duration,
    resends: u64,
    /// The last PUBACK, and the longest time between two.
    last: Option<Instant>,
    down: Duration,
}

struct Run {
    ctx: Context,
    args: Args,
    seen: Mutex<Seen>,
    /// The node each publisher is on.
    nodes: Vec<AtomicUsize>,
    /// The subscribers are away.
    away: tokio::sync::watch::Sender<bool>,
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(args) = parse(&args) else {
        eprintln!(
            "usage: zzq_failover uris=URI,URI,URI stop=CMD|CMD|CMD [rate=1000] [pubs=4] [subs=4] [secs=20] [at=8] [offline=0] [v=5]"
        );
        return ExitCode::FAILURE;
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build();
    let result = rt
        .map_err(|e| e.to_string())
        .and_then(|rt| rt.block_on(run(args)));
    match result {
        Ok(line) => {
            println!("{line}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<String, String> {
    let ctx = Context::new().map_err(|e| e.to_string())?;
    let run = Arc::new(Run {
        nodes: (0..args.pubs).map(|_| AtomicUsize::new(0)).collect(),
        seen: Mutex::new(Seen {
            delivered: vec![Vec::new(); args.subs],
            ..Seen::default()
        }),
        ctx,
        args,
        away: tokio::sync::watch::Sender::new(false),
    });
    // Under MQTT 3.1.1 a clean session ends with its connection, so the
    // subscriptions go to a kept one; each run's cluster is new.
    let clean = run.args.version == Version::V5;
    for s in 0..run.args.subs {
        let (_, c) = run.connect(&format!("fo-sub{s}"), clean).await;
        c.subscribe("fo/#", QoS::AtLeastOnce)
            .await
            .map_err(|e| format!("subscribe: {e}"))?;
        c.disconnect().await;
        tokio::spawn(run.clone().subscribe(s));
    }
    let pubs: Vec<_> = (0..run.args.pubs)
        .map(|p| tokio::spawn(run.clone().publish(p)))
        .collect();
    if run.args.offline {
        tokio::time::sleep(Duration::from_secs(run.args.at - 2)).await;
        run.away.send_replace(true);
        tokio::time::sleep(Duration::from_secs(2)).await;
        run.stop().await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        run.away.send_replace(false);
    } else {
        tokio::time::sleep(Duration::from_secs(run.args.at)).await;
        run.stop().await?;
    }
    for p in pubs {
        p.await.map_err(|e| e.to_string())?;
    }
    {
        // Publishers that never got PUBACKs again were down to the end.
        let mut seen = lock(&run.seen);
        if let Some(last) = seen.last {
            seen.down = seen.down.max(last.elapsed());
        }
    }
    let end = Instant::now() + DRAIN;
    while Instant::now() < end && run.missing() > 0 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(run.report())
}

impl Run {
    /// A client of the first node from `from` on that takes it, if one
    /// does before `until`.
    async fn connect_until(
        &self,
        id: &str,
        clean: bool,
        from: usize,
        until: Option<Instant>,
    ) -> Option<(usize, Client)> {
        let n = self.args.uris.len();
        let mut i = from;
        loop {
            let opts = ClientOptions {
                version: self.args.version,
                clean_start: clean,
                session_expiry: 600,
                connect_timeout: Duration::from_secs(1),
                ..ClientOptions::new(id)
            };
            if let Ok(c) = Client::connect(&self.ctx, &self.args.uris[i % n], opts).await {
                return Some((i % n, c));
            }
            if until.is_some_and(|u| Instant::now() >= u) {
                return None;
            }
            i += 1;
            if i % n == from % n {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    /// A client of the first node from `from` on that takes it.
    async fn connect_from(&self, id: &str, clean: bool, from: usize) -> (usize, Client) {
        loop {
            if let Some(c) = self.connect_until(id, clean, from, None).await {
                return c;
            }
        }
    }

    async fn connect(&self, id: &str, clean: bool) -> (usize, Client) {
        self.connect_from(id, clean, 0).await
    }

    /// Publishes at its share of the rate until the run ends, each message
    /// again over a new connection until a PUBACK comes for it.
    async fn publish(self: Arc<Self>, p: usize) {
        let id = format!("fo-pub{p}");
        let every = Duration::from_secs_f64(self.args.pubs as f64 / self.args.rate as f64);
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let end = Instant::now() + Duration::from_secs(self.args.secs);
        let (mut node, mut client) = (0, None);
        let topic = format!("fo/{p}");
        for n in 0.. {
            tick.tick().await;
            if Instant::now() >= end {
                break;
            }
            let payload = Bytes::from(format!("p{p}-{n}"));
            let first = Instant::now();
            // Again until a PUBACK comes, or the run ends without one.
            while Instant::now() < end {
                let c = if let Some(c) = client.take() {
                    c
                } else {
                    let Some((i, c)) = self.connect_until(&id, true, node, Some(end)).await else {
                        break;
                    };
                    node = i;
                    self.nodes[p].store(i, Relaxed);
                    c
                };
                let sent = c.publish(&topic, payload.clone(), QoS::AtLeastOnce);
                if let Ok(Ok(())) = tokio::time::timeout(ACK_WAIT, sent).await {
                    self.acked(p, n, first);
                    client = Some(c);
                    break;
                }
                lock(&self.seen).resends += 1;
            }
            if Instant::now() >= end {
                // A message that never got its PUBACK waited all along.
                let mut seen = lock(&self.seen);
                seen.stall = seen.stall.max(end - first);
                break;
            }
        }
    }

    fn acked(&self, p: usize, n: u64, first: Instant) {
        let now = Instant::now();
        let mut seen = lock(&self.seen);
        seen.acked.push((p, n));
        seen.stall = seen.stall.max(now - first);
        if let Some(last) = seen.last {
            seen.down = seen.down.max(now - last);
        }
        seen.last = Some(now);
    }

    /// Takes every message of subscriber `s`'s session, again after each
    /// lost connection.
    async fn subscribe(self: Arc<Self>, s: usize) {
        let id = format!("fo-sub{s}");
        let mut away = self.away.subscribe();
        let mut node = 0;
        loop {
            if away.wait_for(|&a| !a).await.is_err() {
                return;
            }
            let (i, mut c) = self.connect_from(&id, false, node).await;
            node = i;
            let gone = loop {
                tokio::select! {
                    m = c.recv() => match m {
                        Some(m) => {
                            if let Some(msg) = parse_payload(&m.payload) {
                                lock(&self.seen).delivered[s].push(msg);
                            }
                        }
                        None => break false,
                    },
                    // Gone until the stop is over: the session keeps what comes.
                    gone = away.wait_for(|&a| a) => break gone.is_ok(),
                }
            };
            if gone {
                c.disconnect().await;
            }
        }
    }

    /// Stops the node most publishers are on.
    async fn stop(&self) -> Result<(), String> {
        let mut count = vec![0; self.args.uris.len()];
        for n in &self.nodes {
            count[n.load(Relaxed)] += 1;
        }
        let node = (0..count.len()).max_by_key(|&i| count[i]).unwrap_or(0);
        let cmd = self.args.stop[node].clone();
        eprintln!("stopping node {node}: {cmd}");
        let status =
            tokio::task::spawn_blocking(move || Command::new("sh").arg("-c").arg(&cmd).status())
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("stop command failed: {status}"))
        }
    }

    /// Acked messages some subscriber lacks.
    fn missing(&self) -> usize {
        let seen = lock(&self.seen);
        let got: Vec<HashSet<(usize, u64)>> = seen
            .delivered
            .iter()
            .map(|d| d.iter().copied().collect())
            .collect();
        seen.acked
            .iter()
            .filter(|m| got.iter().any(|g| !g.contains(m)))
            .count()
    }

    fn report(&self) -> String {
        let lost = self.missing();
        let seen = lock(&self.seen);
        let (dup, reordered) = duplicates(&seen.delivered);
        let down = seen.down.as_millis();
        let a = &self.args;
        format!(
            "failover rate={} offline={} pubs={} subs={} acked={} lost={lost} dup={dup} reordered={reordered} down_ms={down} stall_ms={} resends={}",
            a.rate,
            u8::from(a.offline),
            a.pubs,
            a.subs,
            seen.acked.len(),
            seen.stall.as_millis(),
            seen.resends,
        )
    }
}

/// `p3-17` as publisher 3, message 17.
fn parse_payload(b: &[u8]) -> Option<(usize, u64)> {
    let s = std::str::from_utf8(b).ok()?.strip_prefix('p')?;
    let (p, n) = s.split_once('-')?;
    Some((p.parse().ok()?, n.parse().ok()?))
}

/// Deliveries beyond the first of each message, and first deliveries
/// that came after a later message of the same publisher, over all
/// subscribers.
fn duplicates(delivered: &[Vec<(usize, u64)>]) -> (u64, u64) {
    let (mut dup, mut reordered) = (0, 0);
    for d in delivered {
        let mut firsts: HashSet<(usize, u64)> = HashSet::new();
        let mut last: HashMap<usize, u64> = HashMap::new();
        for &(p, n) in d {
            if !firsts.insert((p, n)) {
                dup += 1;
                continue;
            }
            if last.get(&p).is_some_and(|&l| l > n) {
                reordered += 1;
            }
            let l = last.entry(p).or_insert(n);
            *l = (*l).max(n);
        }
    }
    (dup, reordered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deliveries_count_duplicates_and_late_firsts() {
        let d = vec![
            vec![(0, 0), (0, 2), (0, 1), (0, 2), (1, 0)],
            vec![(0, 0), (0, 0)],
        ];
        assert_eq!(duplicates(&d), (2, 1));
        assert_eq!(parse_payload(b"p3-17"), Some((3, 17)));
        assert_eq!(parse_payload(b"x"), None);
    }

    #[test]
    fn arguments_need_a_stop_command_per_node() {
        let words = |s: &str| s.split(' ').map(str::to_owned).collect::<Vec<_>>();
        let a = parse(&words("uris=a,b,c stop=x|y|z rate=10 at=2 offline=1")).unwrap();
        assert_eq!((a.rate, a.at, a.stop.len(), a.offline), (10, 2, 3, true));
        assert_eq!(a.version, Version::V5);
        assert_eq!(
            parse(&words("uris=a stop=x v=4")).unwrap().version,
            Version::V311
        );
        assert!(parse(&words("uris=a stop=x v=3")).is_none());
        assert!(parse(&words("uris=a stop=x at=1 offline=1")).is_none());
        assert!(parse(&words("uris=a,b stop=x")).is_none());
        assert!(parse(&words("uris=a stop=x nope=1")).is_none());
    }
}

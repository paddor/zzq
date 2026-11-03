//! A cluster of three broker processes: what it acked survives losing a
//! node. A failing test prints its steps and the nodes' stderr.

#![cfg(all(unix, feature = "cluster"))]
#![expect(clippy::unwrap_used, clippy::panic, reason = "tests fail loudly")]

use std::collections::BTreeSet;
use std::fmt::Display;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tokio::time::timeout;
use zzq_broker::Context;
use zzq_client::{Client, ClientOptions, Handle, QoS};

const EXE: &str = env!("CARGO_BIN_EXE_zzq_broker");
const NAMES: [&str; 3] = ["a", "b", "c"];
/// How long a test waits for a leader, a line or a message.
const PATIENCE: Duration = Duration::from_secs(20);
/// How long a slow subscriber takes per message.
const SLOW: Duration = Duration::from_millis(1);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The test's steps and the nodes' stderr, timed from the start.
#[derive(Debug, Clone)]
struct Log {
    start: Instant,
    lines: Arc<Mutex<Vec<String>>>,
}

impl Log {
    fn say(&self, what: impl Display) {
        let t = self.start.elapsed().as_secs_f64();
        lock(&self.lines).push(format!("{t:8.3} {what}"));
    }

    /// Lines so far: [`Log::wait_for`] looks after them.
    fn len(&self) -> usize {
        lock(&self.lines).len()
    }

    fn has(&self, from: usize, text: &str) -> bool {
        lock(&self.lines)[from..].iter().any(|l| l.contains(text))
    }

    /// Waits for a line after the first `from` that has `text`.
    async fn wait_for(&self, from: usize, text: &str) {
        let end = Instant::now() + PATIENCE;
        while !self.has(from, text) {
            assert!(Instant::now() < end, "no {text:?} within {PATIENCE:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// A free TCP port on the loopback address, below the ephemeral range:
/// a port-0 bind or a client socket elsewhere could take one in it before
/// the node binds it. Never the same twice in a process, and each process
/// counts from its own hundred, so two at once seldom meet.
fn free_port() -> u16 {
    use std::sync::atomic::AtomicU16;

    static NEXT: AtomicU16 = AtomicU16::new(0);
    let start = (std::process::id() % 220) as u16 * 100;
    loop {
        // Relaxed: only the count matters.
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let port = 10_000 + (start + n) % 22_000;
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

/// Three nodes of a new cluster on the loopback address. Dropped, it
/// kills them, and prints the log if the test failed.
#[derive(Debug)]
struct Cluster {
    peers: String,
    /// The MQTT endpoint each node binds while it leads.
    uris: Vec<String>,
    nodes: Vec<Option<Child>>,
    log: Log,
}

impl Cluster {
    fn start() -> Cluster {
        Cluster::binding(NAMES.map(|_| format!("tcp://127.0.0.1:{}", free_port())))
    }

    /// Three nodes that bind `uris` while they lead.
    fn binding(uris: [String; 3]) -> Cluster {
        let peers = NAMES
            .map(|n| format!("{n}=tcp://127.0.0.1:{}", free_port()))
            .join(",");
        let log = Log {
            start: Instant::now(),
            lines: Arc::default(),
        };
        let mut c = Cluster {
            peers,
            uris: Vec::from(uris),
            nodes: Vec::new(),
            log,
        };
        c.nodes = (0..3).map(|i| Some(c.spawn(i, true))).collect();
        c
    }

    /// Node `i`, of a new cluster on `bootstrap`, else recovering its
    /// state from the others.
    fn spawn(&self, i: usize, bootstrap: bool) -> Child {
        let mut cmd = Command::new(EXE);
        cmd.args(["--bind", &self.uris[i], "--cluster-node", NAMES[i]])
            .args(["--cluster-peers", &self.peers]);
        if bootstrap {
            cmd.arg("--cluster-bootstrap");
        }
        let mut child = cmd
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stderr = BufReader::new(child.stderr.take().unwrap());
        let log = self.log.clone();
        let name = NAMES[i];
        std::thread::spawn(move || {
            for line in stderr.lines().map_while(Result::ok) {
                log.say(format_args!("{name}: {line}"));
            }
        });
        self.log.say(format_args!("{name} started"));
        child
    }

    fn kill(&mut self, i: usize) {
        let mut child = self.nodes[i].take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        self.log.say(format_args!("{} killed", NAMES[i]));
    }

    /// Starts killed node `i` again: it recovers its state from the
    /// others.
    fn restart(&mut self, i: usize) {
        self.nodes[i] = Some(self.spawn(i, false));
    }

    /// Sends node `i` a signal, such as `-STOP`.
    fn signal(&self, i: usize, signal: &str) {
        let pid = self.nodes[i].as_ref().unwrap().id().to_string();
        let status = Command::new("kill").args([signal, &pid]).status();
        assert!(status.unwrap().success());
        self.log.say(format_args!("{} {signal}", NAMES[i]));
    }

    fn finder(&self) -> Finder {
        Finder {
            uris: self.uris.clone().into(),
            log: self.log.clone(),
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in self.nodes.iter_mut().flatten() {
            let _ = child.kill();
            let _ = child.wait();
        }
        if std::thread::panicking() {
            eprintln!("{}", lock(&self.log.lines).join("\n"));
        }
    }
}

/// Connects clients to whichever node leads.
#[derive(Debug, Clone)]
struct Finder {
    uris: Arc<[String]>,
    log: Log,
}

impl Finder {
    /// A client of the first node that takes it, other than `skip`, and
    /// the node's index. Logs each node's error as it changes.
    async fn connect(
        &self,
        ctx: &Context,
        opts: &ClientOptions,
        skip: Option<usize>,
    ) -> (usize, Client) {
        let end = Instant::now() + PATIENCE;
        let id = &opts.client_id;
        let mut failed = [const { String::new() }; 3];
        loop {
            for (i, uri) in self.uris.iter().enumerate() {
                if Some(i) == skip {
                    continue;
                }
                match Client::connect(ctx, uri, opts.clone()).await {
                    Ok(c) => {
                        let present = c.connected().session_present;
                        let name = NAMES[i];
                        self.log
                            .say(format_args!("{id} connected to {name}, session {present}"));
                        return (i, c);
                    }
                    Err(e) => {
                        let e = e.to_string();
                        if failed[i] != e {
                            self.log.say(format_args!("{id} to {}: {e}", NAMES[i]));
                            failed[i] = e;
                        }
                    }
                }
            }
            assert!(
                Instant::now() < end,
                "{} found no leader within {PATIENCE:?}",
                opts.client_id
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// A client that gives up on a node that does not answer within a
/// second, such as a stopped one.
fn options(id: &str) -> ClientOptions {
    ClientOptions {
        connect_timeout: Duration::from_secs(1),
        ..ClientOptions::new(id)
    }
}

fn durable(id: &str) -> ClientOptions {
    ClientOptions {
        clean_start: false,
        session_expiry: 600,
        ..options(id)
    }
}

/// Publishes `{topic}/0` to `{topic}/{n - 1}` to `topic` as QoS 1, each
/// acked, and returns them.
async fn batch(c: &Client, topic: &str, n: usize) -> BTreeSet<String> {
    let mut acked = BTreeSet::new();
    for i in 0..n {
        let payload = format!("{topic}/{i}");
        c.publish(topic, &payload, QoS::AtLeastOnce).await.unwrap();
        acked.insert(payload);
    }
    acked
}

/// Publishes `{topic}/0`, `{topic}/1` and so on to `topic` as QoS 1 until
/// the connection fails, and adds each acked one to `acked`.
async fn stream(publisher: Handle, topic: String, acked: Arc<Mutex<BTreeSet<String>>>) {
    for i in 0_u64.. {
        let payload = format!("{topic}/{i}");
        if publisher
            .publish(&topic, &payload, QoS::AtLeastOnce)
            .await
            .is_err()
        {
            return;
        }
        lock(&acked).insert(payload);
    }
}

/// A durable subscriber that receives in a task, across reconnects.
#[derive(Debug)]
struct Subscriber {
    got: Arc<Mutex<BTreeSet<String>>>,
    task: JoinHandle<()>,
}

impl Subscriber {
    /// Receives as `opts` on `sub`, then from whichever node leads each
    /// time the connection closes. Takes a message per `pace`.
    fn spawn(
        finder: &Finder,
        ctx: &Context,
        opts: ClientOptions,
        mut sub: Option<Client>,
        pace: Duration,
    ) -> Self {
        let got = Arc::new(Mutex::new(BTreeSet::new()));
        let (finder, ctx, into) = (finder.clone(), ctx.clone(), got.clone());
        let task = tokio::spawn(async move {
            loop {
                if sub.is_none() {
                    sub = Some(finder.connect(&ctx, &opts, None).await.1);
                }
                let Some(m) = sub.as_mut().unwrap().recv().await else {
                    let n = lock(&into).len();
                    finder
                        .log
                        .say(format_args!("{} closed, {n} got", opts.client_id));
                    sub = None;
                    continue;
                };
                lock(&into).insert(String::from_utf8_lossy(&m.payload).into_owned());
                tokio::time::sleep(pace).await;
            }
        });
        Subscriber { got, task }
    }

    /// Waits until it got every one of `want`.
    async fn wait_all(&self, want: &BTreeSet<String>) {
        let end = Instant::now() + PATIENCE;
        loop {
            let missing: Vec<String> = want.difference(&lock(&self.got)).cloned().collect();
            if missing.is_empty() {
                return;
            }
            assert!(
                Instant::now() < end,
                "{} of {} acked messages did not come: {missing:?}",
                missing.len(),
                want.len()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Threads that spin on every CPU, twice over, while it lives.
#[derive(Debug)]
struct Load {
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl Load {
    fn spin() -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let n = std::thread::available_parallelism().map_or(4, usize::from);
        let threads = (0..2 * n)
            .map(|_| {
                let stop = stop.clone();
                // Relaxed: the flag only ends the loop.
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                })
            })
            .collect();
        Load { stop, threads }
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cluster_of_three_processes_keeps_acked_messages_when_the_leader_dies() {
    leader_dies().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spins on every CPU"]
async fn a_cluster_under_cpu_load_keeps_acked_messages_when_the_leader_dies() {
    let _load = Load::spin();
    leader_dies().await;
}

/// The new leader has the offline subscriber's session, with every
/// message acked to the publisher, in order, on one connection.
async fn leader_dies() {
    let mut c = Cluster::start();
    let finder = c.finder();
    let ctx = Context::new().unwrap();
    let (_, sub) = finder.connect(&ctx, &durable("c-sub"), None).await;
    sub.subscribe("q/#", QoS::AtLeastOnce).await.unwrap();
    sub.disconnect().await;
    let (leader, publisher) = finder.connect(&ctx, &options("c-pub"), None).await;
    for i in 0..20 {
        publisher
            .publish("q/1", i.to_string(), QoS::AtLeastOnce)
            .await
            .unwrap();
    }
    c.kill(leader);
    drop(publisher);
    let (_, mut sub) = finder.connect(&ctx, &durable("c-sub"), Some(leader)).await;
    assert!(sub.connected().session_present, "the session is gone");
    for i in 0..20 {
        match timeout(Duration::from_secs(5), sub.recv()).await {
            Ok(Some(m)) => assert_eq!(m.payload, i.to_string()),
            Ok(None) => panic!("the connection closed after {i} of 20 messages"),
            Err(e) => panic!("message {i} of 20: {e} after 5 s"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_offline_subscriber_gets_every_message_acked_before_the_leader_died() {
    killed_mid_stream(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_online_subscriber_gets_every_message_acked_before_the_leader_died() {
    killed_mid_stream(true).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "spins on every CPU"]
async fn an_online_subscriber_under_cpu_load_gets_every_acked_message() {
    let _load = Load::spin();
    killed_mid_stream(true).await;
}

/// Four publishers stream QoS 1 messages, and the leader dies under them.
/// A subscriber gets every acked one: `online` all along, taking fewer
/// than come, or connecting after.
async fn killed_mid_stream(online: bool) {
    let mut c = Cluster::start();
    let finder = c.finder();
    let ctx = Context::new().unwrap();
    let opts = durable("s-sub");
    let (_, sub) = finder.connect(&ctx, &opts, None).await;
    sub.subscribe("s/#", QoS::AtLeastOnce).await.unwrap();
    let mut receiving = None;
    if online {
        receiving = Some(Subscriber::spawn(
            &finder,
            &ctx,
            opts.clone(),
            Some(sub),
            SLOW,
        ));
    } else {
        sub.disconnect().await;
    }
    let (leader, publisher) = finder.connect(&ctx, &options("s-pub"), None).await;
    let acked = Arc::new(Mutex::new(BTreeSet::new()));
    let streams: Vec<_> = (0..4)
        .map(|t| tokio::spawn(stream(publisher.handle(), format!("s/{t}"), acked.clone())))
        .collect();
    // A leader that lost its lease on its own ends the streams early. The
    // node that led them dies all the same.
    let end = Instant::now() + PATIENCE;
    while lock(&acked).len() < 2000 && !streams.iter().all(JoinHandle::is_finished) {
        assert!(Instant::now() < end, "2000 acks did not come");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    c.kill(leader);
    for s in streams {
        timeout(PATIENCE, s).await.unwrap().unwrap();
    }
    let want = lock(&acked).clone();
    c.log.say(format_args!("{} acked", want.len()));
    let sub =
        receiving.unwrap_or_else(|| Subscriber::spawn(&finder, &ctx, opts, None, Duration::ZERO));
    sub.wait_all(&want).await;
}

/// A leader whose client endpoint another socket holds says so.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_that_cannot_bind_its_endpoint_says_so() {
    let held: Vec<std::net::TcpListener> = (0..3)
        .map(|_| std::net::TcpListener::bind(("127.0.0.1", free_port())).unwrap())
        .collect();
    let uris = [0, 1, 2].map(|i| format!("tcp://{}", held[i].local_addr().unwrap()));
    let c = Cluster::binding(uris);
    c.log
        .wait_for(0, "leads but could not bind tcp://127.0.0.1:")
        .await;
}

/// The leader dies, the next takes more messages, the first comes back
/// without its state, and the next dies too.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_node_keeps_acked_messages_when_the_next_leader_dies() {
    let mut c = Cluster::start();
    let finder = c.finder();
    let ctx = Context::new().unwrap();
    let opts = durable("r-sub");
    let (_, sub) = finder.connect(&ctx, &opts, None).await;
    sub.subscribe("r/#", QoS::AtLeastOnce).await.unwrap();
    sub.disconnect().await;
    let (first, publisher) = finder.connect(&ctx, &options("r-pub"), None).await;
    let mut want = batch(&publisher, "r/1", 20).await;
    c.kill(first);
    let (next, publisher) = finder.connect(&ctx, &options("r-pub"), Some(first)).await;
    want.extend(batch(&publisher, "r/2", 20).await);
    let mark = c.log.len();
    c.restart(first);
    let follows = format!("cluster node {} follows", NAMES[first]);
    c.log.wait_for(mark, &follows).await;
    c.kill(next);
    drop(publisher);
    let sub = Subscriber::spawn(&finder, &ctx, opts, None, Duration::ZERO);
    sub.wait_all(&want).await;
}

/// The leader stops while a publish waits on it, the others elect
/// another, and it goes on. A publish it acked then is kept too.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_leader_that_goes_on_loses_no_acked_message() {
    let c = Cluster::start();
    let finder = c.finder();
    let ctx = Context::new().unwrap();
    let opts = durable("p-sub");
    let (_, sub) = finder.connect(&ctx, &opts, None).await;
    sub.subscribe("p/#", QoS::AtLeastOnce).await.unwrap();
    sub.disconnect().await;
    let (first, publisher) = finder.connect(&ctx, &options("p-pub"), None).await;
    let mut want = batch(&publisher, "p/1", 20).await;
    c.signal(first, "-STOP");
    let stale = tokio::spawn(async move {
        let sent = publisher.publish("p/0", "p/stale", QoS::AtLeastOnce);
        timeout(PATIENCE, sent).await.is_ok_and(|r| r.is_ok())
    });
    let (_, next) = finder.connect(&ctx, &options("p-next"), Some(first)).await;
    want.extend(batch(&next, "p/2", 20).await);
    c.signal(first, "-CONT");
    if stale.await.unwrap() {
        c.log.say("the stopped leader acked p/stale");
        want.insert("p/stale".to_owned());
    }
    let sub = Subscriber::spawn(&finder, &ctx, opts, None, Duration::ZERO);
    sub.wait_all(&want).await;
}

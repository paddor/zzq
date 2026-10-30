use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use tokio::net::TcpStream;
use tokio::task::JoinSet;
use zzq_client::{Client, ClientOptions, Context, QoS, Reconnect};

use super::process::{Node, address};
use super::proxy::Proxy;
use super::support::{Run, WAIT, context, next, payload, within};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    Disk,
    Crash,
    Isolation,
    Restart,
}

struct Cluster {
    nodes: Vec<Node>,
    edges: Vec<(usize, usize, Proxy)>,
    front: Proxy,
}

impl Cluster {
    async fn new(run: &Run, kind: Kind) -> Self {
        let count = if kind == Kind::Disk { 1 } else { 3 };
        let mqtt: Vec<_> = (0..count).map(|_| address()).collect();
        let peer: Vec<_> = (0..count).map(|_| address()).collect();
        let mut edges = Vec::new();
        for i in 0..count {
            for (j, addr) in peer.iter().enumerate().take(i) {
                edges.push((i, j, Proxy::new(vec![*addr]).await));
            }
        }
        let front = Proxy::new(mqtt.clone()).await;
        let mut nodes = Vec::new();
        for (i, addr) in mqtt.into_iter().enumerate() {
            let args = if count == 1 {
                vec![
                    "--data-dir".into(),
                    run.dir.join("data").display().to_string(),
                ]
            } else {
                let peers = peer
                    .iter()
                    .enumerate()
                    .map(|(j, actual)| {
                        let target = edges
                            .iter()
                            .find(|(a, b, _)| *a == i && *b == j)
                            .map_or(*actual, |(_, _, p)| p.addr);
                        format!("n{j}=tcp://{target}")
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                vec![
                    "--cluster-node".into(),
                    format!("n{i}"),
                    "--cluster-peers".into(),
                    peers,
                    "--state-file".into(),
                    run.dir.join(format!("n{i}.state")).display().to_string(),
                ]
            };
            let mut node = Node::new(addr, args, &run.dir, i, run.monitor.pids.clone());
            node.start(count > 1);
            nodes.push(node);
        }
        let cluster = Self {
            nodes,
            edges,
            front,
        };
        cluster.leader().await;
        if count > 1 {
            for node in &cluster.nodes {
                node.settled().await;
            }
        }
        cluster
    }

    async fn leader(&self) -> usize {
        within(async {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            loop {
                for (i, n) in self.nodes.iter().enumerate() {
                    if TcpStream::connect(n.addr).await.is_ok() {
                        return i;
                    }
                }
                tick.tick().await;
            }
        })
        .await
    }

    fn isolate(&self, node: usize, isolated: bool) {
        for (a, b, proxy) in &self.edges {
            if *a == node || *b == node {
                proxy.enable(!isolated);
            }
        }
    }

    async fn fault(&mut self, ctx: &Context, run: &Run, kind: Kind, cycle: u64) {
        let leader = self.leader().await;
        eprintln!(
            "fault cycle={cycle} leader={leader} elapsed={:.1}",
            run.started.elapsed().as_secs_f64()
        );
        match kind {
            Kind::Disk => {
                self.nodes[0].stop(false).await;
                self.nodes[0].start(false);
            }
            Kind::Restart => {
                // Followers stop first so the leader can write its current dump.
                for i in 0..3 {
                    if i != leader {
                        self.nodes[i].stop(true).await;
                    }
                }
                self.nodes[leader].stop(true).await;
                for node in &mut self.nodes {
                    node.start(false);
                }
            }
            Kind::Crash => {
                let victim = if cycle.is_multiple_of(2) {
                    leader
                } else {
                    (leader + 1) % 3
                };
                self.nodes[victim].stop(false).await;
                self.leader().await;
                if victim != leader {
                    self.roll_log(ctx, run).await;
                }
                self.nodes[victim].start(false);
                self.nodes[victim].settled().await;
            }
            Kind::Isolation => self.partition(leader, cycle).await,
        }
        self.leader().await;
    }

    async fn partition(&self, leader: usize, cycle: u64) {
        self.isolate(leader, true);
        within(async {
            let mut tick = tokio::time::interval(Duration::from_millis(50));
            while TcpStream::connect(self.nodes[leader].addr).await.is_ok() {
                tick.tick().await;
            }
        })
        .await;
        let elected = self.leader().await;
        assert_ne!(leader, elected, "isolated leader is fenced");
        self.isolate(leader, false);
        if cycle % 2 == 1 {
            // Preserve RAM while losing quorum. Killing two nodes would lose
            // the replicated state and require explicit forced recovery.
            let followers: Vec<_> = (0..3).filter(|i| *i != elected).collect();
            for &i in &followers {
                self.nodes[i].signal("-STOP");
            }
            within(async {
                let mut tick = tokio::time::interval(Duration::from_millis(50));
                while TcpStream::connect(self.nodes[elected].addr).await.is_ok() {
                    tick.tick().await;
                }
            })
            .await;
            for &i in &followers {
                self.nodes[i].signal("-CONT");
            }
        }
    }

    async fn roll_log(&self, ctx: &Context, run: &Run) {
        let c = connect(ctx, &[self.front.addr], "log-roll", true).await;
        // 288 MiB of replacements exceed the default 64 MiB log, while the
        // retained state remains one 256 KiB value. A lagging node needs an image.
        for i in 0..1152 {
            let data = payload(i, 256 << 10, run.seed);
            within(retained_retry(&c, &data)).await;
            run.progress(1);
        }
        within(c.disconnect()).await;
    }
}

async fn retained_retry(client: &Client, payload: &[u8]) {
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        match client
            .publish_retained("image/value", payload, QoS::AtLeastOnce)
            .await
        {
            Ok(()) => return,
            Err(zzq_client::Error::Rejected(code))
                if code == zzq_client::reason::UNSPECIFIED_ERROR.into() =>
            {
                // The image load also follows an election while a node is
                // down. Replacing this retained value is idempotent.
                tick.tick().await;
            }
            Err(error) => panic!("image load failed: {error:?}"),
        }
    }
}

pub(super) async fn run(name: &str, kind: Kind) {
    let run = Arc::new(Run::new(name));
    let ctx = context(2);
    let mut cluster = Cluster::new(&run, kind).await;
    let stop = Arc::new(Control {
        retry_rejections: kind != Kind::Disk,
        ..Control::default()
    });
    let mut workers = JoinSet::new();
    for n in 0..2 {
        let p = connect(&ctx, &[cluster.front.addr], &format!("live-p{n}"), true).await;
        let s = connect(&ctx, &[cluster.front.addr], &format!("live-s{n}"), true).await;
        let topic = format!("live/{n}");
        within(s.subscribe(&topic, QoS::ExactlyOnce)).await.unwrap();
        workers.spawn(live(run.clone(), stop.clone(), (p, s), topic, n));
    }
    let mut cycles = 0;
    while run.running() || cycles < 2 {
        tokio::select! {
            // Workers stay alive until stop. An early exit fails the whole job;
            // canceling this fault future drops the owned cluster on unwind.
            result = workers.join_next() => { result.unwrap().unwrap(); panic!("live worker ended early"); }
            () = async {
                checkpoint(&ctx, &cluster, &run, cycles, false).await;
                cluster.fault(&ctx, &run, kind, cycles).await;
                checkpoint(&ctx, &cluster, &run, cycles, true).await;
                stop.recovered().await;
            } => {}
        }
        run.progress(1);
        cycles += 1;
        // Fault cadence is a scheduled stimulus, independent of readiness checks.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    stop.stop.store(true, Relaxed);
    while let Some(result) = within(workers.join_next()).await {
        result.unwrap();
    }
    for node in &mut cluster.nodes {
        node.stop(true).await;
    }
    drop(cluster);
    drop(ctx);
    Arc::try_unwrap(run).ok().unwrap().finish(cycles);
}

async fn connect(ctx: &Context, targets: &[SocketAddr], id: &str, reconnect: bool) -> Client {
    within(async {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            for addr in targets {
                let opts = ClientOptions {
                    clean_start: false,
                    session_expiry: 7200,
                    inbox: 128,
                    keep_alive: 2,
                    connect_timeout: Duration::from_secs(1),
                    reconnect: reconnect.then_some(Reconnect {
                        min: Duration::from_millis(100),
                        max: Duration::from_millis(500),
                    }),
                    ..ClientOptions::new(id)
                };
                if let Ok(c) = Client::connect(ctx, &format!("tcp://{addr}"), opts).await {
                    return c;
                }
            }
            tick.tick().await;
        }
    })
    .await
}

async fn checkpoint(ctx: &Context, cluster: &Cluster, run: &Run, cycle: u64, check: bool) {
    let targets: Vec<_> = cluster.nodes.iter().map(|n| n.addr).collect();
    let mut sub = connect(ctx, &targets, "checkpoint-sub", false).await;
    let data = payload(cycle, 1024, run.seed);
    if check {
        assert!(
            sub.connected().session_present,
            "checkpoint session survived"
        );
        checkpoint_drain(&mut sub, cycle, run.seed).await;
        within(sub.disconnect()).await;
        let mut retained = connect(ctx, &targets, "checkpoint-retained", false).await;
        within(retained.subscribe("checkpoint-retained", QoS::AtLeastOnce))
            .await
            .unwrap();
        within(async {
            loop {
                let got = next(&mut retained).await;
                let actual = super::support::sequence(&got.payload);
                assert!(actual <= cycle);
                if actual == cycle {
                    assert!(got.retain);
                    super::support::same(&got.payload, &data);
                    break;
                }
            }
        })
        .await;
        within(retained.unsubscribe(&["checkpoint-retained"]))
            .await
            .unwrap();
        within(retained.disconnect()).await;
    } else {
        within(sub.subscribe("checkpoint", QoS::ExactlyOnce))
            .await
            .unwrap();
        within(sub.disconnect()).await;
        let p = connect(ctx, &targets, "checkpoint-pub", false).await;
        for seq in cycle * 16..cycle * 16 + 16 {
            within(p.publish("checkpoint", payload(seq, 1024, run.seed), QoS::ExactlyOnce))
                .await
                .unwrap();
        }
        within(p.publish_retained("checkpoint-retained", &data, QoS::AtLeastOnce))
            .await
            .unwrap();
        within(p.disconnect()).await;
    }
}

#[derive(Default)]
struct Control {
    stop: AtomicBool,
    retry_rejections: bool,
    progress: [AtomicU64; 2],
}

impl Control {
    async fn recovered(&self) {
        let before = self.progress.each_ref().map(|p| p.load(Relaxed));
        within(async {
            let mut tick = tokio::time::interval(Duration::from_millis(20));
            while self
                .progress
                .iter()
                .zip(before)
                .any(|(p, n)| p.load(Relaxed) <= n)
            {
                tick.tick().await;
            }
        })
        .await;
    }
}

async fn live(
    run: Arc<Run>,
    control: Arc<Control>,
    clients: (Client, Client),
    topic: String,
    stream: u64,
) {
    let (publisher, mut sub) = clients;
    let qos = if stream == 0 {
        QoS::AtLeastOnce
    } else {
        QoS::ExactlyOnce
    };
    let mut seq = 0;
    let mut duplicates = 0;
    let mut rejected = 0;
    let mut application_replays = VecDeque::new();
    let mut max_ack = Duration::ZERO;
    while !control.stop.load(Relaxed) {
        let expected = payload(seq, 1024, run.seed);
        let started = Instant::now();
        let acked = AtomicBool::new(false);
        let delivered = AtomicBool::new(false);
        let result = tokio::time::timeout(WAIT, async {
            tokio::join!(
                async {
                    let n = publish_retry(&publisher, (&topic, &expected, qos), &control).await;
                    acked.store(true, Relaxed);
                    n
                },
                async {
                    let n = live_receive(&mut sub, seq, run.seed, qos, &application_replays).await;
                    delivered.store(true, Relaxed);
                    n
                }
            )
        })
        .await;
        let (retries, dup) = result.unwrap_or_else(|_| {
            panic!(
                "live stream={stream} seq={seq} acked={} delivered={} stalled for 30s",
                acked.load(Relaxed),
                delivered.load(Relaxed)
            )
        });
        if retries > 0 {
            application_replays.push_back(seq);
        }
        while application_replays.front().is_some_and(|n| *n + 128 < seq) {
            application_replays.pop_front();
        }
        duplicates += dup;
        rejected += retries;
        max_ack = max_ack.max(started.elapsed());
        seq += 1;
        control.progress[stream as usize].store(seq, Relaxed);
        run.progress(1);
    }
    eprintln!(
        "live stream={stream} acked={seq} duplicates={duplicates} rejected_during_fault={rejected} max_ack_ms={}",
        max_ack.as_millis()
    );
    within(publisher.disconnect()).await;
    within(sub.disconnect()).await;
}

async fn publish_retry(p: &Client, message: (&str, &[u8], QoS), control: &Control) -> u64 {
    let mut retries = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    loop {
        match p.publish(message.0, message.1, message.2).await {
            Ok(()) => return retries,
            Err(zzq_client::Error::Rejected(code))
                if code == zzq_client::reason::UNSPECIFIED_ERROR.into()
                    && control.retry_rejections =>
            {
                // Losing leadership rejects writes that never committed. Retry
                // this application ID, and account for a possible duplicate.
                retries += 1;
                tick.tick().await;
            }
            Err(e) => panic!("publish outside expected fault rejection: {e:?}"),
        }
    }
}

async fn live_receive(
    sub: &mut Client,
    seq: u64,
    seed: u64,
    qos: QoS,
    application_replays: &VecDeque<u64>,
) -> u64 {
    let mut duplicates = 0;
    loop {
        let got = sub.recv().await.expect("live subscriber ended");
        let actual = super::support::sequence(&got.payload);
        super::support::same(&got.payload, &payload(actual, 1024, seed));
        if actual == seq {
            return duplicates;
        }
        assert!(
            actual < seq && (qos == QoS::AtLeastOnce || application_replays.contains(&actual)),
            "out of order or protocol QoS 2 duplicate: expected={seq} actual={actual}"
        );
        duplicates += 1;
    }
}

async fn checkpoint_drain(sub: &mut Client, cycle: u64, seed: u64) {
    within(async {
        let mut expected = cycle * 16;
        while expected < cycle * 16 + 16 {
            let got = next(sub).await;
            let actual = super::support::sequence(&got.payload);
            super::support::same(&got.payload, &payload(actual, 1024, seed));
            assert!(
                actual <= expected,
                "lost checkpoint message: wanted={expected} got={actual}"
            );
            if actual == expected {
                expected += 1;
            }
        }
    })
    .await;
}

//! Three brokers of a cluster on one thread under a paused clock: their
//! links run through a router that can cut them, and each has its own
//! lanes and inproc endpoint for clients.

use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::mpsc;
use zzq_broker::{Broker, BrokerOptions, ClusterLinks, ClusterOptions, ClusterStart, Context};
use zzq_client::{Client, ClientOptions};
use zzq_cluster::driver::Timing;
use zzq_cluster::node::{LogLimits, NodeConfig, NodeStatus, Outgoing, Role};
use zzq_cluster::wire::WireLimits;
use zzq_cluster::{Configuration, Digest, GroupId, NodeId, PipelineLimits};
use zzq_proto::Version;

pub(crate) const NODES: usize = 3;

/// Runs `test` on a current-thread runtime with a paused clock, inside a
/// local task set. A task that never lets virtual time pass, as a message
/// loop between nodes would, fails it after a minute.
pub(crate) fn simulated<F: Future<Output = ()>>(test: impl FnOnce() -> F) {
    let (done, finished) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        if finished.recv_timeout(Duration::from_secs(60)) == Err(RecvTimeoutError::Timeout) {
            eprintln!("no progress in a minute: a task spins in zero virtual time");
            std::process::abort();
        }
    });
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&rt, test());
    drop(done);
}

pub(crate) const fn node_id(i: usize) -> NodeId {
    NodeId::from_bytes([i as u8 + 1; 16])
}

fn node_config(i: usize, log: LogLimits) -> NodeConfig {
    NodeConfig {
        configuration: Configuration::new(
            GroupId::from_bytes([7; 16]),
            1,
            Digest::from_bytes([8; 32]),
            [0, 1, 2].map(node_id),
        )
        .unwrap(),
        local: node_id(i),
        timing: Timing {
            heartbeat: Duration::from_millis(10),
            primary_timeout: Duration::from_millis(100),
            retransmit: Duration::from_millis(20),
            election_timeout: Duration::from_millis(150),
            max_election_timeout: Duration::from_secs(1),
        },
        pipeline: PipelineLimits {
            max_operations: 256,
            max_body_bytes: 64 << 20,
        },
        log,
        wire: WireLimits::for_transfer(256, 64 << 20).unwrap(),
        fingerprint: 2,
    }
}

fn links(outbound: &mpsc::Sender<Outgoing>, inbound: mpsc::Receiver<[Bytes; 3]>) -> ClusterLinks {
    ClusterLinks {
        outbound: outbound.clone(),
        inbound: Arc::new(Mutex::new(Some(inbound))),
    }
}

/// Cut links: `cut[a][b]` drops what node `a` sends node `b`.
type Cuts = Arc<Mutex<[[bool; NODES]; NODES]>>;
/// Where the router hands each node's inbound frames.
type Routes = Arc<Mutex<Vec<mpsc::Sender<[Bytes; 3]>>>>;

/// One node: its broker and lanes, while it runs.
pub(crate) struct Member {
    pub(crate) ctx: Context,
    pub(crate) broker: Option<Broker>,
    /// What the node sends, for the router.
    outbound: mpsc::Sender<Outgoing>,
}

pub(crate) struct Cluster {
    pub(crate) members: Vec<Member>,
    cuts: Cuts,
    routes: Routes,
    log: LogLimits,
}

impl Cluster {
    /// Three nodes of a new cluster.
    pub(crate) async fn new() -> Cluster {
        Cluster::with_log(LogLimits::default()).await
    }

    /// Three nodes of a new cluster whose logs keep at most `log`.
    pub(crate) async fn with_log(log: LogLimits) -> Cluster {
        let mut c = Cluster {
            members: Vec::new(),
            cuts: Cuts::default(),
            routes: Routes::default(),
            log,
        };
        for i in 0..NODES {
            let (outbound, out) = mpsc::channel(4096);
            c.route(i, out);
            let (tx, rx) = mpsc::channel(4096);
            c.routes.lock().unwrap().push(tx);
            let ctx = Context::local(2);
            let broker = c
                .start(i, &ctx, ClusterStart::Bootstrap, links(&outbound, rx))
                .await;
            c.members.push(Member {
                ctx,
                broker: Some(broker),
                outbound,
            });
        }
        c
    }

    /// Routes what node `i` sends, unless the link is cut.
    fn route(&self, i: usize, mut out: mpsc::Receiver<Outgoing>) {
        let (routes, cuts) = (self.routes.clone(), self.cuts.clone());
        tokio::task::spawn_local(async move {
            while let Some(Outgoing { to, frames }) = out.recv().await {
                let Some(j) = (0..NODES).find(|&j| node_id(j) == to) else {
                    continue;
                };
                if cuts.lock().unwrap()[i][j] {
                    continue;
                }
                let tx = routes.lock().unwrap().get(j).cloned();
                if let Some(tx) = tx {
                    let _ = tx.try_send(frames);
                }
            }
        });
    }

    async fn start(
        &self,
        i: usize,
        ctx: &Context,
        start: ClusterStart,
        links: ClusterLinks,
    ) -> Broker {
        let opts = BrokerOptions {
            cluster: Some(ClusterOptions {
                node: node_config(i, self.log),
                start,
                links,
            }),
            ..BrokerOptions::default()
        };
        let broker = Broker::new(ctx, opts).await.unwrap();
        broker.bind(&format!("inproc://node-{i}")).await.unwrap();
        broker
    }

    pub(crate) fn status(&self, i: usize) -> Option<NodeStatus> {
        self.members[i].broker.as_ref()?.cluster_status()
    }

    /// The node that leads, if exactly one does.
    pub(crate) fn leader(&self) -> Option<usize> {
        let mut leaders =
            (0..NODES).filter(|&i| self.status(i).is_some_and(|s| s.role == Role::Leader));
        let first = leaders.next()?;
        leaders.next().is_none().then_some(first)
    }

    /// Waits until `done` holds, polling in virtual time, at most `secs`.
    pub(crate) async fn until(&self, secs: u64, mut done: impl FnMut(&Cluster) -> bool) -> bool {
        for _ in 0..secs * 100 {
            if done(self) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        done(self)
    }

    /// Stops node `i` as a crash would: its broker, its state and its
    /// links are gone.
    pub(crate) fn crash(&mut self, i: usize) {
        self.members[i].broker = None;
        self.isolate(i, true);
    }

    /// Starts node `i` again without its state: it recovers from the
    /// others.
    pub(crate) async fn restart(&mut self, i: usize) {
        let (tx, rx) = mpsc::channel(4096);
        self.routes.lock().unwrap()[i] = tx;
        let ctx = Context::local(2);
        let outbound = self.members[i].outbound.clone();
        let broker = self
            .start(i, &ctx, ClusterStart::Recover, links(&outbound, rx))
            .await;
        self.members[i].ctx = ctx;
        self.members[i].broker = Some(broker);
        self.isolate(i, false);
    }

    /// Cuts node `i` off the others, or joins it again.
    pub(crate) fn isolate(&self, i: usize, cut: bool) {
        let mut cuts = self.cuts.lock().unwrap();
        for j in (0..NODES).filter(|&j| j != i) {
            cuts[i][j] = cut;
            cuts[j][i] = cut;
        }
    }

    /// A client of node `i`, with a session that outlives its connection.
    pub(crate) async fn client(&self, i: usize, id: &str, clean: bool) -> Client {
        self.connect(i, id, clean).await.unwrap()
    }

    /// Connects a client to node `i`, which refuses while it does not lead.
    pub(crate) async fn connect(
        &self,
        i: usize,
        id: &str,
        clean: bool,
    ) -> zzq_client::Result<Client> {
        let opts = ClientOptions {
            version: Version::V5,
            clean_start: clean,
            session_expiry: 3600,
            ..ClientOptions::new(id)
        };
        Client::connect(&self.members[i].ctx, &format!("inproc://node-{i}"), opts).await
    }

    /// Whether node `i` follows node `j` in its view and applied all it
    /// committed.
    pub(crate) fn caught_up(&self, i: usize, j: usize) -> bool {
        match (self.status(i), self.status(j)) {
            (Some(a), Some(b)) => {
                a.role == Role::Follower
                    && !a.recovering
                    && a.view == b.view
                    && a.applied >= b.committed
                    && b.committed.0 > 0
            }
            _ => false,
        }
    }

    /// Moves the leadership on, cutting off each leader in turn until
    /// node `to` leads. Each rejoins once it follows the next.
    pub(crate) async fn lead(&self, to: usize) {
        for _ in 0..NODES {
            let l = self.leader().expect("a leader");
            if l == to {
                return;
            }
            self.isolate(l, true);
            assert!(
                self.until(5, |c| c.leader().is_some_and(|n| n != l)).await,
                "another node than {l} leads"
            );
            self.isolate(l, false);
            let n = self.leader().unwrap();
            assert!(
                self.until(5, |c| c.caught_up(l, n)).await,
                "node {l} follows node {n}"
            );
        }
        assert_eq!(self.leader(), Some(to));
    }
}

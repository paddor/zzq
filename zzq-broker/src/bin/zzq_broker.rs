//! Standalone broker.
//!
//! Usage: `zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM]
//! [--client-ca PEM] [--data-dir PATH] [--state-file PATH]
//! [--max-state-mib N] [--busy-poll] [--spin-us N]
//! [--cluster-node NAME --cluster-peers NAME=URI,NAME=URI,NAME=URI
//! [--cluster-name NAME] [--cluster-bootstrap | --cluster-force-start]]`
//!
//! Without `--bind`, it binds `tcp://0.0.0.0:1883`. `--cert` and `--key`,
//! which go together, enable `tls://`, `wss://` and `quic://` binds.
//! `--client-ca` makes them require client certificates. `--data-dir`
//! keeps retained messages, sessions and their QoS 1/2 messages on disk,
//! and acks what goes there once it is (Linux only).
//! `--state-file` keeps sessions
//! across a restart: SIGTERM or SIGINT write it, and the next start loads
//! it. `--max-state-mib` sets `max_state_bytes`. `--spin-us` sets how long
//! a lane polls on after work before it sleeps. Prints each bound endpoint
//! on its own line, then runs until SIGTERM or SIGINT. With `--data-dir`,
//! it says on stderr when journals there could not make a new segment, as
//! on a full device, and when they can again, and when the state stops
//! growing for lack of room there, and when it has room again.
//!
//! `--cluster-node` and `--cluster-peers` make it node `NAME` of a
//! cluster of three, which `--cluster-peers` lists with the OMQ endpoint
//! each binds for the others, the same on every node. Only the leader
//! binds the `--bind` endpoints. `--cluster-bootstrap` starts a new
//! cluster, once, on all three; a node restarts from `--state-file` if it
//! left one there, else it recovers its state from the other two.
//! `--cluster-force-start` is for when no two nodes have the state: the
//! node starts a new epoch from the newest state it has, its state file or
//! the one it last started from, and the others join once they start
//! without theirs. What was acked after that state is lost. It says on
//! stderr when it leads, follows or elects, and when another node forced
//! an epoch it does not join.

use std::process::ExitCode;
use std::time::Duration;

use zzq_broker::{Broker, BrokerOptions, Config, Context};
use zzq_cluster::node::Role;

struct Args {
    lanes: usize,
    binds: Vec<String>,
    cert: Option<String>,
    key: Option<String>,
    client_ca: Option<String>,
    data_dir: Option<String>,
    state_file: Option<String>,
    max_state_mib: Option<usize>,
    busy_poll: bool,
    spin: Duration,
    cluster: ClusterArgs,
}

#[derive(Default)]
struct ClusterArgs {
    name: Option<String>,
    node: Option<String>,
    peers: Option<String>,
    bootstrap: bool,
    force: bool,
}

fn parse() -> Option<Args> {
    let mut a = Args {
        lanes: 1,
        binds: Vec::new(),
        cert: None,
        key: None,
        client_ca: None,
        data_dir: None,
        state_file: None,
        max_state_mib: None,
        busy_poll: false,
        spin: Config::default().spin,
        cluster: ClusterArgs::default(),
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        match flag.as_str() {
            "--busy-poll" => a.busy_poll = true,
            "--cluster-bootstrap" => a.cluster.bootstrap = true,
            "--cluster-force-start" => a.cluster.force = true,
            _ => set(&mut a, &flag, args.next()?)?,
        }
    }
    if a.binds.is_empty() {
        a.binds.push("tcp://0.0.0.0:1883".to_owned());
    }
    Some(a)
}

/// Sets what `flag` takes `value` for. None for an unknown flag or a
/// value that does not parse.
fn set(a: &mut Args, flag: &str, value: String) -> Option<()> {
    match flag {
        "--lanes" => a.lanes = value.parse().ok()?,
        "--bind" => a.binds.push(value),
        "--cert" => a.cert = Some(value),
        "--key" => a.key = Some(value),
        "--client-ca" => a.client_ca = Some(value),
        "--data-dir" => a.data_dir = Some(value),
        "--state-file" => a.state_file = Some(value),
        "--max-state-mib" => a.max_state_mib = Some(value.parse().ok()?),
        "--spin-us" => a.spin = Duration::from_micros(value.parse().ok()?),
        "--cluster-name" => a.cluster.name = Some(value),
        "--cluster-node" => a.cluster.node = Some(value),
        "--cluster-peers" => a.cluster.peers = Some(value),
        _ => return None,
    }
    Some(())
}

fn options(a: &Args) -> Result<BrokerOptions, String> {
    let mut opts = BrokerOptions {
        data_dir: a.data_dir.as_ref().map(Into::into),
        state_file: a.state_file.as_ref().map(Into::into),
        ..BrokerOptions::default()
    };
    if let Some(mib) = a.max_state_mib {
        opts.max_state_bytes = mib.saturating_mul(1 << 20);
    }
    tls(a, &mut opts)?;
    opts.cluster = cluster::cluster(&a.cluster)?;
    Ok(opts)
}

#[cfg(feature = "tls")]
fn tls(a: &Args, opts: &mut BrokerOptions) -> Result<(), String> {
    let read = |p: &str| std::fs::read(p).map_err(|e| format!("{p}: {e}"));
    if a.cert.is_some() != a.key.is_some() || (a.client_ca.is_some() && a.cert.is_none()) {
        return Err("--cert and --key go together, and --client-ca needs them".to_owned());
    }
    if let (Some(cert), Some(key)) = (&a.cert, &a.key) {
        opts.transport.tls = Some(zzq_broker::TlsOptions {
            cert_chain_pem: Some(read(cert)?),
            key_pem: Some(read(key)?),
            client_ca_pem: a.client_ca.as_deref().map(read).transpose()?,
            ..Default::default()
        });
    }
    Ok(())
}

#[cfg(not(feature = "tls"))]
fn tls(a: &Args, _opts: &mut BrokerOptions) -> Result<(), String> {
    if a.cert.is_some() || a.key.is_some() || a.client_ca.is_some() {
        return Err("built without TLS support".to_owned());
    }
    Ok(())
}

/// The cluster options of the flags.
#[cfg(feature = "cluster")]
mod cluster {
    use std::time::Duration;

    use xxhash_rust::xxh3::xxh3_128;
    use zzq_broker::{ClusterOptions, ClusterStart};
    use zzq_cluster::driver::Timing;
    use zzq_cluster::node::{LogLimits, NodeConfig};
    use zzq_cluster::wire::WireLimits;
    use zzq_cluster::{Configuration, GroupId, NodeId, PipelineLimits, integrity};

    use super::{ClusterArgs, chain};

    /// A node ID, group ID or digest from a name: every node derives the same
    /// from the same flags.
    fn named(context: &str, name: &str) -> [u8; 16] {
        xxh3_128(format!("{context} {name}").as_bytes()).to_be_bytes()
    }

    /// The node `--cluster-node` names of the cluster `--cluster-peers` lists.
    pub(crate) fn cluster(c: &ClusterArgs) -> Result<Option<ClusterOptions>, String> {
        let (node, peers) = match (&c.node, &c.peers) {
            (None, None) => return Ok(None),
            (Some(node), Some(peers)) => (node, peers),
            _ => return Err("--cluster-node and --cluster-peers go together".to_owned()),
        };
        let usage = || "--cluster-peers lists three nodes as NAME=URI,NAME=URI,NAME=URI".to_owned();
        let listed: Vec<(&str, &str)> = peers
            .split(',')
            .map(|p| p.split_once('='))
            .collect::<Option<_>>()
            .ok_or_else(usage)?;
        let [a, b, c_] = listed[..] else {
            return Err(usage());
        };
        let id = |name: &str| NodeId::from_bytes(named("zzq cluster node", name));
        let mut names = [a.0, b.0, c_.0];
        names.sort_unstable();
        let name = c.name.as_deref().unwrap_or("zzq");
        let digest = integrity::hash(
            "zzq-broker cluster configuration",
            names.join("\n").as_bytes(),
        );
        let configuration = Configuration::new(
            GroupId::from_bytes(named("zzq cluster", name)),
            1,
            digest,
            [a, b, c_].map(|(n, _)| id(n)),
        )
        .map_err(|e| format!("--cluster-peers: {e}"))?;
        let nodes: Vec<(NodeId, String)> =
            listed.iter().map(|&(n, e)| (id(n), e.to_owned())).collect();
        if !listed.iter().any(|&(n, _)| n == node) {
            return Err(format!(
                "--cluster-peers does not list --cluster-node {node}"
            ));
        }
        let local = id(node);
        let links =
            zzq_broker::omq_links(local, &nodes).map_err(|e| format!("cluster: {}", chain(&e)))?;
        Ok(Some(ClusterOptions {
            node: node_config(configuration, local)?,
            start: match (c.bootstrap, c.force) {
                (true, true) => {
                    return Err("--cluster-bootstrap or --cluster-force-start, not both".to_owned());
                }
                (true, false) => ClusterStart::Bootstrap,
                (false, true) => ClusterStart::Force,
                (false, false) => ClusterStart::Recover,
            },
            links,
        }))
    }

    fn node_config(configuration: Configuration, local: NodeId) -> Result<NodeConfig, String> {
        Ok(NodeConfig {
            configuration,
            local,
            timing: Timing {
                heartbeat: Duration::from_millis(50),
                primary_timeout: Duration::from_millis(500),
                retransmit: Duration::from_millis(100),
                election_timeout: Duration::from_millis(750),
                max_election_timeout: Duration::from_secs(5),
            },
            pipeline: PipelineLimits {
                max_operations: 256,
                max_body_bytes: 64 << 20,
            },
            log: LogLimits::default(),
            wire: WireLimits::for_transfer(256, 64 << 20).ok_or("cluster wire limits")?,
            // The broker sets it.
            fingerprint: 0,
        })
    }
}

#[cfg(not(feature = "cluster"))]
mod cluster {
    use zzq_broker::ClusterOptions;

    use super::ClusterArgs;

    pub(crate) fn cluster(c: &ClusterArgs) -> Result<Option<ClusterOptions>, String> {
        if c.node.is_some() || c.peers.is_some() {
            return Err("built without cluster support".to_owned());
        }
        Ok(None)
    }
}

/// An error with its causes, outermost first.
fn chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        out.push_str(": ");
        out.push_str(&c.to_string());
        cause = c.source();
    }
    out
}

async fn serve(a: Args, opts: BrokerOptions) -> Result<(), String> {
    // Before anything is bound, so no signal finds the default handler.
    let stopped = stop_signal().map_err(|e| format!("signals: {e}"))?;
    let config = Config {
        io_threads: a.lanes,
        name: "zzq".to_owned(),
        busy_poll: a.busy_poll,
        spin: a.spin,
    };
    let ctx = Context::with_config(&config).map_err(|e| format!("context: {}", chain(&e)))?;
    let max_state = opts.max_state_bytes;
    let broker = Broker::new(&ctx, opts)
        .await
        .map_err(|e| format!("broker: {}", chain(&e)))?;
    for uri in &a.binds {
        let ep = broker
            .bind(uri)
            .await
            .map_err(|e| format!("bind {uri}: {}", chain(&e)))?;
        println!("{ep}");
    }
    tokio::select! {
        () = stopped => {}
        () = watch(&broker, a.data_dir.as_deref(), max_state) => {}
        () = roles(&broker, a.cluster.node.as_deref()) => {}
    }
    broker
        .shutdown()
        .await
        .map_err(|e| format!("shutdown: {}", chain(&e)))
}

/// Says on stderr, checked each second, when journals in the data directory
/// `dir` could not make a new segment, and when they can again, and when the
/// state stops growing for lack of room there, below `max_state`, and when
/// it has room again. Ends never.
async fn watch(broker: &Broker, dir: Option<&str>, max_state: usize) {
    let Some(dir) = dir else {
        return std::future::pending().await;
    };
    let max = u64::try_from(max_state).unwrap_or(u64::MAX);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let (mut full, mut short) = (0, false);
    loop {
        tick.tick().await;
        // A broker that stopped has nothing more to say.
        let Ok(stats) = broker.stats().await else {
            return std::future::pending().await;
        };
        if let Some(line) = full_change(dir, full, stats.full_journals) {
            eprintln!("{line}");
        }
        full = stats.full_journals;
        let line;
        (short, line) = room_change(dir, short, stats.state_bytes, stats.state_limit, max);
        if let Some(line) = line {
            eprintln!("{line}");
        }
    }
}

/// Says on stderr, checked every 100 ms, when cluster node `node` comes to
/// lead, follow or elect. Ends never.
async fn roles(broker: &Broker, node: Option<&str>) {
    let Some(node) = node else {
        return std::future::pending().await;
    };
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let (mut was, mut told) = (None, None);
    loop {
        tick.tick().await;
        let status = broker.cluster_status();
        let role = status.map(|s| s.role);
        if role != was
            && let Some(line) = role.and_then(|r| role_line(node, r))
        {
            eprintln!("{line}");
        }
        was = role;
        let newer = status.and_then(|s| s.newer_epoch.map(|n| (s.epoch, n)));
        if newer != told
            && let Some((epoch, newer)) = newer
        {
            eprintln!(
                "cluster node {node} keeps epoch {epoch}: another node forced epoch {newer}; \
                 restart this one without its state file to join it"
            );
        }
        told = newer;
    }
}

fn role_line(node: &str, role: Role) -> Option<String> {
    let what = match role {
        Role::Leader => "leads",
        Role::Follower => "follows",
        Role::Electing => "elects",
        _ => return None,
    };
    Some(format!("cluster node {node} {what}"))
}

/// What to say when the journals in `dir` that could not make a new
/// segment went from `was` to `now`.
fn full_change(dir: &str, was: u64, now: u64) -> Option<String> {
    match (was, now) {
        (0, 1..) => Some(format!(
            "data directory {dir} is full: {now} journals could not make a new segment"
        )),
        (1.., 0) => Some(format!("data directory {dir} has room again")),
        _ => None,
    }
}

/// Whether the state in `dir` is short of room, given whether it `was`, and
/// what to say if that changed: short once the `state` bytes reach a
/// `limit` the device set below `max`, and no longer once they are back
/// under half of it, or the device no longer sets it.
fn room_change(dir: &str, was: bool, state: u64, limit: u64, max: u64) -> (bool, Option<String>) {
    let by_device = limit < max;
    let short = by_device
        && if was {
            state > limit / 2
        } else {
            state >= limit
        };
    let line = match (was, short) {
        (false, true) => Some(format!(
            "data directory {dir} is short of room: the state stops growing at {} MiB",
            limit >> 20
        )),
        (true, false) => Some(format!("data directory {dir} has room for the state again")),
        _ => None,
    };
    (short, line)
}

/// Handles SIGTERM and SIGINT from now on, and returns the wait for either.
#[cfg(unix)]
fn stop_signal() -> std::io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn stop_signal() -> std::io::Result<impl Future<Output = ()>> {
    Ok(async {
        let _ = tokio::signal::ctrl_c().await;
    })
}

fn main() -> ExitCode {
    let Some(args) = parse() else {
        eprintln!(
            "usage: zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM] \
             [--client-ca PEM] [--data-dir PATH] [--state-file PATH] \
             [--max-state-mib N] [--busy-poll] [--spin-us N] \
             [--cluster-node NAME --cluster-peers NAME=URI,NAME=URI,NAME=URI \
             [--cluster-name NAME] [--cluster-bootstrap | --cluster-force-start]]"
        );
        return ExitCode::FAILURE;
    };
    let result = options(&args).and_then(|opts| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        rt.block_on(serve(args, opts))
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use zzq_cluster::node::Role;

    use super::{ClusterArgs, full_change, role_line, room_change};

    #[test]
    fn a_role_change_is_said_by_node_name() {
        assert_eq!(
            role_line("a", Role::Leader).as_deref(),
            Some("cluster node a leads")
        );
        assert_eq!(
            role_line("b", Role::Follower).as_deref(),
            Some("cluster node b follows")
        );
        assert_eq!(
            role_line("c", Role::Electing).as_deref(),
            Some("cluster node c elects")
        );
    }

    #[test]
    fn cluster_flags_must_name_three_nodes_and_this_one_among_them() {
        let args = |node: Option<&str>, peers: Option<&str>| ClusterArgs {
            node: node.map(Into::into),
            peers: peers.map(Into::into),
            ..ClusterArgs::default()
        };
        let err = |a: ClusterArgs| super::cluster::cluster(&a).err().unwrap_or_default();
        assert!(super::cluster::cluster(&args(None, None)).is_ok_and(|c| c.is_none()));
        let two = "a=tcp://127.0.0.1:1,b=tcp://127.0.0.1:2";
        if cfg!(feature = "cluster") {
            assert!(err(args(Some("a"), None)).contains("go together"));
            assert!(err(args(Some("a"), Some(two))).contains("three nodes"));
            assert!(err(args(Some("a"), Some("a"))).contains("three nodes"));
            let three = "a=tcp://127.0.0.1:1,b=tcp://127.0.0.1:2,c=tcp://127.0.0.1:3";
            assert!(err(args(Some("d"), Some(three))).contains("does not list"));
            let twice = "a=tcp://127.0.0.1:1,a=tcp://127.0.0.1:2,c=tcp://127.0.0.1:3";
            assert!(err(args(Some("a"), Some(twice))).starts_with("--cluster-peers"));
        } else {
            assert!(err(args(Some("a"), Some(two))).contains("without cluster"));
        }
    }

    #[test]
    fn a_full_data_dir_is_said_once_when_it_fills_and_once_when_it_empties() {
        let said = |was, now| full_change("d", was, now);
        assert_eq!(
            said(0, 2).as_deref(),
            Some("data directory d is full: 2 journals could not make a new segment")
        );
        assert_eq!(said(2, 3), None);
        assert_eq!(
            said(3, 0).as_deref(),
            Some("data directory d has room again")
        );
        assert_eq!(said(0, 0), None);
    }

    #[test]
    fn a_data_dir_short_of_room_is_said_once_until_the_state_is_half_of_it() {
        let mib = 1 << 20;
        let said = |was, state, limit| room_change("d", was, state, limit, u64::MAX);
        assert_eq!(
            said(false, 20 * mib, 15 * mib),
            (
                true,
                Some("data directory d is short of room: the state stops growing at 15 MiB".into())
            )
        );
        assert_eq!(said(true, 10 * mib, 15 * mib), (true, None));
        assert_eq!(
            said(true, 7 * mib, 15 * mib),
            (
                false,
                Some("data directory d has room for the state again".into())
            )
        );
        assert_eq!(said(false, 10 * mib, 15 * mib), (false, None));
        // A limit `max_state_bytes` sets is no lack of room.
        assert_eq!(
            room_change("d", false, 20 * mib, 15 * mib, 15 * mib),
            (false, None)
        );
    }
}

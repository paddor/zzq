//! Clusters of three for the failover suite: node `i` on CPU `i`, MQTT on
//! port 1883 + `i`.

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use super::{Running, port_free, stop, store, wait_port};
use crate::paths::Paths;
use crate::runner::Cmd;

/// Nodes of a cluster.
pub(crate) const NODES: u16 = 3;
/// MQTT port of node 0.
const MQTT: u16 = 1883;
/// Port of node 0 for the other nodes.
const PEER: u16 = 7101;
/// How long a cluster may take to form.
const FORM: Duration = Duration::from_secs(60);

/// A running cluster, stopped on drop.
#[derive(Debug)]
pub(crate) struct Cluster {
    nodes: Vec<Child>,
    /// Nodes in containers, and the stores they need.
    containers: Option<Running>,
    /// For each node, the command that sends it `SIGNAL`.
    stops: Vec<String>,
}

impl Cluster {
    /// The nodes' MQTT endpoints: `tcp://…,tcp://…,tcp://…`.
    pub(crate) fn uris() -> String {
        let uris: Vec<String> = (0..NODES)
            .map(|i| format!("tcp://127.0.0.1:{}", MQTT + i))
            .collect();
        uris.join(",")
    }

    /// For each node, the command that sends its process group `signal`:
    /// `kill -KILL -12|…`.
    pub(crate) fn stops(&self, signal: &str) -> String {
        let stops: Vec<String> = self
            .stops
            .iter()
            .map(|s| s.replace("SIGNAL", signal))
            .collect();
        stops.join("|")
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in &mut self.nodes {
            stop(child);
        }
        drop(self.containers.take());
        for i in 0..NODES {
            if let Err(e) = port_free(MQTT + i, Duration::from_secs(60)) {
                eprintln!("# {e}");
            }
        }
    }
}

/// A new cluster of broker `name` in `config`, formed.
///
/// # Errors
///
/// If there is no cluster of `name`, a node does not start, or the
/// cluster does not form within a minute.
pub(crate) fn start(name: &str, config: &str, p: &Paths) -> Result<Cluster, String> {
    match name {
        "zzq" => zzq(p),
        "vernemq" => vernemq(config, p),
        "tbmq" => tbmq(p),
        "nats" => nats(p),
        _ => Err(format!("no cluster of {name}")),
    }
}

/// The options `zzq_failover` needs for broker `name`: `v=4` for one that
/// speaks MQTT 3.1.1 only.
pub(crate) fn client_args(name: &str) -> &'static [&'static str] {
    if name == "nats" { &["v=4"] } else { &[] }
}

fn spawn(cmd: &Cmd, cluster: &mut Cluster) -> Result<(), String> {
    let child = cmd
        .command()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("{cmd}: {e}"))?;
    cluster.stops.push(format!("kill -SIGNAL -{}", child.id()));
    cluster.nodes.push(child);
    Ok(())
}

const fn empty() -> Cluster {
    Cluster {
        nodes: Vec::new(),
        containers: None,
        stops: Vec::new(),
    }
}

fn accepts(port: u16) -> bool {
    TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, port))).is_ok()
}

/// `zzq_broker` nodes, each with 1 IO lane and a state file, once one
/// leads: only the leader listens.
fn zzq(p: &Paths) -> Result<Cluster, String> {
    let dir = store("failover")?;
    let peers: Vec<String> = (0..NODES)
        .map(|i| format!("n{i}=tcp://127.0.0.1:{}", PEER + i))
        .collect();
    let peers = peers.join(",");
    let mut cluster = empty();
    for i in 0..NODES {
        let cmd = Cmd::pinned(&i.to_string(), p.tool("zzq_broker"))
            .args(["--lanes", "1", "--bind"])
            .arg(format!("tcp://127.0.0.1:{}", MQTT + i))
            .args([
                "--cluster-node",
                &format!("n{i}"),
                "--cluster-peers",
                &peers,
            ])
            .arg("--cluster-bootstrap")
            .arg("--state-file")
            .arg(dir.join(format!("n{i}.state")));
        spawn(&cmd, &mut cluster)?;
    }
    let deadline = Instant::now() + FORM;
    while !(0..NODES).any(|i| accepts(MQTT + i)) {
        if Instant::now() >= deadline {
            return Err("no node of the cluster leads after 60s".to_owned());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Ok(cluster)
}

/// `VerneMQ` nodes, each a copy of the release with its own name, ports and
/// stores, once all three joined node 0.
fn vernemq(config: &str, p: &Paths) -> Result<Cluster, String> {
    let rel = p.rival("vernemq-src/_build/default/rel/vernemq")?;
    let conf = fs::read_to_string(p.conf(&format!("vernemq-{config}.conf")))
        .map_err(|e| format!("vernemq-{config}.conf: {e}"))?;
    let dir = store("failover-vernemq")?;
    let mut cluster = empty();
    for i in 0..NODES {
        let node = dir.join(format!("vmq{i}"));
        vernemq_node(&rel, &node, i, &conf)?;
        let cmd = Cmd::pinned(&i.to_string(), node.join("bin/vernemq"))
            .args(["console", "-noshell", "-noinput"]);
        spawn(&cmd, &mut cluster)?;
    }
    for i in 0..NODES {
        wait_port(MQTT + i, FORM)?;
    }
    let deadline = Instant::now() + FORM;
    while joined(&dir) < usize::from(NODES) {
        if Instant::now() >= deadline {
            return Err("the VerneMQ nodes did not join within 60s".to_owned());
        }
        // A join that did not take is asked again.
        for i in 1..NODES {
            let _ = admin(
                &dir,
                i,
                &["cluster", "join", "discovery-node=vmq0@127.0.0.1"],
            );
        }
        thread::sleep(Duration::from_secs(2));
    }
    Ok(cluster)
}

/// Node `i` of a `VerneMQ` cluster at `node`: a copy of release `rel`, its
/// config the original plus `conf` plus its own name, ports and stores.
fn vernemq_node(rel: &Path, node: &Path, i: u16, conf: &str) -> Result<(), String> {
    let copied = Command::new("cp")
        .arg("-a")
        .arg(rel)
        .arg(node)
        .status()
        .map_err(|e| format!("cp: {e}"))?;
    if !copied.success() {
        return Err(format!(
            "cp -a {} {}: {copied}",
            rel.display(),
            node.display()
        ));
    }
    let etc = node.join("etc");
    let orig = fs::read_to_string(etc.join("vernemq.conf.orig"))
        .map_err(|e| format!("vernemq.conf.orig: {e}"))?;
    let data = node.join("data");
    let _ = fs::remove_dir_all(&data);
    let own = format!(
        "nodename = vmq{i}@127.0.0.1\nlistener.tcp.default = 127.0.0.1:{}\nlistener.vmq.clustering = 127.0.0.1:{}\nlistener.http.default = 127.0.0.1:{}\nvmq_generic_msg_store.store_dir = {d}/msgstore\nvmq_swc.directory = {d}/swc_meta\nvmq_swc.dkm_directory = {d}/swc_dkm\n",
        MQTT + i,
        44053 + i,
        8888 + i,
        d = data.display(),
    );
    fs::write(etc.join("vernemq.conf"), orig + conf + &own)
        .map_err(|e| format!("vernemq.conf: {e}"))
}

/// TBMQ nodes in containers beside the stores they share, once a QoS 1
/// message gets through node 0.
fn tbmq(p: &Paths) -> Result<Cluster, String> {
    let ctrs = super::tbmq::cluster_ctrs(&p.conf("tbmq-cluster.env"))?;
    let running = super::containers(&ctrs, Duration::from_secs(2))?;
    super::tbmq::open()?;
    super::delivers(p, "TBMQ", &[])?;
    Ok(Cluster {
        nodes: Vec::new(),
        containers: Some(running),
        stops: (0..usize::from(NODES))
            .map(|i| super::tbmq::signal(i, "SIGNAL"))
            .collect(),
    })
}

/// `nats-server` nodes with `JetStream`, routed to each other, the MQTT
/// streams on all three, once a QoS 1 message gets through node 0.
fn nats(p: &Paths) -> Result<Cluster, String> {
    let bin = p.rival("nats/bin/nats-server")?;
    let conf = fs::read_to_string(p.conf("nats-cluster.conf"))
        .map_err(|e| format!("nats-cluster.conf: {e}"))?;
    let dir = store("failover-nats")?;
    let mut cluster = empty();
    for i in 0..NODES {
        let node = dir.join(format!("n{i}"));
        let path = dir.join(format!("n{i}.conf"));
        let text = conf
            .replace("@I@", &i.to_string())
            .replace("@STORE@", &node.to_string_lossy())
            .replace("@CLIENT@", &(4222 + i).to_string())
            .replace("@ROUTE@", &(6222 + i).to_string())
            .replace("@MQTT@", &(MQTT + i).to_string());
        fs::write(&path, text).map_err(|e| format!("{}: {e}", path.display()))?;
        let cmd = Cmd::pinned(&i.to_string(), &bin).arg("-c").arg(path);
        spawn(&cmd, &mut cluster)?;
    }
    for i in 0..NODES {
        wait_port(MQTT + i, FORM)?;
    }
    // The MQTT streams wait for a JetStream leader and all three nodes.
    super::delivers(p, "NATS", &["v=4"])?;
    Ok(cluster)
}

/// `vmq-admin` of node `i` under `dir`.
fn admin(dir: &Path, i: u16, args: &[&str]) -> Result<String, String> {
    let out = Command::new(dir.join(format!("vmq{i}/bin/vmq-admin")))
        .args(args)
        .stderr(Stdio::null())
        .output()
        .map_err(|e| format!("vmq-admin: {e}"))?;
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The nodes node 0 shows running.
fn joined(dir: &Path) -> usize {
    admin(dir, 0, &["cluster", "show"]).map_or(0, |s| s.matches("| true").count())
}

//! Starts and stops the brokers under test, on CPUs 0-3 and port 1883.

pub(crate) mod cluster;
mod tbmq;

use std::fs::{self, OpenOptions};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::paths::Paths;
use crate::runner::{Cmd, signal_group, wait_exit};

/// The port every broker listens on.
pub(crate) const PORT: u16 = 1883;
/// CPUs of the broker.
const CPUS: &str = "0-3";
/// Rival versions, as built or pulled for the runs.
pub(crate) const VERSIONS: [(&str, &str); 15] = [
    ("rmqtt", "0.24.0"),
    ("emqx", "6.3.2"),
    ("mosquitto", "2.1.2"),
    ("nanomq", "0.25.7"),
    ("rumqttd", "0.20.0"),
    ("xmq", "1.0.0"),
    ("flashmq", "1.27.3"),
    ("vernemq", "2.2.1"),
    ("fluxmq", "1.0.0"),
    ("saguin", "0.1.0-rc.1"),
    ("hivemq", "2026.5"),
    ("tbmq", "2.4.1"),
    ("rumqttc", "0.25.1"),
    ("ntex-mqtt", "9.3.0"),
    ("emqtt", "1.14.6"),
];

/// The version of rival `name`.
pub(crate) fn version(name: &str) -> Option<&'static str> {
    VERSIONS.iter().find(|(n, _)| *n == name).map(|(_, v)| *v)
}

/// A running broker, stopped on drop.
#[derive(Debug)]
pub(crate) struct Running {
    /// The broker's processes on the host, and those of the stores it
    /// needs, for CPU and memory.
    pids: Vec<u32>,
    child: Option<Child>,
    /// Removed in reverse order: the broker before its stores.
    containers: Vec<String>,
    port: u16,
}

impl Running {
    /// The PIDs as the load generators take them: `12,34`.
    pub(crate) fn cpu_pids(&self) -> String {
        let pids: Vec<String> = self.pids.iter().map(u32::to_string).collect();
        pids.join(",")
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            stop(child);
        }
        for name in self.containers.iter().rev() {
            remove(name);
        }
        if let Err(e) = port_free(self.port, Duration::from_secs(60)) {
            eprintln!("# {e}");
        }
    }
}

/// TERM, then KILL if the broker is still up after 15 s.
fn stop(child: &mut Child) {
    signal_group(child.id(), "TERM");
    if !wait_exit(child, Duration::from_secs(15)) {
        eprintln!("# broker {} ignored TERM, killing it", child.id());
        signal_group(child.id(), "KILL");
        let _ = child.wait();
    }
}

fn remove(container: &str) {
    let _ = Command::new("podman")
        .args(["rm", "-f", container])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

fn addr(port: u16) -> SocketAddr {
    (Ipv4Addr::LOCALHOST, port).into()
}

/// Waits up to `limit` until something accepts on `port`.
pub(crate) fn wait_port(port: u16, limit: Duration) -> Result<(), String> {
    let deadline = Instant::now() + limit;
    while TcpStream::connect(addr(port)).is_err() {
        if Instant::now() >= deadline {
            return Err(format!(
                "nothing listens on {port} after {}s",
                limit.as_secs()
            ));
        }
        thread::sleep(Duration::from_millis(500));
    }
    Ok(())
}

/// Waits up to `limit` until nothing accepts on `port`.
pub(crate) fn port_free(port: u16, limit: Duration) -> Result<(), String> {
    let deadline = Instant::now() + limit;
    while TcpStream::connect(addr(port)).is_ok() {
        if Instant::now() >= deadline {
            return Err(format!(
                "port {port} still taken after {}s",
                limit.as_secs()
            ));
        }
        thread::sleep(Duration::from_millis(500));
    }
    Ok(())
}

/// The hard limit on open files, which containers get as theirs.
fn nofile() -> Option<u64> {
    let limits = fs::read_to_string("/proc/self/limits").ok()?;
    let line = limits.lines().find(|l| l.starts_with("Max open files"))?;
    line.split_whitespace().nth(4)?.parse().ok()
}

/// Starts `cmd` pinned to the broker CPUs beside the stores in `running`,
/// waits for its port, then lets it settle for `settle`.
fn process(
    mut running: Running,
    cmd: &Cmd,
    p: &Paths,
    settle: Duration,
) -> Result<Running, String> {
    let log = |p: &Path| OpenOptions::new().create(true).append(true).open(p);
    let (out, err) = match &p.broker_log {
        Some(path) => {
            let f = log(path).map_err(|e| format!("{}: {e}", path.display()))?;
            let g = f.try_clone().map_err(|e| e.to_string())?;
            (Stdio::from(f), Stdio::from(g))
        }
        None => (Stdio::null(), Stdio::null()),
    };
    let child = cmd
        .command()
        .stdin(Stdio::null())
        .stdout(out)
        .stderr(err)
        .spawn()
        .map_err(|e| format!("{cmd}: {e}"))?;
    running.pids.push(child.id());
    running.child = Some(child);
    wait_port(PORT, START)?;
    thread::sleep(settle);
    Ok(running)
}

/// How long a broker, or a store it needs, may take to listen.
const START: Duration = Duration::from_secs(300);

/// A container of a broker, or of a store it needs.
#[derive(Debug, Default)]
struct Ctr {
    name: &'static str,
    image: &'static str,
    /// `podman run` options: volumes, environment, entrypoint.
    args: Vec<String>,
    /// The command after the image.
    cmd: Vec<String>,
    /// The port it listens on once ready.
    port: u16,
}

impl Ctr {
    /// The broker's container, on [`PORT`].
    const fn broker(name: &'static str, image: &'static str, args: Vec<String>) -> Ctr {
        Ctr {
            name,
            image,
            args,
            cmd: Vec::new(),
            port: PORT,
        }
    }
}

/// Starts `ctrs` in order with host networking, each once the one before
/// listens, then lets them settle for `settle`.
fn containers(ctrs: &[Ctr], settle: Duration) -> Result<Running, String> {
    let mut running = Running {
        pids: Vec::new(),
        child: None,
        containers: Vec::new(),
        port: PORT,
    };
    let nofile = nofile().unwrap_or(65_536);
    for c in ctrs {
        remove(c.name);
        let ok = Cmd::pinned(CPUS, "podman")
            .args([
                "run",
                "-d",
                "--name",
                c.name,
                "--network",
                "host",
                "--ulimit",
            ])
            .arg(format!("nofile={nofile}:{nofile}"))
            .args(&c.args)
            .arg(c.image)
            .args(&c.cmd)
            .command()
            .stdout(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        if !ok {
            return Err(format!("{} start failed", c.name));
        }
        running.containers.push(c.name.to_owned());
        wait_port(c.port, START).map_err(|e| format!("{}: {e}", c.name))?;
        let pid = Command::new("podman")
            .args(["inspect", "-f", "{{.State.Pid}}", c.name])
            .output()
            .ok()
            .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok());
        running
            .pids
            .push(pid.ok_or_else(|| format!("no PID for container {}", c.name))?);
    }
    thread::sleep(settle);
    Ok(running)
}

/// An empty directory for the store of `name`, in `zzq-suite` under
/// `TMPDIR`, which should be on the disk under test.
fn store(name: &str) -> Result<PathBuf, String> {
    let dir = std::env::temp_dir().join("zzq-suite").join(name);
    if let Err(e) = fs::remove_dir_all(&dir)
        && e.kind() != io::ErrorKind::NotFound
    {
        // A container wrote it as another user of its user namespace.
        let _ = Command::new("podman")
            .args(["unshare", "rm", "-rf"])
            .arg(&dir)
            .status();
    }
    fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    Ok(dir)
}

/// `conf` copied into the empty store of `name`, with `@STORE@` replaced by
/// the store's path.
fn in_store(name: &str, conf: &Path) -> Result<PathBuf, String> {
    let dir = store(name)?;
    let text = fs::read_to_string(conf).map_err(|e| format!("{}: {e}", conf.display()))?;
    let out = dir.join(conf.file_name().unwrap_or_default());
    fs::write(&out, text.replace("@STORE@", &dir.to_string_lossy()))
        .map_err(|e| format!("{}: {e}", out.display()))?;
    Ok(out)
}

/// A volume that mounts `dir` read-write at `at`, owned by the container's
/// user.
fn data(dir: &Path, at: &str) -> [String; 2] {
    ["-v".to_owned(), format!("{}:{at}:Z,U", dir.display())]
}

/// A volume that mounts `conf` read-only at `at`.
fn volume(conf: &Path, at: &str) -> [String; 2] {
    ["-v".to_owned(), format!("{}:{at}:ro,Z", conf.display())]
}

/// The `-e` lines of an env file, without comments.
fn env_args(file: &Path) -> io::Result<Vec<String>> {
    Ok(fs::read_to_string(file)?
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .flat_map(|l| ["-e".to_owned(), l.to_owned()])
        .collect())
}

/// Writes `etc/vernemq.conf` of release `rel` as its original plus `extra`,
/// with its stores in `stores` if given, and empties its data directory
/// (message store and metadata).
fn vernemq_conf(rel: &Path, extra: &Path, stores: Option<&Path>) -> io::Result<()> {
    let etc = rel.join("etc");
    let mut conf = fs::read_to_string(etc.join("vernemq.conf.orig"))?;
    conf.push_str(&fs::read_to_string(extra)?);
    if let Some(dir) = stores {
        for (key, sub) in [
            ("vmq_generic_msg_store.store_dir", "msgstore"),
            ("vmq_swc.directory", "swc_meta"),
            ("vmq_swc.dkm_directory", "swc_dkm"),
        ] {
            conf.extend([key, " = ", &dir.join(sub).to_string_lossy(), "\n"]);
        }
    }
    fs::write(etc.join("vernemq.conf"), conf)?;
    match fs::remove_dir_all(rel.join("data")) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Starts broker `name` with `config` (`default`, `tuned`, `fixed` or
/// `durable`), or zzq with `lanes` IO lanes, after the stores it needs.
///
/// # Errors
///
/// If it cannot start, or a store or the broker does not listen within
/// [`START`].
pub(crate) fn start(name: &str, config: &str, lanes: usize, p: &Paths) -> Result<Running, String> {
    let durable = config == "durable";
    let mut ctrs = Vec::new();
    if durable && matches!(name, "xmq" | "rmqtt") {
        ctrs.push(redis(&["--appendonly", "yes", "--appendfsync", "always"])?);
    }
    let Some(own) = broker_ctrs(name, config, p)? else {
        let stores = containers(&ctrs, Duration::ZERO)?;
        let cmd = broker_cmd(name, config, lanes, p)?;
        return process(stores, &cmd, p, Duration::from_secs(2));
    };
    ctrs.extend(own);
    let settle = if matches!(name, "emqx" | "hivemq") {
        10
    } else {
        2
    };
    let running = containers(&ctrs, Duration::from_secs(settle))?;
    if name == "tbmq" {
        tbmq::open()?;
        tbmq::ready(p)?;
    }
    Ok(running)
}

const XMQ: &str = "docker.io/alexeyparshin/xmq:latest";

/// Redis on 6379 with `args`, its store emptied.
fn redis(args: &[&str]) -> Result<Ctr, String> {
    let mut cmd = vec![
        "redis-server".to_owned(),
        "--save".to_owned(),
        String::new(),
    ];
    cmd.extend(args.iter().map(|&a| a.to_owned()));
    Ok(Ctr {
        name: "zzq-redis",
        image: "docker.io/library/redis:8-alpine",
        args: data(&store("redis")?, "/data").to_vec(),
        cmd,
        port: 6379,
    })
}

/// The containers of a broker that runs in podman, None for one that runs
/// as a process.
fn broker_ctrs(name: &str, config: &str, p: &Paths) -> Result<Option<Vec<Ctr>>, String> {
    let conf = |ext: &str| p.conf(&format!("{name}-{config}.{ext}"));
    let durable = config == "durable";
    let mut args = Vec::new();
    let ctr = match name {
        "emqx" => {
            if config != "default" {
                args = env_args(&conf("env")).map_err(|e| format!("emqx env: {e}"))?;
            }
            if durable {
                args.extend(data(&store("emqx")?, "/opt/emqx/data"));
            }
            Ctr::broker("zzq-emqx", "docker.io/emqx/emqx:6.3.2", args)
        }
        "mosquitto" => {
            args.extend(volume(&conf("conf"), "/mosquitto/config/mosquitto.conf"));
            if durable {
                args.extend(data(&store("mosquitto")?, "/mosquitto/data"));
            }
            Ctr::broker(
                "zzq-mosquitto",
                "docker.io/library/eclipse-mosquitto:2",
                args,
            )
        }
        "nanomq" => {
            args.extend(volume(&conf("conf"), "/etc/nanomq.conf"));
            // Only the full image is built with SQLite.
            let image = if durable {
                args.extend(data(&store("nanomq")?, "/data"));
                "docker.io/emqx/nanomq:0.25.7-full"
            } else {
                "docker.io/emqx/nanomq:latest"
            };
            Ctr::broker("zzq-nanomq", image, args)
        }
        "xmq" => xmq(config, &conf("json")),
        "hivemq" => {
            // The heap TBMQ gets too; JMX would listen on the host.
            for e in [
                "JAVA_OPTS=-XX:+UnlockExperimentalVMOptions -XX:+UseNUMA -Xms8g -Xmx8g",
                "HIVEMQ_JMX_ENABLED=false",
            ] {
                args.extend(["-e".to_owned(), e.to_owned()]);
            }
            args.extend(volume(&conf("xml"), "/opt/hivemq/conf/config.xml"));
            args.extend(data(&store("hivemq")?, "/opt/hivemq/data"));
            Ctr::broker("zzq-hivemq", "docker.io/hivemq/hivemq-ce:2026.5", args)
        }
        "tbmq" => return tbmq::ctrs(durable, &conf("env")).map(Some),
        _ => return Ok(None),
    };
    Ok(Some(vec![ctr]))
}

/// XMQ, whose entrypoint edits its config in place, so it gets a copy, and
/// sets persistence from the environment.
fn xmq(config: &str, json: &Path) -> Ctr {
    let mut args = Vec::new();
    if config != "default" {
        args.extend(["-e".to_owned(), "XMQ_LOG_LEVEL=WARNING".to_owned()]);
        args.extend(volume(json, "/conf/xmq.json"));
        args.extend(["--entrypoint".to_owned(), "sh".to_owned()]);
    }
    if config == "durable" {
        for e in [
            "XMQ_PERSISTENCE=true",
            "XMQ_REDIS_URI=redis://127.0.0.1:6379",
            "XMQ_CLEAN_START=true",
        ] {
            args.extend(["-e".to_owned(), e.to_owned()]);
        }
    }
    let mut ctr = Ctr::broker("zzq-xmq", XMQ, args);
    if config != "default" {
        ctr.cmd = [
            "-c",
            "cp /conf/xmq.json /etc/xmq/xmq_server.conf && exec xmq-entrypoint.sh",
        ]
        .map(String::from)
        .to_vec();
    }
    ctr
}

/// The command of a broker that runs as a process, after preparing its
/// state.
fn broker_cmd(name: &str, config: &str, lanes: usize, p: &Paths) -> Result<Cmd, String> {
    let conf = |ext: &str| p.conf(&format!("{name}-{config}.{ext}"));
    let rival = |bin: &str| p.rival(bin).map(|b| Cmd::pinned(CPUS, b));
    let durable = config == "durable";
    Ok(match name {
        "zzq" => {
            let cmd = Cmd::pinned(CPUS, p.tool("zzq_broker"))
                .arg("--lanes")
                .arg(lanes.to_string())
                .args(["--bind", "tcp://0.0.0.0:1883"]);
            if durable {
                cmd.arg("--data-dir").arg(store("zzq")?)
            } else {
                cmd
            }
        }
        "rmqtt" => {
            let dir = if durable {
                rmqtt_dir(p)?
            } else {
                p.rival("rmqtt")?
            };
            rival("bin/rmqttd")?.arg("-f").arg(conf("toml")).dir(dir)
        }
        "rumqttd" => rival("rumqttd/bin/rumqttd")?
            .args(["-q", "-c"])
            .arg(conf("toml")),
        "flashmq" => rival("flashmq/bin/flashmq")?
            .arg("--config-file")
            .arg(conf("conf")),
        "saguin" => {
            // Its database path must be absolute.
            let yaml = if durable {
                in_store("saguin", &conf("yaml"))?
            } else {
                conf("yaml")
            };
            rival("saguin/bin/saguin")?.arg("-config").arg(yaml)
        }
        "vernemq" => {
            // Built from source: the official images and packages come
            // under an EULA that forbids competitive analysis.
            let rel = p.rival("vernemq-src/_build/default/rel/vernemq")?;
            let stores = if durable {
                Some(store("vernemq")?)
            } else {
                None
            };
            vernemq_conf(&rel, &conf("conf"), stores.as_deref())
                .map_err(|e| format!("vernemq.conf: {e}"))?;
            Cmd::pinned(CPUS, rel.join("bin/vernemq")).args(["console", "-noshell", "-noinput"])
        }
        "fluxmq" => {
            // Its config keeps Badger in `fluxmq-data` under the working
            // directory; each start gets an empty one.
            rival("fluxmq/bin/fluxmq")?
                .arg("-config")
                .arg(conf("yaml"))
                .dir(store("fluxmq")?)
        }
        _ => return Err(format!("unknown broker {name}")),
    })
}

/// A store for rmqtt to run in: the plugin configs of its source tree, those
/// of `rmqtt-durable-plugins` over them, and its TLS files.
fn rmqtt_dir(p: &Paths) -> Result<PathBuf, String> {
    let dir = store("rmqtt")?;
    let src = p.rival("rmqtt")?;
    let copy = |from: &Path, to: &Path, only_toml: bool| -> io::Result<()> {
        fs::create_dir_all(to)?;
        for e in fs::read_dir(from)? {
            let path = e?.path();
            let toml = path.extension().is_some_and(|x| x == "toml");
            if path.is_file() && (toml || !only_toml) {
                fs::copy(&path, to.join(path.file_name().unwrap_or_default()))?;
            }
        }
        Ok(())
    };
    let plugins = dir.join("rmqtt-plugins");
    copy(&src.join("rmqtt-plugins"), &plugins, true)
        .and_then(|()| copy(&p.conf("rmqtt-durable-plugins"), &plugins, true))
        .and_then(|()| copy(&src.join("rmqtt-bin"), &dir.join("rmqtt-bin"), false))
        .map_err(|e| format!("rmqtt plugins: {e}"))?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    #[test]
    fn ports_are_waited_for_both_ways() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        assert!(wait_port(port, Duration::from_secs(5)).is_ok());
        assert!(port_free(port, Duration::ZERO).is_err());
        drop(l);
        assert!(port_free(port, Duration::from_secs(5)).is_ok());
        assert!(wait_port(port, Duration::ZERO).is_err());
    }

    #[test]
    fn a_running_broker_is_stopped_on_drop() {
        let child = Cmd::new("sh")
            .args(["-c", "sleep 30"])
            .command()
            .spawn()
            .unwrap();
        let pid = child.id();
        let free = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let running = Running {
            pids: vec![pid, 7],
            child: Some(child),
            containers: Vec::new(),
            port: free,
        };
        assert_eq!(running.cpu_pids(), format!("{pid},7"));
        drop(running);
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }

    #[test]
    fn configs_and_limits_are_read() {
        assert!(nofile().is_some_and(|n| n > 0));
        let p = Paths::from_env();
        let env = env_args(&p.conf("emqx-tuned.env")).unwrap();
        assert_eq!(env[0], "-e");
        assert!(env[1].starts_with("EMQX_"));
        assert_eq!(volume(Path::new("/c"), "/etc/x")[1], "/c:/etc/x:ro,Z");
        assert_eq!(version("emqx"), Some("6.3.2"));
        assert!(start("nope", "tuned", 1, &p).is_err());
    }

    #[test]
    fn each_process_broker_gets_its_binary_and_config() {
        let mut p = Paths::from_env();
        p.rivals = Some("/r".into());
        let cmd = |name: &str, config: &str| broker_cmd(name, config, 2, &p).map(|c| c.to_string());
        let zzq = cmd("zzq", "").unwrap();
        assert!(zzq.starts_with("taskset -c 0-3 ") && zzq.contains("zzq_broker --lanes 2 --bind"));
        assert!(cmd("rmqtt", "tuned").unwrap().contains("/r/bin/rmqttd -f "));
        assert!(
            cmd("rumqttd", "default")
                .unwrap()
                .ends_with("rumqttd-default.toml")
        );
        assert!(
            cmd("flashmq", "tuned")
                .unwrap()
                .ends_with("flashmq-tuned.conf")
        );
        assert!(
            cmd("saguin", "tuned")
                .unwrap()
                .ends_with("saguin-tuned.yaml")
        );
        assert!(
            cmd("fluxmq", "fixed")
                .unwrap()
                .ends_with("fluxmq-fixed.yaml")
        );
        assert!(
            cmd("vernemq", "tuned")
                .unwrap_err()
                .starts_with("vernemq.conf")
        );
        assert!(cmd("nope", "tuned").is_err());
        p.rivals = None;
        assert!(broker_cmd("rmqtt", "tuned", 1, &p).is_err());
    }

    #[test]
    fn vernemq_gets_its_config_and_an_empty_store() {
        let rel = std::env::temp_dir().join(format!("zzq-suite-verne-{}", std::process::id()));
        fs::create_dir_all(rel.join("etc")).unwrap();
        fs::create_dir_all(rel.join("data/msgstore")).unwrap();
        fs::write(rel.join("etc/vernemq.conf.orig"), "a = 1\n").unwrap();
        let extra = rel.join("extra.conf");
        fs::write(&extra, "b = 2\n").unwrap();
        vernemq_conf(&rel, &extra, None).unwrap();
        assert_eq!(
            fs::read_to_string(rel.join("etc/vernemq.conf")).unwrap(),
            "a = 1\nb = 2\n"
        );
        assert!(!rel.join("data").exists());
        vernemq_conf(&rel, &extra, Some(Path::new("/s"))).unwrap();
        let conf = fs::read_to_string(rel.join("etc/vernemq.conf")).unwrap();
        assert!(
            conf.ends_with("vmq_swc.dkm_directory = /s/swc_dkm\n"),
            "{conf}"
        );
        fs::remove_dir_all(rel).unwrap();
    }
}

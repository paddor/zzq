//! Starts and stops the brokers under test, on CPUs 0-3 and port 1883.

use std::fs::{self, OpenOptions};
use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::Path;
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
pub(crate) const VERSIONS: [(&str, &str); 13] = [
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
    /// The broker process, on the host, for CPU and memory.
    pub(crate) pid: u32,
    handle: Handle,
    port: u16,
}

#[derive(Debug)]
enum Handle {
    Process(Child),
    Container(String),
}

impl Drop for Running {
    fn drop(&mut self) {
        match &mut self.handle {
            Handle::Process(child) => stop(child),
            Handle::Container(name) => remove(name),
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

/// Starts `cmd` pinned to the broker CPUs, waits for its port, then lets it
/// settle for `settle`.
fn process(cmd: &Cmd, p: &Paths, settle: Duration) -> Result<Running, String> {
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
    let running = Running {
        pid: child.id(),
        handle: Handle::Process(child),
        port: PORT,
    };
    wait_port(PORT, Duration::from_secs(120))?;
    thread::sleep(settle);
    Ok(running)
}

/// Starts container `name` from `image` with host networking.
fn container(name: &str, image: &str, args: &[String], cmd: &[&str]) -> Result<Running, String> {
    remove(name);
    let nofile = nofile().unwrap_or(65_536);
    let run = Cmd::pinned(CPUS, "podman")
        .args(["run", "-d", "--name", name, "--network", "host", "--ulimit"])
        .arg(format!("nofile={nofile}:{nofile}"))
        .args(args)
        .arg(image)
        .args(cmd);
    let ok = run
        .command()
        .stdout(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if !ok {
        return Err(format!("{name} start failed"));
    }
    let mut running = Running {
        pid: 0,
        handle: Handle::Container(name.to_owned()),
        port: PORT,
    };
    wait_port(PORT, Duration::from_secs(120))?;
    let settle = if name == "zzq-emqx" { 10 } else { 2 };
    thread::sleep(Duration::from_secs(settle));
    let pid = Command::new("podman")
        .args(["inspect", "-f", "{{.State.Pid}}", name])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok());
    running.pid = pid.ok_or_else(|| format!("no PID for container {name}"))?;
    Ok(running)
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
/// and empties its data directory (message store and metadata).
fn vernemq_conf(rel: &Path, extra: &Path) -> io::Result<()> {
    let etc = rel.join("etc");
    let mut conf = fs::read_to_string(etc.join("vernemq.conf.orig"))?;
    conf.push_str(&fs::read_to_string(extra)?);
    fs::write(etc.join("vernemq.conf"), conf)?;
    match fs::remove_dir_all(rel.join("data")) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Starts broker `name` with `config` (`default`, `tuned` or `fixed`), or
/// zzq with `lanes` IO lanes.
///
/// # Errors
///
/// If it cannot start, or nothing listens on 1883 within 2 minutes.
pub(crate) fn start(name: &str, config: &str, lanes: usize, p: &Paths) -> Result<Running, String> {
    let conf = |ext: &str| p.conf(&format!("{name}-{config}.{ext}"));
    match name {
        "emqx" => {
            let args = if config == "tuned" {
                env_args(&conf("env")).map_err(|e| format!("emqx env: {e}"))?
            } else {
                Vec::new()
            };
            container("zzq-emqx", "docker.io/emqx/emqx:6.3.2", &args, &[])
        }
        "mosquitto" => container(
            "zzq-mosquitto",
            "docker.io/library/eclipse-mosquitto:2",
            &volume(&conf("conf"), "/mosquitto/config/mosquitto.conf"),
            &[],
        ),
        "nanomq" => container(
            "zzq-nanomq",
            "docker.io/emqx/nanomq:latest",
            &volume(&conf("conf"), "/etc/nanomq.conf"),
            &[],
        ),
        "xmq" if config == "tuned" => {
            // The entrypoint edits the config in place, so it gets a copy.
            let mut args = vec!["-e".to_owned(), "XMQ_LOG_LEVEL=WARNING".to_owned()];
            args.extend(volume(&conf("json"), "/conf/xmq.json"));
            args.extend(["--entrypoint".to_owned(), "sh".to_owned()]);
            let cmd = [
                "-c",
                "cp /conf/xmq.json /etc/xmq/xmq_server.conf && exec xmq-entrypoint.sh",
            ];
            container("zzq-xmq", XMQ, &args, &cmd)
        }
        "xmq" => container("zzq-xmq", XMQ, &[], &[]),
        _ => process(
            &broker_cmd(name, config, lanes, p)?,
            p,
            Duration::from_secs(2),
        ),
    }
}

const XMQ: &str = "docker.io/alexeyparshin/xmq:latest";

/// The command of a broker that runs as a process, after preparing its
/// state.
fn broker_cmd(name: &str, config: &str, lanes: usize, p: &Paths) -> Result<Cmd, String> {
    let conf = |ext: &str| p.conf(&format!("{name}-{config}.{ext}"));
    let rival = |bin: &str| p.rival(bin).map(|b| Cmd::pinned(CPUS, b));
    Ok(match name {
        "zzq" => Cmd::pinned(CPUS, p.tool("zzq_broker"))
            .arg("--lanes")
            .arg(lanes.to_string())
            .args(["--bind", "tcp://0.0.0.0:1883"]),
        "rmqtt" => rival("bin/rmqttd")?
            .arg("-f")
            .arg(conf("toml"))
            .dir(p.rival("rmqtt")?),
        "rumqttd" => rival("rumqttd/bin/rumqttd")?
            .args(["-q", "-c"])
            .arg(conf("toml")),
        "flashmq" => rival("flashmq/bin/flashmq")?
            .arg("--config-file")
            .arg(conf("conf")),
        "saguin" => rival("saguin/bin/saguin")?.arg("-config").arg(conf("yaml")),
        "vernemq" => {
            // Built from source: the official images and packages come
            // under an EULA that forbids competitive analysis.
            let rel = p.rival("vernemq-src/_build/default/rel/vernemq")?;
            vernemq_conf(&rel, &conf("conf")).map_err(|e| format!("vernemq.conf: {e}"))?;
            Cmd::pinned(CPUS, rel.join("bin/vernemq")).args(["console", "-noshell", "-noinput"])
        }
        "fluxmq" => {
            // Its config keeps Badger in `fluxmq-data` under the working
            // directory; each start gets an empty one.
            let dir = std::env::temp_dir().join("zzq-suite");
            let _ = fs::remove_dir_all(dir.join("fluxmq-data"));
            fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
            rival("fluxmq/bin/fluxmq")?
                .arg("-config")
                .arg(conf("yaml"))
                .dir(dir)
        }
        _ => return Err(format!("unknown broker {name}")),
    })
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
        drop(Running {
            pid,
            handle: Handle::Process(child),
            port: free,
        });
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
        vernemq_conf(&rel, &extra).unwrap();
        assert_eq!(
            fs::read_to_string(rel.join("etc/vernemq.conf")).unwrap(),
            "a = 1\nb = 2\n"
        );
        assert!(!rel.join("data").exists());
        vernemq_conf(&rel, &extra).unwrap();
        fs::remove_dir_all(rel).unwrap();
    }
}

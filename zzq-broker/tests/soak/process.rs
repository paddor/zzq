use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use super::resources::Pids;

pub(super) fn address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

pub(super) struct Node {
    pub addr: SocketAddr,
    args: Vec<String>,
    log: PathBuf,
    child: Option<Child>,
    watchdog: Option<Child>,
    pids: Pids,
    log_start: usize,
}

impl Node {
    pub(super) fn new(
        addr: SocketAddr,
        args: Vec<String>,
        dir: &Path,
        index: usize,
        pids: Pids,
    ) -> Self {
        Self {
            addr,
            args,
            log: dir.join(format!("node-{index}.log")),
            child: None,
            watchdog: None,
            pids,
            log_start: 0,
        }
    }

    pub(super) fn start(&mut self, bootstrap: bool) {
        assert!(self.child.is_none());
        self.log_start = std::fs::metadata(&self.log).map_or(0, |m| m.len() as usize);
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log)
            .unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_zzq_broker"));
        command
            .args(["--lanes", "2", "--max-state-mib", "128", "--bind"])
            .arg(format!("tcp://{}", self.addr))
            .args(&self.args)
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        if bootstrap {
            command.arg("--cluster-bootstrap");
        }
        let child = command.spawn().unwrap();
        let pid = child.id();
        self.pids.lock().unwrap().push(pid);
        self.child = Some(child);
        // A nextest timeout or watchdog exit skips Drop. This process then reaps
        // only this broker. Null stdio cannot keep nextest's output pipes open.
        self.watchdog = Some(Command::new("bash")
            .args(["-c", "while kill -0 \"$1\" 2>/dev/null; do sleep 1; done; kill -KILL \"$2\" 2>/dev/null", "soak-watchdog"])
            .arg(std::process::id().to_string()).arg(pid.to_string())
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
    }

    pub(super) fn signal(&self, signal: &str) {
        let pid = self.child.as_ref().unwrap().id();
        assert!(
            Command::new("kill")
                .arg(signal)
                .arg(pid.to_string())
                .status()
                .unwrap()
                .success()
        );
    }

    pub(super) async fn settled(&self) {
        let start = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            let log = std::fs::read_to_string(&self.log).unwrap();
            if log[self.log_start..]
                .lines()
                .rev()
                .find(|s| s.starts_with("cluster node "))
                .is_some_and(|s| s.ends_with("follows") || s.ends_with("leads"))
            {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "node did not settle: {log}"
            );
            tick.tick().await;
        }
    }

    pub(super) async fn stop(&mut self, clean: bool) {
        self.signal(if clean { "-TERM" } else { "-KILL" });
        let start = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        loop {
            if let Some(status) = self.child.as_mut().unwrap().try_wait().unwrap() {
                assert!(!clean || status.success(), "broker clean exit: {status}");
                break;
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "broker did not stop"
            );
            tick.tick().await;
        }
        self.remove();
    }

    fn remove(&mut self) {
        if let Some(child) = self.child.take() {
            self.pids.lock().unwrap().retain(|&pid| pid != child.id());
        }
        if let Some(mut watchdog) = self.watchdog.take() {
            let _ = watchdog.kill();
            let _ = watchdog.wait();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.remove();
    }
}

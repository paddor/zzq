//! Broker child processes end with their guard, on every exit path.

#![cfg(target_os = "linux")]
#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use std::io;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use zzq_bench::child;

fn sh(script: &str) -> Command {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(script);
    cmd
}

fn reaped(pid: u32) -> bool {
    !Path::new(&format!("/proc/{pid}")).exists()
}

/// A file the child writes its PID to, for a failed start that returns
/// no guard.
struct PidFile(PathBuf);

impl PidFile {
    fn new(name: &str) -> PidFile {
        let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("zzq-child-{name}-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        PidFile(path)
    }

    /// Writes the PID, then runs `rest`; `exec` keeps the PID.
    fn script(&self, rest: &str) -> String {
        format!("echo $$ > {}; {rest}", self.0.display())
    }

    fn pid(&self) -> u32 {
        std::fs::read_to_string(&self.0)
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[test]
fn a_broker_child_is_reaped_when_a_tool_fails_early() {
    let mut pid = 0;
    let mut measure = || -> io::Result<()> {
        let broker = child::spawn(sh("echo tcp://127.0.0.1:1; exec sleep 30"))?;
        assert_eq!(broker.addr(), "127.0.0.1:1");
        pid = broker.id();
        Err(io::Error::other("the load failed"))
    };
    assert!(measure().is_err());
    assert!(reaped(pid), "broker {pid} still runs");
}

#[test]
fn a_broker_child_is_reaped_after_a_panic() {
    let mut pid = 0;
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let broker = child::spawn(sh("echo tcp://127.0.0.1:1; exec sleep 30")).unwrap();
        pid = broker.id();
        panic!("a failed assert");
    }));
    assert!(panicked.is_err());
    assert!(reaped(pid), "broker {pid} still runs");
}

#[test]
fn a_broker_child_that_prints_no_endpoint_is_reaped() {
    let file = PidFile::new("no-endpoint");
    let err = child::spawn(sh(&file.script("echo nope; exec sleep 30"))).unwrap_err();
    assert_eq!(err.to_string(), "broker printed no endpoint");
    assert!(reaped(file.pid()));
}

#[test]
fn a_broker_child_on_a_given_address_starts_once_it_listens() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let broker = child::spawn_on(sh("exec sleep 30"), &addr, Duration::from_secs(5)).unwrap();
    assert_eq!(broker.addr(), addr);
    let pid = broker.id();
    drop(broker);
    assert!(reaped(pid), "broker {pid} still runs");
}

#[test]
fn a_broker_child_on_an_address_nobody_listens_on_is_reaped() {
    // Bound but not listening: connects are refused, and no other test
    // gets the port meanwhile.
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = socket.local_addr().unwrap().to_string();
    let file = PidFile::new("nobody-listens");
    let cmd = sh(&file.script("exec sleep 30"));
    let err = child::spawn_on(cmd, &addr, Duration::from_secs(1)).unwrap_err();
    assert_eq!(err.to_string(), format!("nothing listens on {addr}"));
    assert!(reaped(file.pid()));
}

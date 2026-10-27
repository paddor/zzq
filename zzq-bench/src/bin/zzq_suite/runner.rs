//! Runs benchmark commands one run at a time on a quiet system.

use std::ffi::OsString;
use std::fmt;
use std::io::{self, BufRead, BufReader};
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

/// Processes whose presence makes a run unreliable.
const COMPILERS: [&str; 9] = [
    "rustc",
    "cargo",
    "clippy-driver",
    "cc1",
    "cc1plus",
    "ld",
    "lld",
    "mold",
    "beam.smp-build",
];
/// Idle CPU share a run waits for.
const IDLE: f64 = 0.90;
/// Clock ticks per second in `/proc` times.
const USER_HZ: f64 = 100.0;
/// Time after a measured command exits that the others get to finish.
const GRACE: Duration = Duration::from_secs(2);

/// A command line.
#[derive(Debug, Clone)]
pub(crate) struct Cmd {
    argv: Vec<OsString>,
    dir: Option<PathBuf>,
}

impl Cmd {
    pub(crate) fn new(program: impl Into<OsString>) -> Cmd {
        Cmd {
            argv: vec![program.into()],
            dir: None,
        }
    }

    /// `program` under `taskset -c cpus`.
    pub(crate) fn pinned(cpus: &str, program: impl Into<OsString>) -> Cmd {
        Cmd::new("taskset").arg("-c").arg(cpus).arg(program)
    }

    pub(crate) fn arg(mut self, a: impl Into<OsString>) -> Cmd {
        self.argv.push(a.into());
        self
    }

    pub(crate) fn args<A: Into<OsString>>(mut self, args: impl IntoIterator<Item = A>) -> Cmd {
        self.argv.extend(args.into_iter().map(Into::into));
        self
    }

    /// Runs in `dir`.
    pub(crate) fn dir(mut self, dir: PathBuf) -> Cmd {
        self.dir = Some(dir);
        self
    }

    /// A [`Command`] in its own process group, so a stop reaches its
    /// children too.
    pub(crate) fn command(&self) -> Command {
        let mut c = Command::new(&self.argv[0]);
        c.args(&self.argv[1..]).process_group(0);
        if let Some(dir) = &self.dir {
            c.current_dir(dir);
        }
        c
    }
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let words: Vec<_> = self.argv.iter().map(|a| a.to_string_lossy()).collect();
        f.write_str(&words.join(" "))
    }
}

/// Sends `signal` to the process group `pid` leads.
pub(crate) fn signal_group(pid: u32, signal: &str) {
    let _ = Command::new("kill")
        .args(["-s", signal, "--", &format!("-{pid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Waits up to `limit` for `child` to exit. Returns whether it did.
pub(crate) fn wait_exit(child: &mut Child, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if matches!(child.try_wait(), Ok(Some(_)) | Err(_)) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Compiler processes running now.
fn compilers() -> Vec<String> {
    let mut found = Vec::new();
    for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
        if let Ok(comm) = std::fs::read_to_string(e.path().join("comm")) {
            let comm = comm.trim();
            if COMPILERS.contains(&comm) {
                found.push(comm.to_owned());
            }
        }
    }
    found
}

/// Idle and total ticks from the first line of `/proc/stat`.
fn cpu_sample(stat: &str) -> Option<(u64, u64)> {
    let f: Vec<u64> = stat
        .lines()
        .next()?
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    Some((f.get(3)? + f.get(4)?, f.iter().sum()))
}

fn idle_fraction(over: Duration) -> f64 {
    let read = || cpu_sample(&std::fs::read_to_string("/proc/stat").unwrap_or_default());
    let a = read();
    thread::sleep(over);
    match (a, read()) {
        (Some((i0, t0)), Some((i1, t1))) if t1 > t0 => (i1 - i0) as f64 / (t1 - t0) as f64,
        _ => 1.0,
    }
}

/// Waits until no compiler runs and the CPU is at least 90% idle.
fn wait_quiet() {
    let mut said: Option<Instant> = None;
    loop {
        let busy = compilers();
        let idle = idle_fraction(Duration::from_secs(2));
        if busy.is_empty() && idle >= IDLE {
            return;
        }
        if said.is_none_or(|t| t.elapsed() > Duration::from_secs(30)) {
            eprintln!(
                "  waiting for quiet system: idle {:.0}% compilers {}",
                idle * 100.0,
                busy.join(",")
            );
            said = Some(Instant::now());
        }
        thread::sleep(Duration::from_secs(3));
    }
}

/// CPU ticks of process `pid` itself, without its children.
fn own_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, rest) = stat.rsplit_once(") ")?;
    let f: Vec<&str> = rest.split(' ').collect();
    Some(f.get(11)?.parse::<u64>().ok()? + f.get(12)?.parse::<u64>().ok()?)
}

/// Why the output says the run is not data.
pub(crate) fn stop_word(out: &str) -> Option<&'static str> {
    // A stuck publisher is broker overload: the rate stands, the loss
    // does not.
    let warned = out
        .match_indices("warning:")
        .any(|(i, w)| !out[i + w.len()..].starts_with(" publisher stuck"));
    if warned {
        return Some("harness warning");
    }
    ["panicked", "Crash dump", "exception error"]
        .into_iter()
        .find(|w| out.contains(w))
}

/// What a run printed, and each command's CPU in percent of one core.
#[derive(Debug)]
pub(crate) struct Ran {
    pub(crate) out: String,
    pub(crate) cpu: Vec<Option<u32>>,
}

/// Started commands, stopped on drop.
struct Group(Vec<(Child, Instant)>);

impl Drop for Group {
    fn drop(&mut self) {
        for (child, _) in &mut self.0 {
            signal_group(child.id(), "KILL");
            let _ = child.wait();
        }
    }
}

/// Runs one benchmark: `cmds` start in order, 0.3 s apart, and the run ends
/// when `cmds[measure]` exits. The others then get [`GRACE`] and are
/// killed. With `quiet`, each try waits for a quiet system first, and a
/// try that a compiler overlapped runs again, up to 3 times.
///
/// # Errors
///
/// If a command cannot start, the measured one runs past `limit`, the
/// output has a harness warning or a crash, or the system never stays
/// quiet.
pub(crate) fn bench(
    cmds: &[Cmd],
    measure: usize,
    limit: Duration,
    quiet: bool,
) -> Result<Ran, String> {
    for attempt in 1..=3 {
        if quiet {
            wait_quiet();
        }
        let seen = Mutex::new(Vec::new());
        let ran = attempt_once(cmds, measure, limit, quiet.then_some(&seen))?;
        if let Some(why) = stop_word(&ran.out) {
            return Err(format!("{why} in: {}", cmds[measure]));
        }
        let seen = seen.into_inner().unwrap_or_else(PoisonError::into_inner);
        if seen.is_empty() {
            return Ok(ran);
        }
        println!("  CONTAMINATED by {}, rerun {attempt}", seen.join(","));
    }
    Err("system never stayed quiet".to_owned())
}

/// One try of [`bench`]. Collects the compilers it sees into `seen`.
fn attempt_once(
    cmds: &[Cmd],
    measure: usize,
    limit: Duration,
    seen: Option<&Mutex<Vec<String>>>,
) -> Result<Ran, String> {
    let out = Mutex::new(String::new());
    let stop = AtomicBool::new(false);
    let mut last = vec![(0, None); cmds.len()];
    let done = thread::scope(|sc| {
        if let Some(seen) = seen {
            sc.spawn(|| {
                while !stop.load(Relaxed) {
                    let found = compilers();
                    seen.lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .extend(found);
                    thread::sleep(Duration::from_millis(500));
                }
            });
        }
        let result = run_all(cmds, measure, limit, (&out, &mut last), sc);
        stop.store(true, Relaxed);
        result
    });
    let cpu = last
        .iter()
        .map(|&(ticks, span): &(u64, Option<Duration>)| {
            span.filter(|s| !s.is_zero())
                .map(|s| (ticks as f64 / USER_HZ / s.as_secs_f64() * 100.0).round() as u32)
        })
        .collect();
    let out = out.into_inner().unwrap_or_else(PoisonError::into_inner);
    match done {
        Ok(true) => Ok(Ran { out, cpu }),
        Ok(false) => Err(format!(
            "TIMEOUT after {}s: {}",
            limit.as_secs(),
            cmds[measure]
        )),
        Err(e) => Err(e),
    }
}

/// Starts `cmds`, relays their output into `out` and samples their CPU into
/// `last` as (ticks, time since start). Returns whether the measured one
/// exited within `limit`.
fn run_all<'s>(
    cmds: &[Cmd],
    measure: usize,
    limit: Duration,
    (out, last): (&'s Mutex<String>, &mut [(u64, Option<Duration>)]),
    sc: &'s thread::Scope<'s, '_>,
) -> Result<bool, String> {
    let mut group = Group(Vec::new());
    for (i, c) in cmds.iter().enumerate() {
        if i > 0 {
            thread::sleep(Duration::from_millis(300));
        }
        let child = spawn_relayed(c, out, sc).map_err(|e| format!("{c}: {e}"))?;
        group.0.push((child, Instant::now()));
    }
    let exited = group.wait(&[measure], limit, last);
    let rest: Vec<usize> = (0..cmds.len()).filter(|&i| i != measure).collect();
    group.wait(&rest, GRACE, last);
    drop(group);
    Ok(exited)
}

impl Group {
    /// Samples CPU every 20 ms until the commands at `which` exited or
    /// `limit` passed. Returns whether they exited.
    fn wait(
        &mut self,
        which: &[usize],
        limit: Duration,
        last: &mut [(u64, Option<Duration>)],
    ) -> bool {
        let deadline = Instant::now() + limit;
        loop {
            for (i, (child, from)) in self.0.iter().enumerate() {
                if let Some(t) = own_ticks(child.id()) {
                    last[i] = (t, Some(from.elapsed()));
                }
            }
            let done = which.iter().all(|&i| {
                let child = &mut self.0[i].0;
                matches!(child.try_wait(), Ok(Some(_)) | Err(_))
            });
            if done {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Starts `c` with stdout and stderr on one pipe, which a thread copies to
/// our stdout and into `out` line by line.
fn spawn_relayed<'s>(
    c: &Cmd,
    out: &'s Mutex<String>,
    sc: &'s thread::Scope<'s, '_>,
) -> io::Result<Child> {
    let (reader, writer) = io::pipe()?;
    let child = {
        let mut cmd = c.command();
        cmd.stdin(Stdio::null())
            .stdout(writer.try_clone()?)
            .stderr(writer);
        cmd.spawn()?
    };
    sc.spawn(move || {
        let mut r = BufReader::new(reader);
        let mut line = Vec::new();
        while matches!(r.read_until(b'\n', &mut line), Ok(n) if n > 0) {
            let text = String::from_utf8_lossy(&line);
            print!("{text}");
            out.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_str(&text);
            line.clear();
        }
    });
    Ok(child)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sh(script: &str) -> Cmd {
        Cmd::new("sh").arg("-c").arg(script)
    }

    #[test]
    fn a_run_collects_output_and_cpu_and_ends_with_the_measured_command() {
        let started = Instant::now();
        let cmds = [
            sh("echo side; sleep 30"),
            sh("echo main; i=0; while [ $i -lt 20000 ]; do i=$((i+1)); done"),
        ];
        let ran = bench(&cmds, 1, Duration::from_secs(20), false).unwrap();
        assert!(
            ran.out.contains("side\n") && ran.out.contains("main\n"),
            "{ran:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(ran.cpu.len(), 2);
        assert!(ran.cpu[1].is_some(), "{ran:?}");
        assert_eq!(cmds[0].to_string(), "sh -c echo side; sleep 30");
    }

    #[test]
    fn a_run_stops_on_timeouts_warnings_and_bad_commands() {
        let slow = bench(&[sh("sleep 30")], 0, Duration::from_millis(300), false);
        assert!(slow.unwrap_err().starts_with("TIMEOUT"));
        let warned = bench(
            &[sh("echo 'warning: odd'")],
            0,
            Duration::from_secs(5),
            false,
        );
        assert!(warned.unwrap_err().starts_with("harness warning"));
        let missing = bench(
            &[Cmd::new("/nonexistent/zzq")],
            0,
            Duration::from_secs(5),
            false,
        );
        assert!(missing.unwrap_err().contains("/nonexistent/zzq"));
        let dir = bench(
            &[sh("pwd").dir("/".into())],
            0,
            Duration::from_secs(5),
            false,
        );
        assert_eq!(dir.unwrap().out, "/\n");
    }

    #[test]
    fn stop_words_spare_a_stuck_publisher() {
        assert_eq!(
            stop_word("warning: publisher stuck, shutting sockets down\n"),
            None
        );
        assert_eq!(
            stop_word("ok\nthread 'main' panicked at x"),
            Some("panicked")
        );
        assert_eq!(stop_word("Crash dump is being written"), Some("Crash dump"));
        assert_eq!(stop_word("all good"), None);
    }

    #[test]
    fn proc_samples_parse() {
        let stat = "cpu  10 0 5 80 5 0 0 0 0 0\ncpu0 1 2 3\n";
        assert_eq!(cpu_sample(stat), Some((85, 100)));
        assert_eq!(cpu_sample(""), None);
        assert!(own_ticks(std::process::id()).is_some());
        assert!((0.0..=1.0).contains(&idle_fraction(Duration::from_millis(50))));
        assert!(compilers().iter().all(|c| COMPILERS.contains(&c.as_str())));
    }

    #[test]
    fn a_quiet_run_samples_compilers_while_it_runs() {
        let seen = Mutex::new(Vec::new());
        let ran = attempt_once(&[sh("sleep 0.6")], 0, Duration::from_secs(5), Some(&seen));
        assert!(ran.is_ok());
        let seen = seen.into_inner().unwrap();
        assert!(seen.iter().all(|c| COMPILERS.contains(&c.as_str())));
    }

    #[test]
    fn a_child_that_will_not_exit_is_reported() {
        let mut c = sh("sleep 30").command().spawn().unwrap();
        assert!(!wait_exit(&mut c, Duration::from_millis(100)));
        signal_group(c.id(), "KILL");
        assert!(wait_exit(&mut c, Duration::from_secs(5)));
    }
}

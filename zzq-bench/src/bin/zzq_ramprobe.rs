//! Broker RSS per offline session, queued message and retained message.
//!
//! Usage: `zzq_ramprobe [key=value ...] [-- <broker command>]`
//!
//! Keys (defaults): v=5 lanes=1 sessions=50000 per=10 fanout=4
//! retained=200000 sizes=100,1000 sys=1 (0 with a command) addr
//!
//! Each step runs on a fresh broker, as memory freed earlier hides what new
//! state costs. The broker is zzq in a child process on `lanes` lanes, or
//! the command, which listens on `addr` or prints its endpoint first, as
//! `zzq_broker` does. Sessions subscribe to their own topic and one they
//! share. `per` messages go to each session's own topic and `fanout` to
//! the shared one, for each payload size. `sys` is the broker's `$SYS`
//! interval in seconds, for its own estimate: 10 for `zzq_broker`, 0 for
//! brokers other than zzq.

use std::collections::HashMap;
use std::io;
use std::net::TcpStream;
use std::process::{Child, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use zzq_bench::child;
use zzq_bench::ram::{Growth, Probe};

struct Args {
    v5: bool,
    lanes: usize,
    sessions: usize,
    per: usize,
    fanout: usize,
    retained: usize,
    sizes: Vec<usize>,
    sys: Option<Duration>,
    addr: Option<String>,
    command: Vec<String>,
}

#[derive(Clone, Copy)]
enum Step {
    Sessions,
    Queued(usize),
    FannedOut(usize),
    Retained(usize),
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [flag, lanes, sys_ms] = args.as_slice()
        && flag == "--broker-child"
    {
        return broker_child(lanes, sys_ms);
    }
    let Some(args) = parse(&args) else {
        eprintln!("usage: zzq_ramprobe [key=value ...] [-- <broker command>]");
        return ExitCode::FAILURE;
    };
    let mut steps = vec![Step::Sessions];
    steps.extend(args.sizes.iter().map(|&s| Step::Queued(s)));
    steps.extend(args.sizes.iter().map(|&s| Step::FannedOut(s)));
    steps.extend(args.sizes.iter().map(|&s| Step::Retained(s)));
    for step in steps {
        if let Err(e) = run(&args, step) {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

fn parse(args: &[String]) -> Option<Args> {
    let (keys, command) = match args.iter().position(|a| a == "--") {
        Some(i) => (&args[..i], args[i + 1..].to_vec()),
        None => (args, Vec::new()),
    };
    let kv = keys
        .iter()
        .map(|a| a.split_once('='))
        .collect::<Option<HashMap<_, _>>>()?;
    let get = |k: &str, d: &'static str| kv.get(k).copied().unwrap_or(d);
    let num = |k: &str, d: &'static str| get(k, d).parse::<usize>().ok();
    let sys = get("sys", if command.is_empty() { "1" } else { "0" })
        .parse::<f64>()
        .ok()?;
    Some(Args {
        v5: get("v", "5") == "5",
        lanes: num("lanes", "1")?.max(1),
        sessions: num("sessions", "50000")?,
        per: num("per", "10")?,
        fanout: num("fanout", "4")?,
        retained: num("retained", "200000")?,
        sizes: get("sizes", "100,1000")
            .split(',')
            .map(|s| s.parse().ok())
            .collect::<Option<_>>()?,
        sys: (sys > 0.0).then(|| Duration::from_secs_f64(sys)),
        addr: kv.get("addr").map(|&a| a.to_owned()),
        command,
    })
}

/// Measures one step on a fresh broker.
fn run(a: &Args, step: Step) -> io::Result<()> {
    let (mut broker, addr) = start(a)?;
    let grown = measure(a, &addr, broker.id(), step);
    let _ = broker.kill();
    let _ = broker.wait();
    let name = match step {
        Step::Sessions => "offline session, 2 subs".to_owned(),
        Step::Queued(size) => format!("queued {size} B"),
        Step::FannedOut(size) => format!("fanned-out entry {size} B"),
        Step::Retained(size) => format!("retained {size} B"),
    };
    print(&name, &grown?);
    Ok(())
}

fn measure(a: &Args, addr: &str, pid: u32, step: Step) -> io::Result<Growth> {
    let mut probe = Probe::new(addr, pid, a.v5, a.sys)?;
    match step {
        Step::Sessions => probe.sessions(a.sessions),
        Step::Queued(size) => {
            probe.sessions(a.sessions)?;
            probe.queued(a.per, size)
        }
        Step::FannedOut(size) => {
            probe.sessions(a.sessions)?;
            probe.fanned_out(a.fanout, size)
        }
        Step::Retained(size) => probe.retained(a.retained, size),
    }
}

/// Starts the broker and returns its `host:port`.
fn start(a: &Args) -> io::Result<(Child, String)> {
    let Some((exe, args)) = a.command.split_first() else {
        let mut cmd = Command::new(std::env::current_exe()?);
        let sys_ms = a.sys.map_or(0, |d| d.as_millis());
        cmd.arg("--broker-child")
            .arg(a.lanes.to_string())
            .arg(sys_ms.to_string());
        return child::spawn(cmd);
    };
    let mut cmd = Command::new(exe);
    cmd.args(args);
    let Some(addr) = &a.addr else {
        return child::spawn(cmd);
    };
    let mut broker = cmd.stdout(Stdio::null()).spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(addr).is_err() {
        if Instant::now() > deadline {
            let _ = broker.kill();
            let _ = broker.wait();
            return Err(io::Error::other(format!("nothing listens on {addr}")));
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok((broker, addr.clone()))
}

fn print(name: &str, g: &Growth) {
    let rss = g.per_item();
    let estimate = g.estimate_per_item().map_or_else(String::new, |e| {
        let ratio = if rss > 0.0 {
            format!("{:.2}", e / rss)
        } else {
            "-".to_owned()
        };
        format!("  estimate {e:>7.1} B  estimate/RSS {ratio}")
    });
    println!("{name:<26} {:>8} x {rss:>7.1} B RSS{estimate}", g.items);
}

fn broker_child(lanes: &str, sys_ms: &str) -> ExitCode {
    let cfg = zzq_broker::Config {
        io_threads: lanes.parse().unwrap_or(1),
        name: "zzq-ramprobe".to_owned(),
        ..zzq_broker::Config::default()
    };
    let sys_ms = sys_ms.parse().unwrap_or(0);
    let opts = zzq_broker::BrokerOptions {
        sys_interval: (sys_ms > 0).then(|| Duration::from_millis(sys_ms)),
        ..zzq_broker::BrokerOptions::default()
    };
    child::serve(&cfg, opts)
}

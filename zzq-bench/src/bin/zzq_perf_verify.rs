//! Local performance gates for the broker.
//!
//! Thresholds are read from `.perf_hw`, which is intentionally ignored.
//! Without that file, this command runs a smaller smoke gate. The broker and
//! the load run in separate pinned child processes.
//!
//! Env: `ZZQ_PERF_MEASURE_MS` (default 3000), `ZZQ_PERF_WARMUP_MS` (1000).

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::Duration;

use zzq_bench::load::{self, Load, Scenario};
use zzq_bench::wire;

/// One measured case.
struct Case {
    key: &'static str,
    kind: Kind,
}

#[derive(Clone, Copy)]
enum Kind {
    Throughput {
        scenario: Scenario,
        qos: u8,
        size: usize,
        pubs: usize,
        subs: usize,
        /// Puts each pair on two lanes.
        cross: bool,
        /// Topics per publisher.
        topics: usize,
    },
    LatencyP50 {
        qos: u8,
        size: usize,
    },
    /// Broker RSS per idle connection with one subscription each, KiB.
    IdleMemory {
        conns: usize,
    },
}

const fn pairs(qos: u8, size: usize, pubs: usize) -> Kind {
    Kind::Throughput {
        scenario: Scenario::Pairs,
        qos,
        size,
        pubs,
        subs: 1,
        cross: false,
        topics: 1,
    }
}

const fn cross(qos: u8, size: usize, pubs: usize) -> Kind {
    Kind::Throughput {
        scenario: Scenario::Pairs,
        qos,
        size,
        pubs,
        subs: 1,
        cross: true,
        topics: 1,
    }
}

const fn fanout(qos: u8, size: usize, subs: usize) -> Kind {
    Kind::Throughput {
        scenario: Scenario::Fanout,
        qos,
        size,
        pubs: 1,
        subs,
        cross: false,
        topics: 1,
    }
}

/// Pairs where each publisher cycles through `topics` topics.
const fn spread(qos: u8, topics: usize) -> Kind {
    Kind::Throughput {
        scenario: Scenario::Pairs,
        qos,
        size: 16,
        pubs: 1,
        subs: 1,
        cross: false,
        topics,
    }
}

const ONE_LANE: &[Case] = &[
    // First, so memory freed by earlier cases does not hide the cost.
    Case {
        key: "idle_conn_kib",
        kind: Kind::IdleMemory { conns: 20_000 },
    },
    Case {
        key: "qos0_16b_msgs_s",
        kind: pairs(0, 16, 1),
    },
    Case {
        key: "qos0_256b_msgs_s",
        kind: pairs(0, 256, 1),
    },
    Case {
        key: "qos0_1k_msgs_s",
        kind: pairs(0, 1024, 1),
    },
    Case {
        key: "qos1_16b_msgs_s",
        kind: pairs(1, 16, 1),
    },
    Case {
        key: "qos2_16b_msgs_s",
        kind: pairs(2, 16, 1),
    },
    Case {
        key: "topics100k_qos0_16b_msgs_s",
        kind: spread(0, 100_000),
    },
    Case {
        key: "topics100k_qos1_16b_msgs_s",
        kind: spread(1, 100_000),
    },
    Case {
        key: "fanout16_qos0_16b_msgs_s",
        kind: fanout(0, 16, 16),
    },
    Case {
        key: "fanout16_qos1_16b_msgs_s",
        kind: fanout(1, 16, 16),
    },
    Case {
        key: "rtt_qos0_16b_p50_us",
        kind: Kind::LatencyP50 { qos: 0, size: 16 },
    },
    Case {
        key: "rtt_qos1_16b_p50_us",
        kind: Kind::LatencyP50 { qos: 1, size: 16 },
    },
];

const TWO_LANES: &[Case] = &[
    Case {
        key: "pairs2_qos0_16b_msgs_s",
        kind: pairs(0, 16, 2),
    },
    Case {
        key: "pairs2_qos1_16b_msgs_s",
        kind: pairs(1, 16, 2),
    },
    Case {
        key: "cross2_qos0_16b_msgs_s",
        kind: cross(0, 16, 2),
    },
    Case {
        key: "cross2_qos1_16b_msgs_s",
        kind: cross(1, 16, 2),
    },
    Case {
        key: "fanout16_qos0_16b_msgs_s",
        kind: fanout(0, 16, 16),
    },
];

const BUSY: &[Case] = &[
    Case {
        key: "rtt_qos0_16b_p50_us",
        kind: Kind::LatencyP50 { qos: 0, size: 16 },
    },
    Case {
        key: "rtt_qos1_16b_p50_us",
        kind: Kind::LatencyP50 { qos: 1, size: 16 },
    },
];

/// One broker configuration and its cases.
struct Section {
    name: &'static str,
    lanes: usize,
    /// Lanes poll without sleeping.
    busy: bool,
    cases: &'static [Case],
}

const SECTIONS: &[Section] = &[
    Section {
        name: "broker_1io",
        lanes: 1,
        busy: false,
        cases: ONE_LANE,
    },
    Section {
        name: "broker_2io",
        lanes: 2,
        busy: false,
        cases: TWO_LANES,
    },
    Section {
        name: "broker_1io_busy",
        lanes: 1,
        busy: true,
        cases: BUSY,
    },
];

/// Thresholds used without `.perf_hw`.
fn smoke(key: &str) -> f64 {
    if key.ends_with("_us") {
        1000.0
    } else if key.ends_with("_kib") {
        64.0
    } else {
        100_000.0
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("--broker-child") => {
            let lanes = args.get(2).and_then(|n| n.parse().ok()).unwrap_or(1);
            return broker_child(lanes, args.get(3).is_some_and(|b| b == "busy"));
        }
        Some("--load-child") => return load_child(&args[2..]),
        _ => {}
    }
    let thresholds = if let Ok(text) = std::fs::read_to_string(".perf_hw") {
        println!("threshold file: .perf_hw");
        Some(parse_ini(&text))
    } else {
        println!(".perf_hw absent: using smoke thresholds");
        None
    };
    let mut failed = 0;
    for sec in SECTIONS {
        let section = sec.name;
        let limits = thresholds.as_ref().and_then(|t| t.get(section));
        if thresholds.is_some() && limits.is_none() {
            continue;
        }
        match run_section(sec, limits) {
            Ok(n) => failed += n,
            Err(e) => {
                println!("[{section}] error: {e}");
                failed += 1;
            }
        }
    }
    if failed == 0 {
        println!("perf gate: PASS");
        ExitCode::SUCCESS
    } else {
        println!("perf gate: FAIL ({failed})");
        ExitCode::FAILURE
    }
}

fn run_section(sec: &Section, limits: Option<&HashMap<String, f64>>) -> std::io::Result<usize> {
    let (section, lanes, cases) = (sec.name, sec.lanes, sec.cases);
    let cpus = std::thread::available_parallelism().map_or(6, usize::from);
    let broker_cpus = format!("0-{}", lanes - 1);
    let load_cpus = format!("{lanes}-{}", cpus - 1);
    let (mut broker, addr) = spawn_broker(&broker_cpus, lanes, sec.busy)?;
    let mut failed = 0;
    for case in cases {
        let Some(limit) = limits.map_or(Some(smoke(case.key)), |l| l.get(case.key).copied()) else {
            continue;
        };
        let line = match case.kind {
            Kind::IdleMemory { conns } => idle_memory(&addr, conns, broker.id()),
            kind => run_load(&load_cpus, &addr, lanes, kind),
        };
        let Some(value) = line.as_ref().ok().and_then(|l| result_value(l, case.kind)) else {
            println!(
                "[{section}] {}: no result ({})",
                case.key,
                line.err().unwrap_or_default()
            );
            failed += 1;
            continue;
        };
        let lower_better = case.key.ends_with("_us") || case.key.ends_with("_kib");
        let ok = if lower_better {
            value <= limit
        } else {
            value >= limit
        };
        let clean = line
            .as_ref()
            .is_ok_and(|l| !l.contains("closed=") || l.contains("closed=0"));
        let verdict = if ok && clean { "PASS" } else { "FAIL" };
        if verdict == "FAIL" {
            failed += 1;
        }
        let bound = if lower_better { "max" } else { "min" };
        println!(
            "[{section}] {} = {} ({bound} {}) {verdict}  {}",
            case.key,
            human(value),
            human(limit),
            line.unwrap_or_default()
        );
    }
    let _ = broker.kill();
    let _ = broker.wait();
    Ok(failed)
}

fn human(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.2}M", v / 1e6)
    } else if v >= 1e4 {
        format!("{:.0}K", v / 1e3)
    } else {
        format!("{v:.1}")
    }
}

fn pinned(cpus: &str) -> Command {
    let exe = std::env::current_exe().unwrap_or_else(|_| "zzq_perf_verify".into());
    let taskset_ok = Command::new("taskset")
        .arg("--version")
        .stdout(Stdio::null())
        .status()
        .is_ok();
    if taskset_ok {
        let mut c = Command::new("taskset");
        c.arg("-c").arg(cpus).arg(exe);
        c
    } else {
        Command::new(exe)
    }
}

fn spawn_broker(cpus: &str, lanes: usize, busy: bool) -> std::io::Result<(Child, String)> {
    let mut child = pinned(cpus)
        .arg("--broker-child")
        .arg(lanes.to_string())
        .arg(if busy { "busy" } else { "idle" })
        .stdout(Stdio::piped())
        .spawn()?;
    let mut line = String::new();
    if let Some(out) = child.stdout.take() {
        BufReader::new(out).read_line(&mut line)?;
    }
    let addr = line.trim().trim_start_matches("tcp://").to_owned();
    if addr.is_empty() {
        let _ = child.kill();
        return Err(std::io::Error::other("broker child printed no endpoint"));
    }
    Ok((child, addr))
}

fn run_load(cpus: &str, addr: &str, lanes: usize, kind: Kind) -> Result<String, String> {
    let mut c = pinned(cpus);
    c.arg("--load-child").arg(addr).arg(lanes.to_string());
    match kind {
        Kind::Throughput {
            scenario,
            qos,
            size,
            pubs,
            subs,
            cross,
            topics,
        } => {
            let s = match scenario {
                Scenario::Pairs => "pairs",
                Scenario::Fanout => "fanout",
                Scenario::Fanin => "fanin",
            };
            let s = if cross { "cross" } else { s };
            c.args([
                s,
                &qos.to_string(),
                &size.to_string(),
                &pubs.to_string(),
                &subs.to_string(),
                &topics.to_string(),
            ]);
        }
        Kind::LatencyP50 { qos, size } => {
            c.args(["rtt", &qos.to_string(), &size.to_string(), "0", "0", "1"]);
        }
        Kind::IdleMemory { .. } => return Err("measured in process".to_owned()),
    }
    let out = c.output().map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    if out.status.success() && !text.is_empty() {
        Ok(text)
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_owned())
    }
}

/// Opens `conns` connections from this process, each subscribed to its own
/// topic, and reports how much the broker's RSS grew per connection.
fn idle_memory(addr: &str, conns: usize, broker: u32) -> Result<String, String> {
    let rss = || -> Result<f64, String> {
        let status = std::fs::read_to_string(format!("/proc/{broker}/status"))
            .map_err(|e| format!("broker status: {e}"))?;
        status
            .lines()
            .find_map(|l| l.strip_prefix("VmRSS:"))
            .and_then(|v| v.trim().trim_end_matches("kB").trim().parse().ok())
            .ok_or_else(|| "no VmRSS".to_owned())
    };
    let before = rss()?;
    let mut open = Vec::with_capacity(conns);
    for i in 0..conns {
        let (mut s, mut rd, _) = wire::connect(addr, &format!("idle-{i}"), true)
            .map_err(|e| format!("connect {i}: {e}"))?;
        wire::subscribe(&mut s, &mut rd, &format!("idle/{i}"), 0, true)
            .map_err(|e| format!("subscribe {i}: {e}"))?;
        open.push(s);
    }
    std::thread::sleep(Duration::from_millis(500));
    let after = rss()?;
    drop(open);
    let per = (after - before) / conns as f64;
    Ok(format!(
        "conns={conns} rss_before_kib={before} rss_after_kib={after} kib_per_conn={per:.2}"
    ))
}

fn result_value(line: &str, kind: Kind) -> Option<f64> {
    let key = match kind {
        Kind::Throughput { .. } => "delivered=",
        Kind::LatencyP50 { .. } => "p50=",
        Kind::IdleMemory { .. } => "kib_per_conn=",
    };
    line.split_whitespace()
        .find_map(|w| w.strip_prefix(key))
        .and_then(|v| v.parse().ok())
}

fn env_ms(name: &str, default: u64) -> Duration {
    Duration::from_millis(
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}

fn load_child(args: &[String]) -> ExitCode {
    let [addr, lanes, scenario, qos, size, pubs, subs, topics] = args else {
        return ExitCode::FAILURE;
    };
    let num = |s: &str| s.parse::<usize>().unwrap_or(0);
    let cfg = Load {
        addr: addr.clone(),
        qos: num(qos) as u8,
        size: num(size),
        pubs: num(pubs).max(1),
        subs: num(subs).max(1),
        lanes: num(lanes).max(1),
        topics: num(topics).max(1),
        warmup: env_ms("ZZQ_PERF_WARMUP_MS", 1000),
        measure: env_ms("ZZQ_PERF_MEASURE_MS", 3000),
        ..Load::default()
    };
    let cross = scenario == "cross";
    let scenario = match scenario.as_str() {
        "rtt" => {
            return match load::latency(&cfg, 20_000) {
                Ok(l) => {
                    println!(
                        "p50={:.1} p99={:.1} p999={:.1} lost={}",
                        l.p50, l.p99, l.p999, l.lost
                    );
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("{e}");
                    ExitCode::FAILURE
                }
            };
        }
        "fanout" => Scenario::Fanout,
        "fanin" => Scenario::Fanin,
        _ => Scenario::Pairs,
    };
    let cfg = Load { cross, ..cfg };
    match load::throughput(scenario, &cfg) {
        Ok(t) => {
            println!(
                "delivered={:.0} published={:.0} loss={:.2} closed={}",
                t.delivered, t.published, t.loss, t.closed
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn broker_child(lanes: usize, busy: bool) -> ExitCode {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return ExitCode::FAILURE;
    };
    rt.block_on(async {
        let cfg = zzq_broker::Config {
            io_threads: lanes,
            name: "zzq-perf".to_owned(),
            busy_poll: busy,
            ..zzq_broker::Config::default()
        };
        let Ok(ctx) = zzq_broker::Context::with_config(&cfg) else {
            return ExitCode::FAILURE;
        };
        let Ok(broker) = zzq_broker::Broker::new(&ctx, zzq_broker::BrokerOptions::default()).await
        else {
            return ExitCode::FAILURE;
        };
        let Ok(ep) = broker.bind("tcp://127.0.0.1:0").await else {
            return ExitCode::FAILURE;
        };
        println!("{ep}");
        std::future::pending::<()>().await;
        ExitCode::SUCCESS
    })
}

fn parse_ini(text: &str) -> HashMap<String, HashMap<String, f64>> {
    let mut out: HashMap<String, HashMap<String, f64>> = HashMap::new();
    let mut section = String::new();
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(s) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            s.clone_into(&mut section);
            out.entry(section.clone()).or_default();
        } else if let Some((k, v)) = line.split_once('=')
            && let Ok(v) = v.trim().parse()
        {
            out.entry(section.clone())
                .or_default()
                .insert(k.trim().to_owned(), v);
        }
    }
    out
}

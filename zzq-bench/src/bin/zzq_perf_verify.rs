//! Local performance gates for the broker.
//!
//! Thresholds are read from `.perf_hw`, which is intentionally ignored.
//! Without that file, this command runs a smaller smoke gate. The broker and
//! the load run in separate pinned child processes. State memory cases each
//! get a fresh broker child, as memory freed earlier hides what state costs.
//! Durable sections run each case on a fresh broker and data directory under
//! `TMPDIR`, which must be on the disk to measure, with subscribers that keep
//! their sessions.
//!
//! Env: `ZZQ_PERF_MEASURE_MS` (default 3000), `ZZQ_PERF_WARMUP_MS` (1000).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::Duration;

use zzq_bench::child;
use zzq_bench::load::{self, Keep, Load, Scenario, Until};
use zzq_bench::ram::{self, Probe};
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
    /// Broker RSS per piece of state, bytes.
    StateMemory(State),
    /// Peak broker RSS on a fresh broker, KiB, under a QoS 0 fan-out with
    /// most subscribers on the lane after the publisher's, which falls behind.
    BehindMemory {
        subs: usize,
    },
}

/// State added to a fresh broker.
#[derive(Clone, Copy)]
enum State {
    /// Offline sessions with two subscriptions each.
    Session,
    /// QoS 1 messages of 100 B, 10 for each of the offline sessions.
    Queued,
    /// Retained messages of 100 B.
    Retained,
}

/// Offline sessions for the state memory cases.
const SESSIONS: usize = 20_000;

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

const fn fanin(qos: u8, size: usize, pubs: usize) -> Kind {
    Kind::Throughput {
        scenario: Scenario::Fanin,
        qos,
        size,
        pubs,
        subs: 1,
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
        key: "offline_session_bytes",
        kind: Kind::StateMemory(State::Session),
    },
    Case {
        key: "queued_100b_bytes",
        kind: Kind::StateMemory(State::Queued),
    },
    Case {
        key: "retained_100b_bytes",
        kind: Kind::StateMemory(State::Retained),
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
        key: "fanout16_qos2_16b_msgs_s",
        kind: fanout(2, 16, 16),
    },
    Case {
        key: "pairs16_qos0_16b_msgs_s",
        kind: pairs(0, 16, 16),
    },
    Case {
        key: "pairs16_qos2_16b_msgs_s",
        kind: pairs(2, 16, 16),
    },
    Case {
        key: "fanin16_qos0_16b_msgs_s",
        kind: fanin(0, 16, 16),
    },
    Case {
        key: "fanin16_qos1_16b_msgs_s",
        kind: fanin(1, 16, 16),
    },
    Case {
        key: "fanin16_qos2_16b_msgs_s",
        kind: fanin(2, 16, 16),
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
        key: "fanout16_behind_rss_kib",
        kind: Kind::BehindMemory { subs: 16 },
    },
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
        key: "pairs2_qos2_16b_msgs_s",
        kind: pairs(2, 16, 2),
    },
    Case {
        key: "cross2_qos2_16b_msgs_s",
        kind: cross(2, 16, 2),
    },
    Case {
        key: "fanout16_qos0_16b_msgs_s",
        kind: fanout(0, 16, 16),
    },
    Case {
        key: "fanout16_qos2_16b_msgs_s",
        kind: fanout(2, 16, 16),
    },
    Case {
        key: "fanin16_qos2_16b_msgs_s",
        kind: fanin(2, 16, 16),
    },
];

/// The journal paths, each case on a fresh broker and data directory.
const ONE_LANE_DURABLE: &[Case] = &[
    Case {
        key: "qos1_16b_msgs_s",
        kind: pairs(1, 16, 1),
    },
    Case {
        key: "qos2_16b_msgs_s",
        kind: pairs(2, 16, 1),
    },
    Case {
        key: "qos1_4k_msgs_s",
        kind: pairs(1, 4096, 1),
    },
    Case {
        key: "qos2_4k_msgs_s",
        kind: pairs(2, 4096, 1),
    },
    Case {
        key: "pairs16_qos1_16b_msgs_s",
        kind: pairs(1, 16, 16),
    },
    Case {
        key: "pairs16_qos2_16b_msgs_s",
        kind: pairs(2, 16, 16),
    },
    Case {
        key: "fanout16_qos1_16b_msgs_s",
        kind: fanout(1, 16, 16),
    },
    Case {
        key: "fanout16_qos2_16b_msgs_s",
        kind: fanout(2, 16, 16),
    },
    Case {
        key: "fanin16_qos1_16b_msgs_s",
        kind: fanin(1, 16, 16),
    },
    Case {
        key: "fanin16_qos2_16b_msgs_s",
        kind: fanin(2, 16, 16),
    },
    Case {
        key: "ack_qos1_16b_p50_us",
        kind: Kind::LatencyP50 { qos: 1, size: 16 },
    },
];

const TWO_LANES_DURABLE: &[Case] = &[
    Case {
        key: "cross2_qos1_16b_msgs_s",
        kind: cross(1, 16, 2),
    },
    Case {
        key: "cross2_qos2_16b_msgs_s",
        kind: cross(2, 16, 2),
    },
    Case {
        key: "pairs16_qos1_16b_msgs_s",
        kind: pairs(1, 16, 16),
    },
    Case {
        key: "pairs16_qos2_16b_msgs_s",
        kind: pairs(2, 16, 16),
    },
    Case {
        key: "fanout16_qos2_16b_msgs_s",
        kind: fanout(2, 16, 16),
    },
    Case {
        key: "fanin16_qos2_16b_msgs_s",
        kind: fanin(2, 16, 16),
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
    /// The broker has a data directory, and subscribers keep their sessions.
    durable: bool,
    cases: &'static [Case],
}

const SECTIONS: &[Section] = &[
    Section {
        name: "broker_1io",
        lanes: 1,
        busy: false,
        durable: false,
        cases: ONE_LANE,
    },
    Section {
        name: "broker_2io",
        lanes: 2,
        busy: false,
        durable: false,
        cases: TWO_LANES,
    },
    Section {
        name: "broker_1io_busy",
        lanes: 1,
        busy: true,
        durable: false,
        cases: BUSY,
    },
    Section {
        name: "broker_1io_durable",
        lanes: 1,
        busy: false,
        durable: true,
        cases: ONE_LANE_DURABLE,
    },
    Section {
        name: "broker_2io_durable",
        lanes: 2,
        busy: false,
        durable: true,
        cases: TWO_LANES_DURABLE,
    },
];

/// Tries of a durable case that misses its threshold. A VM's disk swings 2-3x
/// between runs, and a real regression misses on every try.
const DURABLE_TRIES: usize = 3;

/// Thresholds used without `.perf_hw`.
fn smoke(key: &str, durable: bool) -> f64 {
    if durable {
        // Whatever the disk.
        if key.ends_with("_us") {
            100_000.0
        } else {
            100.0
        }
    } else if key.ends_with("_us") {
        1000.0
    } else if key.ends_with("_rss_kib") {
        512.0 * 1024.0
    } else if key.ends_with("_kib") {
        64.0
    } else if key.ends_with("_bytes") {
        4096.0
    } else {
        100_000.0
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("--broker-child") => {
            let lanes = args.get(2).and_then(|n| n.parse().ok()).unwrap_or(1);
            let busy = args.get(3).is_some_and(|b| b == "busy");
            return broker_child(lanes, busy, args.get(4).map(PathBuf::from));
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

/// Runs `measure` up to `tries` times, until a result `passes`. Returns the
/// last result and how many it took.
fn until_passes<T>(
    tries: usize,
    mut measure: impl FnMut() -> T,
    passes: impl Fn(&T) -> bool,
) -> (T, usize) {
    let mut result = measure();
    let mut took = 1;
    while took < tries && !passes(&result) {
        result = measure();
        took += 1;
    }
    (result, took)
}

/// Whether a smaller value of the case `key` is better.
fn lower_better(key: &str) -> bool {
    ["_us", "_kib", "_bytes"]
        .iter()
        .any(|unit| key.ends_with(unit))
}

/// Whether a result meets `limit`, from a run that closed no connection.
fn meets(line: &Result<String, String>, case: &Case, limit: f64) -> bool {
    let Ok(line) = line else {
        return false;
    };
    let clean = !line.contains("closed=") || line.contains("closed=0");
    clean
        && result_value(line, case.kind).is_some_and(|value| {
            if lower_better(case.key) {
                value <= limit
            } else {
                value >= limit
            }
        })
}

fn run_section(sec: &Section, limits: Option<&HashMap<String, f64>>) -> std::io::Result<usize> {
    let (section, cases) = (sec.name, sec.cases);
    let (broker_cpus, load_cpus) = sec.cpus();
    // A durable case starts its own, on a new data directory.
    let mut shared = if sec.durable {
        None
    } else {
        Some(spawn_broker(&broker_cpus, sec, None)?)
    };
    let mut failed = 0;
    for case in cases {
        let threshold = |l: &HashMap<String, f64>| l.get(case.key).copied();
        let Some(limit) = limits.map_or(Some(smoke(case.key, sec.durable)), threshold) else {
            continue;
        };
        let tries = if sec.durable { DURABLE_TRIES } else { 1 };
        let measure = || match (&shared, case.kind) {
            (Some((broker, addr)), Kind::IdleMemory { conns }) => {
                idle_memory(addr, conns, broker.id())
            }
            (_, Kind::StateMemory(state)) => state_memory(&broker_cpus, sec, state),
            (_, Kind::BehindMemory { subs }) => behind_memory(&broker_cpus, &load_cpus, sec, subs),
            (Some((_, addr)), kind) => run_load(&load_cpus, addr, sec, kind),
            (None, kind) => durable_load(&broker_cpus, &load_cpus, sec, kind),
        };
        let (line, took) = until_passes(tries, measure, |l| meets(l, case, limit));
        let Some(value) = line.as_ref().ok().and_then(|l| result_value(l, case.kind)) else {
            println!(
                "[{section}] {}: no result ({})",
                case.key,
                line.err().unwrap_or_default()
            );
            failed += 1;
            continue;
        };
        let verdict = if meets(&line, case, limit) {
            "PASS"
        } else {
            failed += 1;
            "FAIL"
        };
        let bound = if lower_better(case.key) { "max" } else { "min" };
        println!(
            "[{section}] {} = {} ({bound} {}) {verdict}  tries={took} {}",
            case.key,
            human(value),
            human(limit),
            line.unwrap_or_default()
        );
    }
    if let Some((broker, _)) = &mut shared {
        let _ = broker.kill();
        let _ = broker.wait();
    }
    Ok(failed)
}

impl Section {
    /// The CPUs of the broker and of the load. A broker with a data directory
    /// has its disk threads beside its lanes, as in `zzq_suite`.
    fn cpus(&self) -> (String, String) {
        let cpus = std::thread::available_parallelism().map_or(6, usize::from);
        let broker = if self.durable {
            cpus.saturating_sub(2).max(self.lanes)
        } else {
            self.lanes
        };
        (
            format!("0-{}", broker - 1),
            format!("{broker}-{}", cpus - 1),
        )
    }
}

/// Runs `kind` on a broker of its own with an empty data directory, as what a
/// journal holds changes how fast it goes.
fn durable_load(
    broker_cpus: &str,
    load_cpus: &str,
    sec: &Section,
    kind: Kind,
) -> Result<String, String> {
    let dir = std::env::temp_dir().join(format!("zzq-perf-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let spawned = spawn_broker(broker_cpus, sec, Some(&dir)).map_err(|e| e.to_string());
    let line = spawned.and_then(|(mut broker, addr)| {
        let line = run_load(load_cpus, &addr, sec, kind);
        let _ = broker.kill();
        let _ = broker.wait();
        line
    });
    let _ = std::fs::remove_dir_all(&dir);
    line
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

fn spawn_broker(cpus: &str, sec: &Section, dir: Option<&Path>) -> std::io::Result<(Child, String)> {
    let mut cmd = pinned(cpus);
    cmd.arg("--broker-child")
        .arg(sec.lanes.to_string())
        .arg(if sec.busy { "busy" } else { "idle" });
    if let Some(dir) = dir {
        cmd.arg(dir);
    }
    child::spawn(cmd)
}

fn run_load(cpus: &str, addr: &str, sec: &Section, kind: Kind) -> Result<String, String> {
    let mut c = pinned(cpus);
    c.arg("--load-child")
        .arg(addr)
        .arg(sec.lanes.to_string())
        .arg(if sec.durable { "keep" } else { "ram" });
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
        Kind::BehindMemory { subs } => {
            c.args(["behind", "0", "16", "1", &subs.to_string(), "1"]);
        }
        Kind::IdleMemory { .. } | Kind::StateMemory(_) => {
            return Err("measured in process".to_owned());
        }
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
        ram::rss(broker)
            .map(|b| b as f64 / 1024.0)
            .map_err(|e| format!("broker RSS: {e}"))
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

/// Adds state to a fresh broker child and reports how much its RSS grew
/// per item.
fn state_memory(cpus: &str, sec: &Section, state: State) -> Result<String, String> {
    let (mut broker, addr) = spawn_broker(cpus, sec, None).map_err(|e| e.to_string())?;
    let grown = Probe::new(&addr, broker.id(), true, None).and_then(|mut probe| match state {
        State::Session => probe.sessions(SESSIONS),
        State::Queued => probe.sessions(SESSIONS).and_then(|_| probe.queued(10, 100)),
        State::Retained => probe.retained(100_000, 100),
    });
    let _ = broker.kill();
    let _ = broker.wait();
    let g = grown.map_err(|e| e.to_string())?;
    Ok(format!(
        "items={} rss_before_kib={} rss_after_kib={} bytes_each={:.1}",
        g.items,
        g.rss_before >> 10,
        g.rss_after >> 10,
        g.per_item()
    ))
}

/// Runs the fan-out of [`Kind::BehindMemory`] on a fresh broker child and
/// reports the broker's peak RSS.
fn behind_memory(
    broker_cpus: &str,
    load_cpus: &str,
    sec: &Section,
    subs: usize,
) -> Result<String, String> {
    let (mut broker, addr) = spawn_broker(broker_cpus, sec, None).map_err(|e| e.to_string())?;
    let line = run_load(load_cpus, &addr, sec, Kind::BehindMemory { subs });
    let peak = ram::peak_rss(broker.id());
    let _ = broker.kill();
    let _ = broker.wait();
    let (line, peak) = (line?, peak.map_err(|e| e.to_string())?);
    Ok(format!("{line} peak_rss_kib={}", peak >> 10))
}

fn result_value(line: &str, kind: Kind) -> Option<f64> {
    let key = match kind {
        Kind::Throughput { .. } => "delivered=",
        Kind::LatencyP50 { .. } => "p50=",
        Kind::IdleMemory { .. } => "kib_per_conn=",
        Kind::StateMemory(_) => "bytes_each=",
        Kind::BehindMemory { .. } => "peak_rss_kib=",
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
    let [addr, lanes, keep, scenario, qos, size, pubs, subs, topics] = args else {
        return ExitCode::FAILURE;
    };
    let keep = keep == "keep";
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
        keep: Keep {
            sessions: keep,
            ..Keep::default()
        },
        ..Load::default()
    };
    let cross = scenario == "cross";
    let scenario_name = scenario.as_str();
    let scenario = match scenario_name {
        "rtt" => return latency_child(&cfg, keep),
        "fanout" | "behind" => Scenario::Fanout,
        "fanin" => Scenario::Fanin,
        _ => Scenario::Pairs,
    };
    // A fifth of the subscribers share the publisher's lane.
    let local = (scenario_name == "behind").then_some(cfg.subs / 5);
    let cfg = Load {
        cross,
        local,
        ..cfg
    };
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

/// Prints round trips. With `keep`, what a journal costs: the wait for the ack.
fn latency_child(cfg: &Load, keep: bool) -> ExitCode {
    let until = if keep { Until::Acked } else { Until::Delivered };
    match load::latency(cfg, 20_000, until) {
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
    }
}

fn broker_child(lanes: usize, busy: bool, data_dir: Option<PathBuf>) -> ExitCode {
    let cfg = zzq_broker::Config {
        io_threads: lanes,
        name: "zzq-perf".to_owned(),
        busy_poll: busy,
        ..zzq_broker::Config::default()
    };
    let opts = zzq_broker::BrokerOptions {
        data_dir,
        ..zzq_broker::BrokerOptions::default()
    };
    child::serve(&cfg, opts)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_means_one_case_in_its_section() {
        for sec in SECTIONS {
            let mut keys: Vec<_> = sec.cases.iter().map(|c| c.key).collect();
            keys.sort_unstable();
            let all = keys.len();
            keys.dedup();
            assert_eq!(keys.len(), all, "{}", sec.name);
        }
    }

    #[test]
    fn the_durable_sections_gate_qos2() {
        let durable: Vec<_> = SECTIONS.iter().filter(|s| s.durable).collect();
        assert_eq!(durable.len(), 2);
        for sec in durable {
            assert!(sec.name.ends_with("_durable"));
            assert!(
                sec.cases
                    .iter()
                    .any(|c| matches!(c.kind, Kind::Throughput { qos: 2, .. }))
            );
        }
    }

    #[test]
    fn a_durable_case_gets_a_smoke_threshold_any_disk_meets() {
        assert_eq!(smoke("qos2_16b_msgs_s", true), 100.0);
        assert_eq!(smoke("ack_qos1_16b_p50_us", true), 100_000.0);
        assert_eq!(smoke("qos2_16b_msgs_s", false), 100_000.0);
    }

    #[test]
    fn a_memory_case_under_load_gets_room_for_its_buffers_in_the_smoke_gate() {
        assert_eq!(smoke("fanout16_behind_rss_kib", false), 512.0 * 1024.0);
        assert_eq!(smoke("idle_conn_kib", false), 64.0);
        let line = "delivered=1 peak_rss_kib=37000";
        let kind = Kind::BehindMemory { subs: 16 };
        assert_eq!(result_value(line, kind), Some(37000.0));
    }

    #[test]
    fn a_case_stops_at_the_first_try_that_passes() {
        let mut values = [1, 2, 3].into_iter();
        let found = until_passes(3, || values.next().unwrap(), |v| *v >= 2);
        assert_eq!(found, (2, 2));
    }

    #[test]
    fn a_case_that_always_misses_runs_out_of_tries() {
        let mut runs = 0;
        let measure = || {
            runs += 1;
            runs
        };
        assert_eq!(until_passes(3, measure, |_| false), (3, 3));
        let mut runs = 0;
        let measure = || {
            runs += 1;
            runs
        };
        assert_eq!(until_passes(1, measure, |_| false), (1, 1));
    }

    #[test]
    fn a_result_meets_its_limit_on_the_side_of_its_unit_on_a_clean_run() {
        let rate = Case {
            key: "qos2_16b_msgs_s",
            kind: pairs(2, 16, 1),
        };
        let meets_rate = |line: &str| meets(&Ok(line.into()), &rate, 100.0);
        assert!(meets_rate("delivered=100 closed=0"));
        assert!(!meets_rate("delivered=99 closed=0"));
        assert!(!meets_rate("delivered=100 closed=1"));
        assert!(!meets(&Err("broker died".into()), &rate, 100.0));
        let latency = Case {
            key: "ack_qos1_16b_p50_us",
            kind: Kind::LatencyP50 { qos: 1, size: 16 },
        };
        assert!(meets(&Ok("p50=100".into()), &latency, 100.0));
        assert!(!meets(&Ok("p50=101".into()), &latency, 100.0));
    }

    #[test]
    fn a_threshold_file_has_sections_of_numbers() {
        let ini = parse_ini("# note\n[a]\nx = 1.5\ny=not\n\n[b]\nz=2\n");
        assert_eq!(ini["a"].get("x"), Some(&1.5));
        assert_eq!(ini["a"].get("y"), None);
        assert_eq!(ini["b"].get("z"), Some(&2.0));
    }
}

//! Fixed-load test for any MQTT broker: many connections, each publishing
//! on a schedule.
//!
//! Usage: `zzq_loadbench <p2p|fanin|fanout> [key=value ...]`
//!
//! Keys (defaults): `port=1883 addrs=8 v=5 qos=1 size=16 pubs=10000 subs=0
//! rates=10000,20000,50000,100000,200000 warmup=2 secs=8 connect_rate=5000
//! cpu_pid=0 trace=0`
//!
//! `rates` are offered deliveries per second, one step each. `cpu_pid` has
//! the broker's PIDs, comma-separated, for its CPU and memory. `trace=1`
//! also prints where a window's time went.

use std::collections::HashMap;
use std::process::ExitCode;
use std::time::Duration;

use zzq_bench::fixed::{self, Fixed, Report, Scenario};

/// Above this generator CPU (of 200%), results measure the generator.
const SATURATED: f64 = 180.0;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((scenario, cfg)) = parse(&args) else {
        eprintln!("usage: zzq_loadbench <p2p|fanin|fanout> [key=value ...]");
        return ExitCode::FAILURE;
    };
    let head = format!(
        "fixed {scenario} v{} qos={} size={} pubs={} subs={}",
        if cfg.v5 { 5 } else { 4 },
        cfg.qos,
        cfg.size,
        cfg.pubs,
        scenario.subscribers(&cfg),
    );
    let result = fixed::run(scenario, &cfg, |r| match r {
        Report::Connect(c) if c.ok() => {
            eprintln!(
                "connected {} in {:.1}s, {} retries",
                c.clients,
                c.took.as_secs_f64(),
                c.retries
            );
        }
        Report::Connect(c) => {
            let why = if c.timed_out { "timeout" } else { "failed" };
            eprintln!("connect {why}: {} of {} connected", c.connected, c.clients);
            if let Some(e) = &c.error {
                eprintln!("first failure: {e}");
            }
        }
        Report::Step(step) => {
            if step.cpu.own > SATURATED {
                println!(
                    "warning: load generator saturated ({:.0}% of 200%)",
                    step.cpu.own
                );
            }
            println!("{head}  {step}");
            if let Some(t) = &step.trace {
                eprint!("{t}");
            }
        }
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn parse(args: &[String]) -> Option<(Scenario, Fixed)> {
    let (scenario, rest) = args.split_first()?;
    let kv = rest
        .iter()
        .map(|a| a.split_once('='))
        .collect::<Option<HashMap<_, _>>>()?;
    let get = |k: &str| kv.get(k).copied();
    let num = |k: &str, d: u64| get(k).map_or(Some(d), |v| v.parse().ok());
    let secs = |k: &str, d: Duration| {
        get(k).map_or(Some(d), |v| {
            v.parse()
                .ok()
                .and_then(|s| Duration::try_from_secs_f64(s).ok())
        })
    };
    let d = Fixed::default();
    let cfg = Fixed {
        port: u16::try_from(num("port", d.port.into())?).ok()?,
        addrs: u8::try_from(num("addrs", d.addrs.into())?).ok()?,
        v5: get("v").is_none_or(|v| v == "5"),
        qos: u8::try_from(num("qos", d.qos.into())?).ok()?,
        size: usize::try_from(num("size", d.size as u64)?).ok()?,
        pubs: usize::try_from(num("pubs", d.pubs as u64)?).ok()?,
        subs: usize::try_from(num("subs", d.subs as u64)?).ok()?,
        rates: match get("rates") {
            Some(r) => r
                .split(',')
                .map(|x| x.parse().ok())
                .collect::<Option<_>>()?,
            None => d.rates,
        },
        warmup: secs("warmup", d.warmup)?,
        measure: secs("secs", d.measure)?,
        connect_rate: get("connect_rate").map_or(Some(d.connect_rate), |v| v.parse().ok())?,
        broker_pids: zzq_bench::cpu::pids(get("cpu_pid").unwrap_or("0"))?,
        trace: get("trace") == Some("1"),
    };
    Some((scenario.parse().ok()?, cfg))
}

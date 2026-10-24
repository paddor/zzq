//! Load generator for any MQTT broker.
//!
//! Usage: `zzq_brokerbench <pairs|fanout|fanin|rtt> [key=value ...]`
//!
//! Keys (defaults): addr=127.0.0.1:1883 v=5 qos=0 size=16 pubs=1 subs=1
//! warmup=1 secs=5 window=100 iters=20000 lanes=1 cross=0 topics=1
//!
//! `lanes` and `cross` place clients on the lanes of a zzq broker.

use std::collections::HashMap;
use std::process::ExitCode;
use std::time::Duration;

use zzq_bench::load::{self, Latency, Load, Scenario, Throughput};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((scenario, cfg, iters)) = parse(&args) else {
        eprintln!("usage: zzq_brokerbench <pairs|fanout|fanin|rtt> [key=value ...]");
        return ExitCode::FAILURE;
    };
    let result = match scenario.as_str() {
        "rtt" => load::latency(&cfg, iters).map(|l| print_latency(&cfg, iters, &l)),
        "pairs" => {
            load::throughput(Scenario::Pairs, &cfg).map(|t| print_throughput("pairs", &cfg, &t))
        }
        "fanout" => {
            load::throughput(Scenario::Fanout, &cfg).map(|t| print_throughput("fanout", &cfg, &t))
        }
        "fanin" => {
            load::throughput(Scenario::Fanin, &cfg).map(|t| print_throughput("fanin", &cfg, &t))
        }
        _ => {
            eprintln!("unknown scenario {scenario}");
            return ExitCode::FAILURE;
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn parse(args: &[String]) -> Option<(String, Load, usize)> {
    let (scenario, rest) = args.split_first()?;
    let kv = rest
        .iter()
        .map(|a| a.split_once('='))
        .collect::<Option<HashMap<_, _>>>()?;
    let get = |k: &str, d: &'static str| kv.get(k).copied().unwrap_or(d);
    let num = |k: &str, d: &'static str| get(k, d).parse::<u64>().ok();
    let cfg = Load {
        addr: get("addr", "127.0.0.1:1883").to_owned(),
        v5: get("v", "5") == "5",
        qos: num("qos", "0")? as u8,
        size: num("size", "16")? as usize,
        pubs: num("pubs", "1")? as usize,
        subs: num("subs", "1")? as usize,
        warmup: Duration::from_secs(num("warmup", "1")?),
        measure: Duration::from_secs(num("secs", "5")?),
        window: num("window", "100")? as usize,
        lanes: num("lanes", "1")? as usize,
        cross: get("cross", "0") == "1",
        topics: num("topics", "1")? as usize,
    };
    Some((scenario.clone(), cfg, num("iters", "20000")? as usize))
}

fn print_latency(cfg: &Load, iters: usize, l: &Latency) {
    println!(
        "   rtt v{} qos={} size={:>5} iters={iters}  p50 {:.1}us  p90 {:.1}us  p99 {:.1}us  p99.9 {:.1}us  max {:.1}us  lost {}",
        if cfg.v5 { 5 } else { 4 },
        cfg.qos,
        cfg.size,
        l.p50,
        l.p90,
        l.p99,
        l.p999,
        l.max,
        l.lost
    );
}

fn print_throughput(name: &str, cfg: &Load, t: &Throughput) {
    let (pubs, subs) = match name {
        "pairs" => (cfg.pubs, cfg.pubs),
        "fanout" => (1, cfg.subs),
        _ => (cfg.pubs, 1),
    };
    let secs: Vec<String> = t
        .per_second
        .iter()
        .map(|r| format!("{:.2}M", r / 1e6))
        .collect();
    println!(
        "{name:>6} v{} qos={} size={:>5} pubs={} subs={}  pub {:>9.0}/s  delivered {:>9.0}/s {:>7.1} MB/s  loss {:>5.1}%  closed {}  per-sec [{}]",
        if cfg.v5 { 5 } else { 4 },
        cfg.qos,
        cfg.size,
        pubs,
        subs,
        t.published,
        t.delivered,
        t.delivered * cfg.size as f64 / 1e6,
        t.loss,
        t.closed,
        secs.join(" ")
    );
}

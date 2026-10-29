//! Serial MQTT benchmark suites, recorded into the result cache.
//!
//! Usage: `zzq_suite [--fill] [--smoke] <suite>`
//!
//! - `zzq [lanes]`: the broker suite against `zzq_broker` (4 IO lanes).
//! - `<rival> [default|tuned]`: the broker suite against rmqtt, emqx,
//!   mosquitto, nanomq, rumqttd, xmq, flashmq, vernemq, fluxmq, saguin,
//!   hivemq or tbmq (tuned).
//! - `clients`, `zzqclient`: client libraries against `zzq_fakebroker`.
//! - `fixed <broker> [config|lanes]`: the fixed-load test, zzq with 1 IO
//!   lane, a rival tuned.
//! - `durable <broker> [lanes]`: QoS 1 and 2 to persistent sessions with
//!   the broker's storage on, zzq with 1 IO lane and a data directory, a
//!   fresh broker for each cell.
//! - `failover <zzq | vernemq [cluster|netsplit] | tbmq>`: QoS 1 under load on a
//!   cluster of three, the node the publishers use killed or stopped
//!   mid-run, a fresh cluster per cell.
//! - `all [name...]`: every suite above, or the named ones (`zzq-1`,
//!   `rmqtt-tuned`, `fixed-zzq-2`, `durable-emqx`, ...), one process each,
//!   logged to `$XDG_CACHE_HOME/zzq/logs/<date>/<name>.log`. Stops at the
//!   first that fails.
//!
//! Each run waits for a quiet system first: no compiler running and at
//! least 90% idle CPU. A run a compiler overlapped runs again. Harness
//! warnings, panics and timeouts stop the suite; broker-side closes and
//! loss are data. Each result is a row in `brokers.jsonl`, `clients.jsonl`
//! or `fixed.jsonl` in `$XDG_CACHE_HOME/zzq`.
//!
//! `--fill` skips cells the cache has for that implementation and config.
//! `--smoke` runs the first cell of each part briefly, without waiting for
//! a quiet system, and prints rows instead of recording them.
//!
//! Environment: `ZZQ_BENCH_BIN` (the zzq tools, `zzq_broker` and the rival
//! client benches; default: this binary's directory), `ZZQ_RIVALS` (rival
//! builds), `ZZQ_BROKER_LOG` (broker output) and `TMPDIR` (broker stores,
//! under `zzq-suite`).

mod brokers;
mod parse;
mod paths;
mod record;
mod runner;
mod suites;

use std::fs::{self, OpenOptions};
use std::process::{Command, ExitCode, Stdio};

use paths::Paths;
use record::{Cache, fields};
use suites::{Shape, Suite};

/// Rival brokers, in the order `all` runs them.
const RIVALS: [&str; 12] = [
    "rmqtt",
    "emqx",
    "mosquitto",
    "nanomq",
    "rumqttd",
    "xmq",
    "flashmq",
    "vernemq",
    "fluxmq",
    "saguin",
    "hivemq",
    "tbmq",
];
/// Rivals with a fixed-load config of their own, as the tuned one runs out
/// of memory at 20K connections.
const OWN_FIXED: [&str; 3] = ["nanomq", "fluxmq", "vernemq"];
/// Rivals with storage that keeps QoS 1/2 messages of persistent sessions,
/// in the order `all` runs them. rumqttd has none, `FlashMQ` only writes a
/// snapshot each hour, and `FluxMQ` 1.0.0 refuses persistent sessions on
/// one node.
const DURABLE: [&str; 9] = [
    "rmqtt",
    "emqx",
    "mosquitto",
    "nanomq",
    "xmq",
    "vernemq",
    "saguin",
    "hivemq",
    "tbmq",
];
/// Rivals also run with their default config.
const DEFAULTS: [&str; 3] = ["vernemq", "fluxmq", "saguin"];

const USAGE: &str = "usage: zzq_suite [--fill] [--smoke] <zzq [lanes] | <rival> [default|tuned] | clients | zzqclient | fixed <broker> [config|lanes] | durable <broker> [lanes] | failover <zzq | vernemq [cluster|netsplit] | tbmq> | all [name...]>";

fn main() -> ExitCode {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let mut flag = |f: &str| {
        let had = args.iter().any(|a| a == f);
        args.retain(|a| a != f);
        had
    };
    let (fill, smoke) = (flag("--fill"), flag("--smoke"));
    let paths = Paths::from_env();
    let cache = Cache::new(zzq_bench::cache::dir(), smoke);
    let suite = Suite {
        paths: &paths,
        cache: &cache,
        fill,
        smoke,
    };
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match run(&suite, &args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("STOP: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(suite: &Suite<'_>, args: &[&str]) -> Result<(), String> {
    match args {
        ["all", names @ ..] => return all(suite, names),
        ["fixed", name, rest @ ..] => return fixed(suite, name, rest.first().copied()),
        ["durable", name, rest @ ..] => return durable(suite, name, rest.first().copied()),
        ["failover", name, rest @ ..] => return failover(suite, name, rest.first().copied()),
        _ => {}
    }
    let (name, opt) = match args {
        [name] => (*name, None),
        [name, opt] => (*name, Some(*opt)),
        _ => return Err(USAGE.to_owned()),
    };
    header(&[name], opt);
    match name {
        "clients" => suite.clients(false)?,
        "zzqclient" => suite.clients(true)?,
        "zzq" => {
            let lanes = lanes(opt.unwrap_or("4"))?;
            println!("# zzq_broker --lanes {lanes}");
            let shape = Shape {
                full: true,
                warmup: 1,
                pinned: zzq_pinned(lanes),
                ..Shape::default()
            };
            let who = zzq_who(suite, lanes, "default");
            suite.brokers(
                &who,
                ("zzq", "", lanes),
                &suites::broker_cells(&shape),
                false,
            )?;
        }
        rival if RIVALS.contains(&rival) => {
            let config = opt.unwrap_or("tuned");
            let who = rival_who(rival, config);
            // rumqttd's commit log grows to about 2 GB before it runs
            // steady, 3.5 s at 4 KiB, so it warms up for 5 s. It never
            // frees a topic's log, and every run uses new topics, so each
            // run gets a fresh broker. It speaks MQTT 5 only in part.
            let rumqttd = rival == "rumqttd";
            let shape = Shape {
                full: config == "tuned",
                extra: if rumqttd {
                    vec!["v=4".to_owned()]
                } else {
                    Vec::new()
                },
                warmup: if rumqttd { 5 } else { 1 },
                pinned: Vec::new(),
            };
            let cells = suites::broker_cells(&shape);
            // HiveMQ CE works off a QoS 0 backlog for minutes after a run,
            // and in one suite stopped listening after the 16-pair run, so
            // it too gets a fresh broker per run.
            let fresh = rumqttd || rival == "hivemq";
            suite.brokers(&who, (rival, config, 0), &cells, fresh)?;
        }
        _ => return Err(USAGE.to_owned()),
    }
    println!("# done {}", record::now());
    Ok(())
}

/// `# fixed zzq 2 <time>`: what runs, and when.
fn header(words: &[&str], opt: Option<&str>) {
    let mut words = words.to_vec();
    words.extend(opt);
    println!("# {} {}", words.join(" "), record::now());
}

fn lanes(s: &str) -> Result<usize, String> {
    s.parse()
        .ok()
        .filter(|&n| n > 0)
        .ok_or_else(|| format!("bad lane count {s}"))
}

/// With more lanes, a single pair crosses lanes, the case that differs from
/// one lane. Client IDs alone would pick it by chance.
fn zzq_pinned(lanes: usize) -> Vec<String> {
    if lanes > 1 {
        vec![format!("lanes={lanes}"), "cross=1".to_owned()]
    } else {
        Vec::new()
    }
}

fn zzq_who(suite: &Suite<'_>, lanes: usize, config: &str) -> parse::Row {
    let rev = record::revision(&suite.paths.repo, &["zzq-proto", "zzq-tokio", "zzq-broker"]);
    fields(&[
        ("impl", format!("zzq-{lanes}io").into()),
        ("version", rev.into()),
        ("config", config.into()),
    ])
}

fn rival_who(name: &str, config: &str) -> parse::Row {
    fields(&[
        ("impl", name.into()),
        ("version", brokers::version(name).unwrap_or_default().into()),
        ("config", config.into()),
    ])
}

/// `fixed <broker> [config|lanes]`.
fn fixed(suite: &Suite<'_>, name: &str, opt: Option<&str>) -> Result<(), String> {
    header(&["fixed", name], opt);
    if name == "zzq" {
        let lanes = lanes(opt.unwrap_or("1"))?;
        suite.fixed(&zzq_who(suite, lanes, "default"), ("zzq", "", lanes), &[])?;
    } else if RIVALS.contains(&name) {
        let config = opt.unwrap_or("tuned");
        let extra: &[&str] = if name == "rumqttd" { &["v=4"] } else { &[] };
        suite.fixed(&rival_who(name, config), (name, config, 0), extra)?;
    } else {
        return Err(format!("unknown broker {name}"));
    }
    println!("# done {}", record::now());
    Ok(())
}

/// `durable <broker> [lanes]`: a fresh broker for each cell, so no cell
/// finds the sessions and messages of the one before.
fn durable(suite: &Suite<'_>, name: &str, opt: Option<&str>) -> Result<(), String> {
    header(&["durable", name], opt);
    let (who, broker, pinned) = if name == "zzq" {
        let lanes = lanes(opt.unwrap_or("1"))?;
        let who = zzq_who(suite, lanes, "durable");
        (who, ("zzq", "durable", lanes), zzq_pinned(lanes))
    } else if DURABLE.contains(&name) {
        (rival_who(name, "durable"), (name, "durable", 0), Vec::new())
    } else {
        return Err(format!("no durable config for {name}"));
    };
    suite.brokers(&who, broker, &suites::durable_cells(&pinned), true)?;
    println!("# done {}", record::now());
    Ok(())
}

/// `failover <broker> [config]`.
fn failover(suite: &Suite<'_>, name: &str, opt: Option<&str>) -> Result<(), String> {
    header(&["failover", name], opt);
    match name {
        "zzq" => suite.failover(&zzq_who(suite, 1, "cluster"), "zzq", "")?,
        "vernemq" => {
            let config = opt.unwrap_or("cluster");
            suite.failover(&rival_who(name, config), name, config)?;
        }
        "tbmq" => suite.failover(&rival_who(name, "cluster"), name, "cluster")?,
        _ => return Err(format!("no cluster of {name}")),
    }
    println!("# done {}", record::now());
    Ok(())
}

/// Every suite of `all`: its name and arguments.
fn all_suites() -> Vec<(String, Vec<String>)> {
    let s = |name: String, args: &[&str]| (name, args.iter().map(|&a| a.to_owned()).collect());
    let mut out = vec![
        s("zzq-1".into(), &["zzq", "1"]),
        s("zzq-2".into(), &["zzq", "2"]),
    ];
    out.extend(RIVALS.map(|r| s(format!("{r}-tuned"), &[r, "tuned"])));
    out.extend(DEFAULTS.map(|r| s(format!("{r}-default"), &[r, "default"])));
    out.push(s("clients".into(), &["clients"]));
    out.push(s("zzqclient".into(), &["zzqclient"]));
    out.push(s("fixed-zzq-1".into(), &["fixed", "zzq", "1"]));
    out.push(s("fixed-zzq-2".into(), &["fixed", "zzq", "2"]));
    out.extend(RIVALS.map(|r| {
        let config = if OWN_FIXED.contains(&r) {
            "fixed"
        } else {
            "tuned"
        };
        s(format!("fixed-{r}"), &["fixed", r, config])
    }));
    out.push(s("durable-zzq-1".into(), &["durable", "zzq", "1"]));
    out.push(s("durable-zzq-2".into(), &["durable", "zzq", "2"]));
    out.extend(DURABLE.map(|r| s(format!("durable-{r}"), &["durable", r])));
    out.push(s("failover-zzq".into(), &["failover", "zzq"]));
    out.push(s(
        "failover-vernemq-cluster".into(),
        &["failover", "vernemq", "cluster"],
    ));
    out.push(s(
        "failover-vernemq-netsplit".into(),
        &["failover", "vernemq", "netsplit"],
    ));
    out.push(s("failover-tbmq".into(), &["failover", "tbmq"]));
    out
}

/// Runs each named suite, or all, in its own process with its own log.
fn all(suite: &Suite<'_>, names: &[&str]) -> Result<(), String> {
    let suites = all_suites();
    let want: Vec<&(String, Vec<String>)> = if names.is_empty() {
        suites.iter().collect()
    } else {
        names
            .iter()
            .map(|n| {
                suites
                    .iter()
                    .find(|(name, _)| name == n)
                    .ok_or(format!("unknown suite {n}"))
            })
            .collect::<Result<_, _>>()?
    };
    let dir = zzq_bench::cache::dir().join("logs").join(record::today());
    fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let me = std::env::current_exe().map_err(|e| e.to_string())?;
    for (name, args) in want {
        let log = dir.join(format!("{name}.log"));
        println!("## {name} {} > {}", record::now(), log.display());
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .map_err(|e| format!("{}: {e}", log.display()))?;
        let err = file.try_clone().map_err(|e| e.to_string())?;
        let flags = [("--fill", suite.fill), ("--smoke", suite.smoke)];
        let ok = Command::new(&me)
            .args(flags.iter().filter(|(_, on)| *on).map(|(f, _)| f))
            .args(args)
            .stdin(Stdio::null())
            .stdout(file)
            .stderr(err)
            .status()
            .is_ok_and(|s| s.success());
        if !ok {
            return Err(format!("{name} failed, see {}", log.display()));
        }
    }
    println!("## all done {}", record::now());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_runs_every_suite_once() {
        let suites = all_suites();
        assert_eq!(suites.len(), 2 + 12 + 3 + 2 + 2 + 12 + 2 + 9 + 4);
        let find = |n: &str| suites.iter().find(|(name, _)| name == n).unwrap().1.clone();
        assert_eq!(find("fixed-nanomq"), ["fixed", "nanomq", "fixed"]);
        assert_eq!(find("fixed-emqx"), ["fixed", "emqx", "tuned"]);
        assert_eq!(find("saguin-default"), ["saguin", "default"]);
        assert_eq!(find("durable-zzq-2"), ["durable", "zzq", "2"]);
        assert_eq!(find("durable-tbmq"), ["durable", "tbmq"]);
    }

    #[test]
    fn who_rows_name_the_implementation() {
        assert_eq!(rival_who("emqx", "tuned")["version"], "6.3.2");
        assert_eq!(lanes("2"), Ok(2));
        assert!(lanes("0").is_err() && lanes("x").is_err());
        assert_eq!(zzq_pinned(1), Vec::<String>::new());
        assert_eq!(zzq_pinned(2), ["lanes=2", "cross=1"]);
    }
}

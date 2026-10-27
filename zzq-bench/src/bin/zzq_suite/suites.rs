//! The cells of each suite, and how a cell becomes a cache row.

use std::time::Duration;

use serde_json::Value;

use crate::brokers::{self, Running};
use crate::parse::{self, Row};
use crate::paths::Paths;
use crate::record::{Cache, fields};
use crate::runner::{self, Cmd, Ran};

/// Payload sizes of the full suites.
const SIZES: [usize; 4] = [16, 256, 1024, 4096];
/// CPUs of the load generator.
const LOAD_CPUS: &str = "4,5";
/// Fixed-load scenarios: publishers, subscribers (point to point has one
/// per publisher) and the offered deliveries per second, which the load
/// generator keeps under 180% CPU.
const FIXED: [(&str, usize, Option<usize>, &str); 3] = [
    ("p2p", 10_000, None, "10000,20000,40000,60000,80000"),
    ("fanin", 10_000, Some(100), "10000,20000,40000,60000,80000"),
    ("fanout", 5, Some(1000), "25000,50000,100000,150000,200000"),
];

/// One `zzq_brokerbench` run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Load {
    scenario: &'static str,
    qos: u8,
    size: usize,
    /// Publishers for pairs and fan-in, subscribers for fan-out.
    peers: Option<usize>,
    /// More `key=value` arguments. Later ones win.
    extra: Vec<String>,
}

impl Load {
    /// The cell it measures.
    fn cell(&self) -> Row {
        let mut cell = fields(&[("qos", self.qos.into()), ("msg_size", self.size.into())]);
        if self.scenario == "rtt" {
            cell.insert("kind".into(), "rtt".into());
            return cell;
        }
        let n = self.peers.unwrap_or(1);
        let (pubs, subs) = match self.scenario {
            "pairs" => (n, n),
            "fanout" => (1, n),
            _ => (n, 1),
        };
        cell.extend(fields(&[
            ("kind", "throughput".into()),
            ("scenario", self.scenario.into()),
            ("pubs", pubs.into()),
            ("subs", subs.into()),
        ]));
        cell
    }

    fn cmd(&self, p: &Paths, broker_pid: u32) -> Cmd {
        let mut cmd = Cmd::pinned(LOAD_CPUS, p.tool("zzq_brokerbench"))
            .arg(self.scenario)
            .arg(format!("qos={}", self.qos))
            .arg(format!("size={}", self.size));
        if let Some(n) = self.peers {
            let key = if self.scenario == "fanout" {
                "subs"
            } else {
                "pubs"
            };
            cmd = cmd.arg(format!("{key}={n}"));
        }
        cmd.args(&self.extra).arg(format!("cpu_pid={broker_pid}"))
    }
}

/// How a broker suite runs.
#[derive(Debug, Clone, Default)]
pub(crate) struct Shape {
    /// Every scenario, size and QoS, not the few default-config cells.
    pub(crate) full: bool,
    /// On every command, such as `v=4`.
    pub(crate) extra: Vec<String>,
    /// Seconds of warmup.
    pub(crate) warmup: u64,
    /// On the single-pair and round-trip commands, such as zzq lane
    /// placement.
    pub(crate) pinned: Vec<String>,
}

/// The cells of a broker suite.
pub(crate) fn broker_cells(shape: &Shape) -> Vec<Load> {
    let mut timed = vec![format!("warmup={}", shape.warmup), "secs=5".to_owned()];
    timed.extend(shape.extra.iter().cloned());
    let load = |scenario, qos, size, peers, extra: &[String]| Load {
        scenario,
        qos,
        size,
        peers,
        extra: extra.to_vec(),
    };
    if !shape.full {
        return vec![
            load("pairs", 0, 16, None, &timed),
            load("pairs", 1, 16, None, &timed),
            load("pairs", 0, 16, Some(16), &timed),
            load("fanin", 0, 16, Some(16), &timed),
            load("rtt", 0, 16, None, &shape.extra),
        ];
    }
    let mut pinned_timed = timed.clone();
    pinned_timed.extend(shape.pinned.iter().cloned());
    let mut cells = Vec::new();
    for (scenario, peers) in [
        ("pairs", None),
        ("pairs", Some(16)),
        ("fanout", Some(16)),
        ("fanin", Some(16)),
    ] {
        let extra = if peers.is_none() {
            &pinned_timed
        } else {
            &timed
        };
        for size in SIZES {
            for qos in 0..3 {
                cells.push(load(scenario, qos, size, peers, extra));
            }
        }
    }
    cells.push(load("pairs", 0, 16, Some(64), &timed));
    cells.push(load("pairs", 0, 1024, Some(64), &timed));
    cells.push(load("pairs", 1, 16, Some(64), &timed));
    // The load generator measures round trips at QoS 0 and 1 only.
    let mut rtt = shape.extra.clone();
    rtt.extend(shape.pinned.iter().cloned());
    for size in SIZES {
        for qos in 0..2 {
            cells.push(load("rtt", qos, size, None, &rtt));
        }
    }
    cells
}

/// A client library's bench.
struct Lib {
    name: &'static str,
    mqtt: &'static str,
    header: &'static str,
    /// Ports of the fake broker: sink, source.
    ports: (u16, u16),
    /// Messages per QoS: publishing, subscribing.
    n: ([usize; 3], [usize; 3]),
}

const LIBS: [Lib; 4] = [
    Lib {
        name: "rumqttc",
        mqtt: "3.1.1",
        header: "## rumqttc 0.25.1 (v3.1.1, current-thread Tokio)",
        ports: (18_858, 18_859),
        n: (
            [2_000_000, 500_000, 500_000],
            [10_000_000, 3_000_000, 2_000_000],
        ),
    },
    Lib {
        name: "ntex-mqtt",
        mqtt: "5.0",
        header: "## ntex-mqtt 9.3 (v5, ntex on current-thread Tokio)",
        ports: (18_850, 18_851),
        n: (
            [3_000_000, 500_000, 1_000_000],
            [3_000_000, 3_000_000, 2_000_000],
        ),
    },
    Lib {
        name: "emqtt",
        mqtt: "5.0",
        header: "## emqtt 1.14.6 (v5, OTP 27; pub QoS 1/2 with 100 caller processes)",
        ports: (18_852, 18_853),
        n: ([300_000, 150_000, 100_000], [3_000_000, 200_000, 100_000]),
    },
    Lib {
        name: "zzq-client",
        mqtt: "5.0",
        header: "## zzq-client (v5, current-thread Tokio caller, 1 IO lane)",
        ports: (18_856, 18_857),
        n: (
            [3_000_000, 500_000, 1_000_000],
            [3_000_000, 3_000_000, 2_000_000],
        ),
    },
];

/// One run of a suite: where things are and where results go.
#[derive(Debug)]
pub(crate) struct Suite<'a> {
    pub(crate) paths: &'a Paths,
    pub(crate) cache: &'a Cache,
    /// Skip cells the cache has for this implementation and config.
    pub(crate) fill: bool,
    /// The first cell of each part, briefly, without waiting for a quiet
    /// system; rows are printed, not recorded.
    pub(crate) smoke: bool,
}

impl Suite<'_> {
    fn bench(&self, cmds: &[Cmd], measure: usize, limit: Duration) -> Result<Ran, String> {
        runner::bench(cmds, measure, limit, !self.smoke)
    }

    fn skip(&self, file: &str, who: &Row, cell: &Row) -> bool {
        self.fill && self.cache.has(file, who, cell)
    }

    /// Runs `cells` against broker `name`: one broker for all of them, or a
    /// fresh one per cell with `fresh`.
    pub(crate) fn brokers(
        &self,
        who: &Row,
        broker: (&str, &str, usize),
        cells: &[Load],
        fresh: bool,
    ) -> Result<(), String> {
        let (name, config, lanes) = broker;
        let start = || brokers::start(name, config, lanes, self.paths);
        let shared = if fresh { None } else { Some(start()?) };
        let cells = if self.smoke {
            smoke_cells(cells)
        } else {
            cells.to_vec()
        };
        for load in &cells {
            let cell = load.cell();
            if self.skip("brokers", who, &cell) {
                continue;
            }
            let own: Option<Running> = if fresh { Some(start()?) } else { None };
            let pid = own.as_ref().or(shared.as_ref()).map_or(0, |b| b.pid);
            let cmd = load.cmd(self.paths, pid);
            let ran = self.bench(std::slice::from_ref(&cmd), 0, Duration::from_secs(120))?;
            drop(own);
            let row = ran
                .out
                .lines()
                .rev()
                .find_map(parse::broker_row)
                .ok_or_else(|| format!("no result in: {cmd}"))?;
            self.cache.record("brokers", who, row)?;
        }
        Ok(())
    }

    /// The fixed-load suite: a fresh broker per scenario, so the backlog of
    /// a failed step cannot slow the next one. Each step is a row.
    pub(crate) fn fixed(
        &self,
        who: &Row,
        broker: (&str, &str, usize),
        extra: &[&str],
    ) -> Result<(), String> {
        let (name, config, lanes) = broker;
        for (scenario, pubs, subs, rates) in FIXED {
            let cell = fields(&[
                ("kind", "fixed".into()),
                ("scenario", scenario.into()),
                ("qos", 1.into()),
                ("msg_size", 16.into()),
                ("pubs", pubs.into()),
                ("subs", subs.unwrap_or(pubs).into()),
            ]);
            if self.skip("fixed", who, &cell) {
                continue;
            }
            let running = brokers::start(name, config, lanes, self.paths)?;
            let mut cmd = Cmd::pinned(LOAD_CPUS, self.paths.tool("zzq_loadbench"))
                .arg(scenario)
                .arg(format!("pubs={pubs}"));
            if let Some(subs) = subs {
                cmd = cmd.arg(format!("subs={subs}"));
            }
            cmd = match (self.smoke, rates.split(',').next()) {
                (true, Some(first)) => cmd.arg(format!("rates={first}")).arg("secs=2"),
                _ => cmd.arg(format!("rates={rates}")),
            };
            let cmd = cmd.args(extra).arg(format!("cpu_pid={}", running.pid));
            let ran = self.bench(std::slice::from_ref(&cmd), 0, Duration::from_secs(300))?;
            drop(running);
            let rows: Vec<Row> = ran.out.lines().filter_map(parse::fixed_row).collect();
            if rows.is_empty() {
                return Err(format!("no result in: {cmd}"));
            }
            for row in rows {
                self.cache.record("fixed", who, row)?;
            }
            if self.smoke {
                break;
            }
        }
        Ok(())
    }

    /// The client suite: each library against the fake broker, or only
    /// zzq-client with `zzq`.
    pub(crate) fn clients(&self, zzq: bool) -> Result<(), String> {
        for lib in LIBS.iter().filter(|l| (l.name == "zzq-client") == zzq) {
            println!("{}", lib.header);
            let version = if zzq {
                crate::record::revision(&self.paths.repo, &["zzq-proto", "zzq-tokio", "zzq-client"])
            } else {
                brokers::version(lib.name).unwrap_or_default().to_owned()
            };
            let who = fields(&[
                ("impl", lib.name.into()),
                ("version", version.into()),
                ("mqtt", lib.mqtt.into()),
            ]);
            let sizes: &[usize] = if self.smoke { &[16] } else { &SIZES };
            for &size in sizes {
                for publish in [true, false] {
                    for qos in 0..3u8 {
                        let n = if self.smoke {
                            10_000
                        } else if publish {
                            lib.n.0[usize::from(qos)]
                        } else {
                            lib.n.1[usize::from(qos)]
                        };
                        self.client_cell(&who, lib, (publish, qos, size, n))?;
                        if self.smoke {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// One client run: the fake broker measures publishing, the client
    /// subscribing.
    fn client_cell(
        &self,
        who: &Row,
        lib: &Lib,
        (publish, qos, size, n): (bool, u8, usize, usize),
    ) -> Result<(), String> {
        let kind = if publish { "pub" } else { "sub" };
        let cell = fields(&[
            ("kind", kind.into()),
            ("qos", qos.into()),
            ("msg_size", size.into()),
        ]);
        if self.skip("clients", who, &cell) {
            return Ok(());
        }
        let port = if publish { lib.ports.0 } else { lib.ports.1 };
        let common = [
            port.to_string(),
            qos.to_string(),
            size.to_string(),
            n.to_string(),
        ];
        let fake = Cmd::new(self.paths.tool("zzq_fakebroker"))
            .arg(if publish { "sink" } else { "source" })
            .args(&common);
        let client = self.client_cmd(lib, kind)?.args(&common);
        let client = if lib.name == "emqtt" && publish {
            client.arg(if qos == 0 { "1" } else { "100" })
        } else {
            client
        };
        let measure = usize::from(!publish);
        let ran = self.bench(&[fake, client], measure, Duration::from_secs(120))?;
        let mut row = ran
            .out
            .lines()
            .rev()
            .filter_map(parse::client_row)
            .find(|r| r.get("kind") == Some(&Value::from(kind)))
            .ok_or_else(|| format!("no {kind} result for {} qos={qos} size={size}", lib.name))?;
        if let Some(cpu) = ran.cpu.get(1).copied().flatten() {
            row.insert("cpu_pct".into(), cpu.into());
        }
        self.cache.record("clients", who, row)
    }

    /// The client command, up to its mode.
    fn client_cmd(&self, lib: &Lib, kind: &str) -> Result<Cmd, String> {
        let p = self.paths;
        Ok(match lib.name {
            "rumqttc" => Cmd::new(p.tool("zzq_rumqttc_bench")),
            "ntex-mqtt" => Cmd::new(p.tool("zzq_ntex_bench")),
            "emqtt" => Cmd::new("escript")
                .arg(p.harness.join("emqtt_bench.escript"))
                .arg(p.rival("emqtt-bench/lib")?),
            _ => Cmd::new(p.tool("zzq_client_bench")),
        }
        .arg(kind))
    }
}

/// The first cell, briefly.
fn smoke_cells(cells: &[Load]) -> Vec<Load> {
    cells
        .iter()
        .take(1)
        .cloned()
        .map(|mut l| {
            l.extra.extend(["warmup=0".to_owned(), "secs=1".to_owned()]);
            l
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_full_suite_covers_every_scenario_size_and_qos() {
        let shape = Shape {
            full: true,
            extra: vec!["v=4".into()],
            warmup: 5,
            pinned: vec!["lanes=2".into(), "cross=1".into()],
        };
        let cells = broker_cells(&shape);
        assert_eq!(cells.len(), 4 * 4 * 3 + 3 + 4 * 2);
        let p = Paths::from_env();
        let first = cells[0].cmd(&p, 7).to_string();
        assert!(
            first.ends_with(
                "zzq_brokerbench pairs qos=0 size=16 warmup=5 secs=5 v=4 lanes=2 cross=1 cpu_pid=7"
            ),
            "{first}"
        );
        let fanout = cells.iter().find(|l| l.scenario == "fanout").unwrap();
        assert!(
            fanout
                .cmd(&p, 7)
                .to_string()
                .contains("size=16 subs=16 warmup=5")
        );
        assert_eq!(
            Value::Object(fanout.cell()),
            json!({"kind": "throughput", "scenario": "fanout", "qos": 0, "msg_size": 16,
                   "pubs": 1, "subs": 16})
        );
        let rtt = cells.last().unwrap();
        assert_eq!(rtt.extra, ["v=4", "lanes=2", "cross=1"]);
        assert_eq!(
            Value::Object(rtt.cell()),
            json!({"kind": "rtt", "qos": 1, "msg_size": 4096})
        );
    }

    #[test]
    fn a_default_suite_and_a_smoke_run_are_short() {
        let shape = Shape {
            warmup: 1,
            ..Shape::default()
        };
        let cells = broker_cells(&shape);
        assert_eq!(cells.len(), 5);
        assert_eq!(cells[4].extra, Vec::<String>::new());
        let pairs = cells[2].cell();
        assert_eq!((&pairs["pubs"], &pairs["subs"]), (&json!(16), &json!(16)));
        assert_eq!(cells[3].cell()["subs"], 1);
        let smoke = smoke_cells(&cells);
        assert_eq!(smoke.len(), 1);
        assert_eq!(smoke[0].extra, ["warmup=1", "secs=5", "warmup=0", "secs=1"]);
    }
}

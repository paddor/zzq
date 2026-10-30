//! Comparison charts from the result cache.
//!
//! Reads the rows `zzq_suite` appends to `brokers.jsonl`,
//! `clients.jsonl` and `fixed.jsonl` in `$XDG_CACHE_HOME/zzq`
//! (`~/.cache/zzq`), and writes `doc/charts/brokers.svg`, `clients.svg`,
//! `fixed-load.svg` and `durable.svg` under the current directory. Per cell, the newest run
//! wins. The line under the title comes from `.chart_hw` in the repo root
//! (ignored), with `prefix=` and `postfix=` lines, or from `ZZQ_HW_PREFIX`
//! and `ZZQ_HW_POSTFIX`.
//!
//! Usage: `zzq_chart [brokers|clients|fixed|durable]`

mod data;
mod draw;
mod fixed;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use data::{BrokerRow, ClientRow, FixedRow};
use draw::{Chart, CpuShow, Impl, Key, Latency, Result, Row, RowData, RttAxis};
use fixed::{FixedChart, Scenario, Step};
use plotters::style::RGBColor;

const SIZES: &[u64] = &[16, 256, 1024, 4096];

const RED: RGBColor = RGBColor(239, 68, 68);
const ORANGE: RGBColor = RGBColor(249, 115, 22);
const YELLOW: RGBColor = RGBColor(250, 204, 21);
const PURPLE: RGBColor = RGBColor(168, 85, 247);
const BLUE: RGBColor = RGBColor(96, 165, 250);
const CYAN: RGBColor = RGBColor(34, 211, 238);
const GREEN: RGBColor = RGBColor(16, 185, 129);
const LIME: RGBColor = RGBColor(190, 242, 100);
const PINK: RGBColor = RGBColor(244, 114, 182);
const WHITE: RGBColor = RGBColor(243, 244, 246);
const BROWN: RGBColor = RGBColor(180, 83, 9);
const INDIGO: RGBColor = RGBColor(99, 102, 241);
const TAN: RGBColor = RGBColor(187, 170, 153);
const MAGENTA: RGBColor = RGBColor(255, 17, 255);
const OLIVE: RGBColor = RGBColor(154, 160, 30);

const fn rival(
    key: &'static str,
    label: &'static str,
    runs_on: &'static str,
    color: RGBColor,
) -> Impl {
    Impl {
        key,
        config: "tuned",
        label,
        runs_on,
        color,
        ours: false,
    }
}

const BROKERS: &[Impl] = &[
    Impl {
        key: "zzq-1io",
        config: "default",
        label: "zzq",
        runs_on: "1 IO lane",
        color: RED,
        ours: true,
    },
    Impl {
        key: "zzq-2io",
        config: "default",
        label: "zzq",
        runs_on: "2 IO lanes",
        color: ORANGE,
        ours: true,
    },
    rival("rmqtt", "rmqtt", "CPUs 0-3", YELLOW),
    rival("emqx", "EMQX", "CPUs 0-3", PURPLE),
    rival("mosquitto", "Mosquitto", "CPUs 0-3, 1 thread", BLUE),
    rival("nanomq", "NanoMQ", "CPUs 0-3", CYAN),
    rival("rumqttd", "rumqttd", "CPUs 0-3", GREEN),
    rival("xmq", "XMQ", "CPUs 0-3", LIME),
    rival("flashmq", "FlashMQ", "CPUs 0-3, 4 threads", PINK),
    rival("vernemq", "VerneMQ", "CPUs 0-3", WHITE),
    rival("fluxmq", "FluxMQ", "CPUs 0-3", BROWN),
    rival("saguin", "Sag\u{fc}in", "CPUs 0-3", INDIGO),
    rival("hivemq", "HiveMQ CE", "CPUs 0-3", TAN),
    rival(
        "tbmq",
        "TBMQ",
        "CPUs 0-3, with Kafka, Valkey, PostgreSQL",
        MAGENTA,
    ),
    rival("nats", "NATS", "CPUs 0-3, MQTT 3.1.1", OLIVE),
];

const fn stored(
    key: &'static str,
    label: &'static str,
    runs_on: &'static str,
    color: RGBColor,
) -> Impl {
    Impl {
        key,
        config: "durable",
        label,
        runs_on,
        color,
        ours: false,
    }
}

/// zzq and the rivals with their storage on, as `zzq_suite durable` runs
/// them. The colors match [`BROKERS`].
const DURABLE: &[Impl] = &[
    Impl {
        key: "zzq-1io",
        config: "durable",
        label: "zzq",
        runs_on: "1 IO lane, data dir",
        color: RED,
        ours: true,
    },
    Impl {
        key: "zzq-2io",
        config: "durable",
        label: "zzq",
        runs_on: "2 IO lanes, data dir",
        color: ORANGE,
        ours: true,
    },
    stored("rmqtt", "rmqtt", "sled sessions, Redis messages", YELLOW),
    stored("emqx", "EMQX", "durable sessions", PURPLE),
    stored("mosquitto", "Mosquitto", "SQLite plugin", BLUE),
    stored("nanomq", "NanoMQ", "SQLite", CYAN),
    stored("xmq", "XMQ", "Redis, Super-Durable", LIME),
    stored("vernemq", "VerneMQ", "LevelDB", WHITE),
    stored("saguin", "Sag\u{fc}in", "SQLite", INDIGO),
    stored("hivemq", "HiveMQ CE", "file persistence", TAN),
    stored("tbmq", "TBMQ", "Kafka, Valkey, PostgreSQL", MAGENTA),
    stored("nats", "NATS", "JetStream, every write synced", OLIVE),
];

/// Broker throughput rows: scenario, publishers, subscribers, title.
const BROKER_ROWS: &[(&str, u32, u32, &str)] = &[
    ("pairs", 1, 1, "1 publisher \u{2192} 1 subscriber"),
    (
        "pairs",
        16,
        16,
        "16 pairs: 16 publishers \u{2192} 16 subscribers, a topic each",
    ),
    (
        "fanout",
        1,
        16,
        "fan-out: 1 publisher \u{2192} 16 subscribers (delivered)",
    ),
    (
        "fanin",
        16,
        1,
        "fan-in: 16 publishers \u{2192} 1 subscriber",
    ),
];

const CLIENTS: &[Impl] = &[
    Impl {
        key: "zzq-client",
        config: "",
        label: "zzq-client",
        runs_on: "current-thread Tokio + 1 IO lane",
        color: RED,
        ours: true,
    },
    rival("rumqttc", "rumqttc", "current-thread Tokio", GREEN),
    rival(
        "ntex-mqtt",
        "ntex-mqtt",
        "ntex on current-thread Tokio",
        BLUE,
    ),
    rival("emqtt", "emqtt", "Erlang/OTP 27", PURPLE),
];

/// Client rows: kind and title.
const CLIENT_ROWS: &[(&str, &str)] = &[
    ("pub", "publish: client \u{2192} fake broker"),
    ("sub", "subscribe: fake broker \u{2192} client"),
];

fn main() -> ExitCode {
    let charts: &[fn() -> Result] = match std::env::args().nth(1).as_deref() {
        None => &[brokers, clients, fixed_load, durable],
        Some("brokers") => &[brokers],
        Some("durable") => &[durable],
        Some("clients") => &[clients],
        Some("fixed") => &[fixed_load],
        Some(_) => {
            eprintln!("usage: zzq_chart [brokers|clients|fixed|durable]");
            return ExitCode::FAILURE;
        }
    };
    for chart in charts {
        if let Err(e) = chart() {
            eprintln!("zzq_chart: {e}");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

/// What a broker row measured: impl, kind, scenario, publishers,
/// subscribers, QoS and size.
type BrokerCell = (
    &'static str,
    String,
    Option<String>,
    Option<u32>,
    Option<u32>,
    u8,
    u64,
);

fn brokers() -> Result {
    let rows = newest_brokers(BROKERS);
    let chart = Chart {
        out: out_path("brokers.svg"),
        title: "MQTT brokers, TCP loopback",
        sizes: SIZES,
        qos: 0..3,
        impls: BROKERS,
        versions: versions(rows.values().map(|r| (&r.impl_name, &r.run_id, &r.version))),
        rows: broker_rows(
            &rows,
            "round trip, 1 message in flight: p99, whiskers p50 to p99.9",
            RttAxis::Linear,
        ),
        cpu: CpuShow::BelowAxis,
        cpu_of: "broker",
        notes: &[
            "Tuned configs. Broker on CPUs 0-3, load generator (zzq_brokerbench, raw sockets) \
             on CPUs 4-5. 1 s warmup (rumqttd 5 s, while its message log grows), then 5 s.",
            "Throughput is delivered messages. QoS 1/2 publishers keep 100 in flight; QoS 0 \
             has no flow control, so it can trail QoS 1 at large sizes.",
            "Where brokers drop messages, close subscribers or stall under this load: \
             COMPARISONS.md.",
            "zzq on 2 IO lanes: the single pair and the round trip cross lanes. rumqttd and NATS \
             speak MQTT 3.1.1, the rest 5.0. Round trips at QoS 0 and 1 only.",
        ],
    };
    write(&chart)
}

fn durable() -> Result {
    let rows = newest_brokers(DURABLE);
    let chart = Chart {
        out: out_path("durable.svg"),
        title: "MQTT brokers with storage on, TCP loopback",
        sizes: SIZES,
        qos: 1..3,
        impls: DURABLE,
        versions: versions(rows.values().map(|r| (&r.impl_name, &r.run_id, &r.version))),
        rows: broker_rows(
            &rows,
            "time to PUBACK, 1 QoS 1 message in flight: p99, whiskers p50 to p99.9",
            RttAxis::Log,
        ),
        cpu: CpuShow::BelowAxis,
        cpu_of: "broker",
        notes: DURABLE_NOTES,
    };
    write(&chart)
}

const DURABLE_NOTES: &[&str] = &[
    "Subscribers keep their sessions (clean start off, no expiry), so a broker with storage keeps \
     each QoS 1/2 message. Stores on an SSD (XFS): a synced 4 KiB write takes 0.7 ms into \
     allocated blocks, 1.8 ms when it extends a file.",
    "Each broker runs its most durable setting. Most send the PUBACK before the write is synced: \
     COMPARISONS.md says which wait.",
    "A fresh broker and store per cell. 1 s warmup, then 5 s. QoS 1/2 publishers keep 100 in \
     flight. Broker on CPUs 0-3, with the stores it needs; its CPU sums them.",
    "Time to PUBACK: one QoS 1 publish in flight, from its send to its PUBACK.",
];

/// The newest result per broker cell of `impls`.
fn newest_brokers(impls: &'static [Impl]) -> BTreeMap<BrokerCell, BrokerRow> {
    data::newest(
        data::load::<BrokerRow>("brokers.jsonl")
            .into_iter()
            .filter_map(|r| {
                let imp = impls
                    .iter()
                    .find(|i| i.key == r.impl_name && i.config == r.config)?;
                let cell = (
                    imp.key,
                    r.kind.clone(),
                    r.scenario.clone(),
                    r.pubs,
                    r.subs,
                    r.qos,
                    r.msg_size,
                );
                Some((cell, r))
            }),
        |r| &r.run_id,
    )
}

/// The throughput rows, then the round-trip row titled `rtt_title`.
fn broker_rows(
    rows: &BTreeMap<BrokerCell, BrokerRow>,
    rtt_title: &'static str,
    rtt_axis: RttAxis,
) -> Vec<Row> {
    let mut tput: Vec<BTreeMap<Key, f64>> = vec![BTreeMap::new(); BROKER_ROWS.len()];
    let mut cpu: Vec<BTreeMap<Key, f64>> = vec![BTreeMap::new(); BROKER_ROWS.len() + 1];
    let mut rtt = BTreeMap::new();
    for ((key, kind, scenario, pubs, subs, qos, size), r) in rows {
        let cell = (*key, *qos, *size);
        let row = if kind == "rtt" {
            if let (Some(p50), Some(p99), Some(p999)) = (r.p50_us, r.p99_us, r.p999_us) {
                rtt.insert(cell, Latency { p50, p99, p999 });
            }
            BROKER_ROWS.len()
        } else if let Some(i) = BROKER_ROWS.iter().position(|&(s, p, b, _)| {
            scenario.as_deref() == Some(s) && *pubs == Some(p) && *subs == Some(b)
        }) && let Some(msgs_s) = r.msgs_s
        {
            tput[i].insert(cell, msgs_s);
            i
        } else {
            continue;
        };
        if let Some(c) = r.cpu_pct {
            cpu[row].insert(cell, c);
        }
    }

    let mut cpu = cpu.into_iter();
    let mut chart_rows: Vec<Row> = BROKER_ROWS
        .iter()
        .zip(tput)
        .zip(cpu.by_ref())
        .map(|((&(_, _, _, title), m), cpu)| Row {
            title,
            data: RowData::Throughput(m),
            cpu,
        })
        .collect();
    chart_rows.push(Row {
        title: rtt_title,
        data: RowData::Rtt(rtt, rtt_axis),
        cpu: cpu.next().unwrap_or_default(),
    });
    chart_rows
}

fn clients() -> Result {
    let rows = data::newest(
        data::load::<ClientRow>("clients.jsonl")
            .into_iter()
            .filter_map(|r| {
                let imp = CLIENTS.iter().find(|i| i.key == r.impl_name)?;
                Some(((imp.key, r.kind.clone(), r.qos, r.msg_size), r))
            }),
        |r| &r.run_id,
    );

    let chart_rows = CLIENT_ROWS
        .iter()
        .map(|&(kind, title)| {
            let mine = || rows.iter().filter(move |((_, k, _, _), _)| k == kind);
            Row {
                title,
                data: RowData::Throughput(
                    mine()
                        .map(|(&(key, _, qos, size), r)| ((key, qos, size), r.msgs_s))
                        .collect(),
                ),
                cpu: mine()
                    .filter_map(|(&(key, _, qos, size), r)| Some(((key, qos, size), r.cpu_pct?)))
                    .collect(),
            }
        })
        .collect();
    let versions = versions(rows.values().map(|r| (&r.impl_name, &r.run_id, &r.version)));
    let chart = Chart {
        out: out_path("clients.svg"),
        title: "MQTT clients, one connection, TCP loopback",
        sizes: SIZES,
        qos: 0..3,
        impls: CLIENTS,
        versions,
        rows: chart_rows,
        cpu: CpuShow::BelowAxis,
        cpu_of: "client",
        notes: &[
            "Against zzq_fakebroker, 100 QoS 1/2 messages in flight: it counts what a \
             publishing client sends; a subscribing client counts what it receives.",
            "zzq-client acks a QoS 1/2 message once the application took it; rumqttc acks on \
             read. rumqttc speaks MQTT 3.1.1, the rest 5.0.",
            "emqtt publishes QoS 1 and 2 from 100 processes.",
        ],
    };
    write(&chart)
}

/// Fixed-load rows: scenario, offered deliveries per second, title.
const FIXED_ROWS: &[(&str, &[u64], &str)] = &[
    (
        "p2p",
        &[10_000, 20_000, 40_000, 60_000, 80_000],
        "point to point: 10K publishers \u{2192} 10K subscribers, a topic each (20K connections)",
    ),
    (
        "fanin",
        &[10_000, 20_000, 40_000, 60_000, 80_000],
        "fan-in: 10K publishers \u{2192} a shared subscription of 100",
    ),
    (
        "fanout",
        &[25_000, 50_000, 100_000, 150_000, 200_000],
        "fan-out: 5 publishers \u{2192} 1000 subscribers (deliveries/s)",
    ),
];

fn fixed_load() -> Result {
    let rows = data::newest(
        latest_runs(
            data::load::<FixedRow>("fixed.jsonl")
                .into_iter()
                .filter_map(|r| {
                    // NanoMQ runs this test with its queue at the default.
                    let imp = BROKERS.iter().find(|i| {
                        i.key == r.impl_name && (i.config == r.config || r.config == "fixed")
                    })?;
                    Some(((imp.key, r.scenario.clone(), r.offered_s), r))
                })
                .collect(),
        ),
        |r| &r.run_id,
    );
    let scenarios = FIXED_ROWS
        .iter()
        .map(|&(scenario, rates, title)| Scenario {
            title,
            rates: rates.to_vec(),
            steps: fixed_steps(&rows, scenario),
        })
        .collect();
    let chart = FixedChart {
        out: out_path("fixed-load.svg"),
        title: "MQTT brokers at fixed load, TCP loopback",
        impls: BROKERS,
        versions: versions(rows.values().map(|r| (&r.impl_name, &r.run_id, &r.version))),
        rows: scenarios,
        notes: FIXED_NOTES,
    };
    fixed::draw(&chart)?;
    println!("Written: {}", chart.out.display());
    Ok(())
}

const FIXED_NOTES: &[&str] = &[
    "QoS 1, 16 B, MQTT 5 (rumqttd and NATS 3.1.1). Each publisher sends on a fixed schedule; the \
     offered rate counts deliveries. Steps run 2 s, then measure 8 s, on the same connections.",
    "Latency runs from the send to delivery, or from when it was due if the broker held it back. \
     Seconds with host CPU steal are left out. Broker on CPUs 0-3, load generator on CPUs 4-5.",
    "A step fails under 95% delivered, on a closed connection, or at a median over 100 ms; a \
     cross marks it and no higher step runs.",
    "A broker that drops clients while they connect fails its first step with 0% delivered. \
     Fan-out latency includes the generator reading each burst of 1000 deliveries.",
    "Tuned configs, except where they run out of memory at 20K connections: NanoMQ's queue \
     (2048), FluxMQ's send queue (1000), VerneMQ's socket buffers (128 KiB). See COMPARISONS.md.",
    "NATS has no shared subscriptions: it takes the fan-in filter as a plain one, which matches no \
     topic.",
];

/// One scenario's steps per impl and offered rate.
type FixedKey = (&'static str, String, u64);

/// The rows of each impl's newest run of each scenario. A run stops at its
/// first failed step, so taking steps above it from an older run would mix
/// runs.
fn latest_runs(rows: Vec<(FixedKey, FixedRow)>) -> Vec<(FixedKey, FixedRow)> {
    let mut latest: BTreeMap<(&str, String), String> = BTreeMap::new();
    for ((key, scenario, _), r) in &rows {
        let e = latest.entry((*key, scenario.clone())).or_default();
        if r.run_id > *e {
            e.clone_from(&r.run_id);
        }
    }
    rows.into_iter()
        .filter(|((key, scenario, _), r)| latest[&(*key, scenario.clone())] == r.run_id)
        .collect()
}

fn fixed_steps(
    rows: &BTreeMap<FixedKey, FixedRow>,
    scenario: &str,
) -> BTreeMap<(&'static str, u64), Step> {
    let steps: BTreeMap<_, _> = rows
        .iter()
        .filter(|((_, s, _), _)| s == scenario)
        .map(|(&(key, _, rate), r)| {
            let step = Step {
                lat: Latency {
                    p50: r.p50_us,
                    p99: r.p99_us,
                    p999: r.p999_us,
                },
                cpu: r.cpu_pct,
                rss: r.rss_mib,
                delivered_pct: 100.0 * r.msgs_s / rate as f64,
                closed: r.closed,
                failed: r.failed,
            };
            ((key, rate), step)
        })
        .collect();
    for imp in BROKERS {
        if !steps.keys().any(|&(k, _)| k == imp.key) {
            eprintln!("missing: fixed {scenario} | {} {}", imp.label, imp.runs_on);
        }
    }
    steps
}

/// The version of each impl's newest run that has one.
fn versions<'a>(
    rows: impl Iterator<Item = (&'a String, &'a String, &'a Option<String>)>,
) -> BTreeMap<&'static str, String> {
    let mut newest: BTreeMap<&str, (&str, &str)> = BTreeMap::new();
    for (name, run_id, version) in rows {
        let Some(version) = version else { continue };
        let e = newest.entry(name).or_insert((run_id, version));
        if run_id.as_str() >= e.0 {
            *e = (run_id, version);
        }
    }
    BROKERS
        .iter()
        .chain(CLIENTS)
        .filter_map(|i| Some((i.key, newest.get(i.key)?.1.to_owned())))
        .collect()
}

fn out_path(name: &str) -> PathBuf {
    PathBuf::from("doc/charts").join(name)
}

/// Lists the cells a chart has no result for, then draws it.
fn write(chart: &Chart<'_>) -> Result {
    for row in &chart.rows {
        for imp in chart.impls {
            for qos in chart.qos.clone().filter(|&q| q < row.data.qos_levels()) {
                let missing: Vec<String> = chart
                    .sizes
                    .iter()
                    .filter(|&&size| !row.data.has(&(imp.key, qos, size)))
                    .map(u64::to_string)
                    .collect();
                if !missing.is_empty() {
                    eprintln!(
                        "missing: {} | {} {} QoS {qos} | sizes {}",
                        row.title,
                        imp.label,
                        imp.runs_on,
                        missing.join(", ")
                    );
                }
            }
        }
    }
    draw::draw(chart)?;
    println!("Written: {}", chart.out.display());
    Ok(())
}

//! Comparison charts from the result cache.
//!
//! Reads the rows `tmp/mqtt-bench/run.rb` appends to `brokers.jsonl` and
//! `clients.jsonl` in `$XDG_CACHE_HOME/zzq` (`~/.cache/zzq`), and writes
//! `doc/charts/brokers.svg` and `doc/charts/clients.svg` under the current
//! directory. Per cell, the newest run wins. The line under the title comes
//! from `.chart_hw` in the repo root (ignored), with `prefix=` and
//! `postfix=` lines, or from `ZZQ_HW_PREFIX` and `ZZQ_HW_POSTFIX`.
//!
//! Usage: `zzq_chart [brokers|clients]`

mod data;
mod draw;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use data::{BrokerRow, ClientRow};
use draw::{Chart, Impl, Key, Latency, Result, Row, RowData};
use plotters::style::RGBColor;

const SIZES: &[u64] = &[16, 256, 1024, 4096];

const RED: RGBColor = RGBColor(239, 68, 68);
const ORANGE: RGBColor = RGBColor(249, 115, 22);
const YELLOW: RGBColor = RGBColor(250, 204, 21);
const PURPLE: RGBColor = RGBColor(168, 85, 247);
const BLUE: RGBColor = RGBColor(96, 165, 250);
const CYAN: RGBColor = RGBColor(34, 211, 238);
const GREEN: RGBColor = RGBColor(16, 185, 129);

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
        None => &[brokers, clients],
        Some("brokers") => &[brokers],
        Some("clients") => &[clients],
        Some(_) => {
            eprintln!("usage: zzq_chart [brokers|clients]");
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
    let rows = newest_brokers();
    let chart = Chart {
        out: out_path("brokers.svg"),
        title: "MQTT brokers, TCP loopback",
        sizes: SIZES,
        impls: BROKERS,
        versions: versions(rows.values().map(|r| (&r.impl_name, &r.run_id, &r.version))),
        rows: broker_rows(&rows),
        notes: [
            "Tuned configs. Broker on CPUs 0-3, load generator (zzq_brokerbench, raw sockets) \
             on CPUs 4-5. 1 s warmup, then 5 s.",
            "Throughput is delivered messages. rumqttd speaks MQTT 3.1.1, the rest 5.0. \
             Round trips at QoS 0 and 1 only.",
        ],
    };
    write(&chart)
}

/// The newest result per broker cell.
fn newest_brokers() -> BTreeMap<BrokerCell, BrokerRow> {
    data::newest(
        data::load::<BrokerRow>("brokers.jsonl")
            .into_iter()
            .filter_map(|r| {
                let imp = BROKERS
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

/// The throughput rows, then the round-trip row.
fn broker_rows(rows: &BTreeMap<BrokerCell, BrokerRow>) -> Vec<Row> {
    let mut tput: Vec<BTreeMap<Key, f64>> = vec![BTreeMap::new(); BROKER_ROWS.len()];
    let mut rtt = BTreeMap::new();
    for ((key, kind, scenario, pubs, subs, qos, size), r) in rows {
        let cell = (*key, *qos, *size);
        if kind == "rtt" {
            if let (Some(p50), Some(p99), Some(p999)) = (r.p50_us, r.p99_us, r.p999_us) {
                rtt.insert(cell, Latency { p50, p99, p999 });
            }
        } else if let Some(i) = BROKER_ROWS.iter().position(|&(s, p, b, _)| {
            scenario.as_deref() == Some(s) && *pubs == Some(p) && *subs == Some(b)
        }) && let Some(msgs_s) = r.msgs_s
        {
            tput[i].insert(cell, msgs_s);
        }
    }

    let mut chart_rows: Vec<Row> = BROKER_ROWS
        .iter()
        .zip(tput)
        .map(|(&(_, _, _, title), m)| Row {
            title,
            data: RowData::Throughput(m),
        })
        .collect();
    chart_rows.push(Row {
        title: "round trip, 1 message in flight: p99, whiskers p50 to p99.9",
        data: RowData::Rtt(rtt),
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
        .map(|&(kind, title)| Row {
            title,
            data: RowData::Throughput(
                rows.iter()
                    .filter(|((_, k, _, _), _)| k == kind)
                    .map(|(&(key, _, qos, size), r)| ((key, qos, size), r.msgs_s))
                    .collect(),
            ),
        })
        .collect();
    let versions = versions(rows.values().map(|r| (&r.impl_name, &r.run_id, &r.version)));
    let chart = Chart {
        out: out_path("clients.svg"),
        title: "MQTT clients, one connection, TCP loopback",
        sizes: SIZES,
        impls: CLIENTS,
        versions,
        rows: chart_rows,
        notes: [
            "Against zzq_fakebroker: it counts what a publishing client sends; a subscribing \
             client counts what it receives.",
            "rumqttc speaks MQTT 3.1.1, the rest 5.0. emqtt publishes QoS 1 and 2 from 100 \
             processes.",
        ],
    };
    write(&chart)
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
            for qos in 0..row.data.qos_levels() {
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

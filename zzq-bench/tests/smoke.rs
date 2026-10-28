//! Short runs of the load generator and the perf gate against a real
//! broker. They check that the tools work, not how fast the broker is.

#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use zzq_bench::fixed::{self, Fixed, Report};
use zzq_bench::load::{self, Keep, Load, Scenario};
use zzq_broker::{Authorizer, Broker, BrokerOptions, ClientInfo, Context};

async fn broker() -> (Context, Broker, String) {
    broker_with(BrokerOptions::default()).await
}

async fn broker_with(opts: BrokerOptions) -> (Context, Broker, String) {
    let ctx = Context::new().unwrap();
    let broker = Broker::new(&ctx, opts).await.unwrap();
    let endpoint = broker.bind("tcp://127.0.0.1:0").await.unwrap();
    let addr = endpoint.socket_addr_string().unwrap();
    (ctx, broker, addr)
}

fn short(addr: &str) -> Load {
    Load {
        addr: addr.to_owned(),
        warmup: Duration::ZERO,
        measure: Duration::from_millis(100),
        ..Load::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn every_scenario_moves_messages() {
    let (_ctx, _broker, addr) = broker().await;
    let cases = [
        (Scenario::Pairs, 0, true, 1),
        (Scenario::Pairs, 1, false, 1),
        (Scenario::Pairs, 2, true, 1),
        (Scenario::Fanout, 1, true, 1),
        (Scenario::Fanin, 0, true, 3),
    ];
    for (scenario, qos, v5, topics) in cases {
        let load = Load {
            qos,
            v5,
            subs: 2,
            pubs: 2,
            topics,
            ..short(&addr)
        };
        let t = tokio::task::spawn_blocking(move || load::throughput(scenario, &load))
            .await
            .unwrap()
            .unwrap();
        assert!(
            t.delivered > 0.0 && t.closed == 0,
            "{scenario:?} qos {qos}: {t:?}"
        );
    }
    let rtt = Load {
        qos: 1,
        ..short(&addr)
    };
    let l = tokio::task::spawn_blocking(move || load::latency(&rtt, 50))
        .await
        .unwrap()
        .unwrap();
    assert!(l.p50 > 0.0 && l.lost == 0, "{l:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn kept_publishes_and_sessions_stay_in_the_broker() {
    let opts = BrokerOptions {
        sys_interval: None,
        ..BrokerOptions::default()
    };
    let (_ctx, broker, addr) = broker_with(opts).await;
    let load = Load {
        qos: 1,
        pubs: 2,
        keep: Keep {
            retain: true,
            sessions: true,
        },
        ..short(&addr)
    };
    let t = tokio::task::spawn_blocking(move || load::throughput(Scenario::Pairs, &load))
        .await
        .unwrap()
        .unwrap();
    assert!(t.delivered > 0.0 && t.closed == 0, "{t:?}");
    let stats = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let stats = broker.stats().await.unwrap();
            if stats.connections == 0 {
                return stats;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!((stats.retained, stats.sessions), (2, 2), "{stats:?}");
}

/// Lets no one publish.
struct NoPublish;

impl Authorizer for NoPublish {
    fn may_publish(&self, _: &ClientInfo<'_>, _: &str) -> bool {
        false
    }

    fn may_subscribe(&self, _: &ClientInfo<'_>, _: &str) -> bool {
        true
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_the_broker_refuses_count_as_nacks() {
    let opts = BrokerOptions {
        authorizer: Some(Arc::new(NoPublish)),
        ..BrokerOptions::default()
    };
    let (_ctx, _broker, addr) = broker_with(opts).await;
    for qos in [1, 2] {
        let load = Load {
            qos,
            ..short(&addr)
        };
        let t = tokio::task::spawn_blocking(move || load::throughput(Scenario::Pairs, &load))
            .await
            .unwrap()
            .unwrap();
        assert!(
            t.nacks > 100 && t.delivered == 0.0 && t.closed == 0,
            "qos {qos}: {t:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_load_generator_runs_from_the_command_line() {
    let (_ctx, _broker, addr) = broker().await;
    let exe = env!("CARGO_BIN_EXE_zzq_brokerbench");
    let pid = format!("cpu_pid={}", std::process::id());
    let out = Command::new(exe)
        .args(["rtt", &format!("addr={addr}"), "iters=50", "lanes=2"])
        .args(["cross=1", &pid])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("rtt v5") && stdout.contains("lost 0  cpu "),
        "{stdout}"
    );
    let out = Command::new(exe)
        .args(["pairs", &format!("addr={addr}"), "warmup=0", "secs=1"])
        .arg(&pid)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("closed 0 nacks 0  cpu "),
        "{stdout}"
    );
    let out = Command::new(exe).arg("nope").output().unwrap();
    assert!(!out.status.success());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_fixed_load_test_runs_every_scenario_from_the_command_line() {
    let (_ctx, _broker, addr) = broker().await;
    let port = addr.rsplit_once(':').unwrap().1.to_owned();
    let exe = env!("CARGO_BIN_EXE_zzq_loadbench");
    let pid = std::process::id().to_string();
    let runs: Vec<_> = [
        ("p2p", "pubs=10", "subs=0", "trace=1"),
        ("fanin", "pubs=10", "subs=2", "trace=0"),
        ("fanout", "pubs=2", "subs=5", "trace=0"),
    ]
    .into_iter()
    .map(|(scenario, pubs, subs, trace)| {
        let child = Command::new(exe)
            .args([scenario, pubs, subs, trace, "rates=200,400", "warmup=1.2"])
            .args(["secs=1", "addrs=1", "connect_rate=1000"])
            .arg(format!("port={port}"))
            .arg(format!("cpu_pid={pid}"))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        (scenario, child)
    })
    .collect();
    for (scenario, child) in runs {
        let out = tokio::task::spawn_blocking(move || child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "{scenario}: {stdout}{stderr}");
        let lines: Vec<&str> = stdout.lines().collect();
        assert_eq!(lines.len(), 2, "{scenario}: {stdout}{stderr}");
        for (line, rate) in lines.iter().zip(["200", "400"]) {
            assert!(
                line.starts_with(&format!("fixed {scenario} v5 qos=1 size=16")),
                "{line}"
            );
            assert!(line.contains(&format!("offered {rate}/s")), "{line}");
            assert!(line.contains("  cpu ") && line.contains(" rss "), "{line}");
            assert!(line.ends_with("  ok"), "{line}");
        }
        assert!(stderr.contains("connected "), "{stderr}");
        assert_eq!(stderr.contains("sends late"), scenario == "p2p", "{stderr}");
    }
    let out = Command::new(exe).arg("nope").output().unwrap();
    assert!(!out.status.success());
}

#[test]
fn a_fixed_load_run_without_a_broker_fails_its_first_step() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let cfg = Fixed {
        port,
        addrs: 1,
        pubs: 3,
        rates: vec![100.0],
        ..Fixed::default()
    };
    let (mut connect, mut steps) = (None, Vec::new());
    fixed::run(fixed::Scenario::P2p, &cfg, |r| match r {
        Report::Connect(c) => connect = Some(c.clone()),
        Report::Step(s) => steps.push(s.clone()),
    })
    .unwrap();
    let connect = connect.unwrap();
    assert!(!connect.ok() && connect.connected == 0, "{connect:?}");
    assert_eq!(connect.clients, 6);
    assert!(
        connect.retries > 0 && connect.error.is_some(),
        "{connect:?}"
    );
    assert_eq!(steps.len(), 1);
    assert!(steps[0].failed && steps[0].closed == 6, "{:?}", steps[0]);
    assert!(steps[0].to_string().ends_with("samples 0  failed"));
}

#[test]
fn a_fixed_load_run_rejects_settings_it_cannot_measure() {
    let short = Fixed {
        warmup: Duration::from_secs(1),
        ..Fixed::default()
    };
    let no_subs = Fixed {
        subs: 0,
        ..Fixed::default()
    };
    for (scenario, cfg) in [
        (fixed::Scenario::P2p, short),
        (fixed::Scenario::Fanin, no_subs),
    ] {
        let e = fixed::run(scenario, &cfg, |_| panic!("ran")).unwrap_err();
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput, "{e}");
    }
}

#[test]
fn the_ram_probe_measures_every_step() {
    let exe = env!("CARGO_BIN_EXE_zzq_ramprobe");
    let out = Command::new(exe)
        .args([
            "sessions=20",
            "per=2",
            "fanout=1",
            "retained=30",
            "sizes=100",
        ])
        .arg("sys=0.05")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{stdout}");
    for (step, items) in [
        ("offline session", 20),
        ("queued 100 B", 40),
        ("fanned-out entry 100 B", 20),
        ("retained 100 B", 30),
    ] {
        let line = stdout.lines().find(|l| l.starts_with(step));
        let shown = line.and_then(|l| l.split(" x ").next()?.split_whitespace().last());
        assert_eq!(shown, Some(items.to_string().as_str()), "{stdout}");
    }
    assert_eq!(stdout.matches("estimate/RSS").count(), 4, "{stdout}");
    for args in [&["nope"][..], &["sessions=1", "--", "true"]] {
        let out = Command::new(exe).args(args).output().unwrap();
        assert!(!out.status.success(), "{args:?}");
    }
}

#[test]
fn the_perf_gate_runs_one_case() {
    // The gate pins the broker and the load to separate CPUs.
    if std::thread::available_parallelism().map_or(1, usize::from) < 2 {
        return;
    }
    let dir = std::env::temp_dir().join(format!("zzq-gate-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(".perf_hw"), "[broker_1io]\nqos0_16b_msgs_s=1\n").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_zzq_perf_verify"))
        .current_dir(&dir)
        .env("ZZQ_PERF_MEASURE_MS", "200")
        .env("ZZQ_PERF_WARMUP_MS", "0")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("perf gate: PASS"),
        "{stdout}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_chart_tool_draws_every_chart_from_the_cache() {
    let dir = std::env::temp_dir().join(format!("zzq-chart-{}", std::process::id()));
    let cache = dir.join("cache/zzq");
    std::fs::create_dir_all(&cache).unwrap();
    let brokers = [
        r#"{"run_id":"2026-10-24T00:00:00Z","impl":"zzq-1io","config":"default","kind":"throughput","scenario":"pairs","pubs":1,"subs":1,"qos":0,"msg_size":16,"msgs_s":1e6,"cpu_pct":99}"#,
        r#"{"run_id":"2026-10-24T00:00:00Z","impl":"rmqtt","version":"0.24.0","config":"tuned","kind":"rtt","qos":1,"msg_size":256,"p50_us":40,"p99_us":50,"p999_us":900}"#,
    ];
    std::fs::write(cache.join("brokers.jsonl"), brokers.join("\n")).unwrap();
    let client = r#"{"run_id":"2026-10-24T00:00:00Z","impl":"rumqttc","version":"0.25.1","kind":"sub","qos":2,"msg_size":1024,"msgs_s":5e5}"#;
    std::fs::write(cache.join("clients.jsonl"), client).unwrap();
    let fixed = [
        r#"{"run_id":"2026-10-24T00:00:00Z","impl":"xmq","version":"1.0.0","config":"tuned","scenario":"p2p","offered_s":10000,"msgs_s":10000,"p50_us":100,"p99_us":900,"p999_us":2000,"cpu_pct":80,"rss_mib":100,"failed":false}"#,
        r#"{"run_id":"2026-10-24T00:00:00Z","impl":"xmq","version":"1.0.0","config":"tuned","scenario":"p2p","offered_s":20000,"msgs_s":14000,"p50_us":4e5,"p99_us":9e5,"p999_us":1e6,"cpu_pct":390,"rss_mib":120,"failed":true}"#,
    ];
    std::fs::write(cache.join("fixed.jsonl"), fixed.join("\n")).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_zzq_chart"))
        .current_dir(&dir)
        .env("XDG_CACHE_HOME", dir.join("cache"))
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success() && stderr.contains("missing:"),
        "{stderr}"
    );
    for (file, texts) in [
        (
            "brokers.svg",
            ["MQTT brokers", "rmqtt v0.24.0", "p99.9: 900", "CPU %"],
        ),
        (
            "clients.svg",
            ["MQTT clients", "rumqttc v0.25.1", "subscribe", "publish"],
        ),
        (
            "fixed-load.svg",
            [
                "fixed load",
                "XMQ v1.0.0",
                "XMQ, 20K/s: 70% delivered",
                "point to point",
            ],
        ),
    ] {
        let svg = std::fs::read_to_string(dir.join("doc/charts").join(file)).unwrap();
        for text in texts {
            assert!(svg.contains(text), "{file} lacks {text}");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A port nothing listens on right now.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn the_fake_broker_measures_a_client_at_every_qos() {
    let fake = env!("CARGO_BIN_EXE_zzq_fakebroker");
    let client = env!("CARGO_BIN_EXE_zzq_client_bench");
    for qos in ["0", "1", "2"] {
        for (role, mode) in [("sink", "pub"), ("source", "sub")] {
            let port = free_port().to_string();
            let args = [&port, qos, "16", "2000"];
            let server = Command::new(fake).arg(role).args(args).spawn().unwrap();
            let mut tries = 0;
            let out = loop {
                let out = Command::new(client).arg(mode).args(args).output().unwrap();
                tries += 1;
                if out.status.success() || tries == 50 {
                    break out;
                }
                std::thread::sleep(Duration::from_millis(50));
            };
            let measured = server.wait_with_output().unwrap();
            assert!(
                out.status.success() && measured.status.success(),
                "{qos} {mode}"
            );
            if mode == "sub" {
                let stdout = String::from_utf8_lossy(&out.stdout);
                assert!(stdout.contains(&format!("zzq-sub qos={qos}")), "{stdout}");
            }
        }
    }
    for exe in [fake, client] {
        assert!(!Command::new(exe).arg("nope").status().unwrap().success());
    }
}

#[test]
fn the_suite_smoke_runs_a_suite_and_records_nothing() {
    let suite = env!("CARGO_BIN_EXE_zzq_suite");
    let cache = std::env::temp_dir().join(format!("zzq-suite-smoke-{}", std::process::id()));
    let run = |args: &[&str]| {
        Command::new(suite)
            .args(args)
            .env("XDG_CACHE_HOME", &cache)
            .output()
            .unwrap()
    };
    let out = run(&["--smoke", "zzqclient"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{stdout}");
    assert!(
        stdout.contains(r#""kind":"pub""#) && stdout.contains(r#""kind":"sub""#),
        "{stdout}"
    );
    assert!(!cache.join("zzq/clients.jsonl").exists());

    let out = run(&["--smoke", "all", "zzqclient"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let logs: Vec<_> = std::fs::read_dir(cache.join("zzq/logs"))
        .unwrap()
        .flatten()
        .collect();
    let log = std::fs::read_to_string(logs[0].path().join("zzqclient.log")).unwrap();
    assert!(log.contains("row {"), "{log}");

    assert!(!run(&["nope"]).status.success());
    assert!(!run(&["all", "nope"]).status.success());
    assert!(!run(&["fixed", "nope"]).status.success());
    std::fs::remove_dir_all(&cache).unwrap();
}

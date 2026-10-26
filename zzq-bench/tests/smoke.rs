//! Short runs of the load generator and the perf gate against a real
//! broker. They check that the tools work, not how fast the broker is.

#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use std::process::Command;
use std::time::Duration;

use zzq_bench::load::{self, Load, Scenario};
use zzq_broker::{Broker, BrokerOptions, Context};

async fn broker() -> (Context, Broker, String) {
    let ctx = Context::new().unwrap();
    let broker = Broker::new(&ctx, BrokerOptions::default()).await.unwrap();
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
async fn the_load_generator_runs_from_the_command_line() {
    let (_ctx, _broker, addr) = broker().await;
    let exe = env!("CARGO_BIN_EXE_zzq_brokerbench");
    let out = Command::new(exe)
        .args(["rtt", &format!("addr={addr}"), "iters=50"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("rtt v5"),
        "{stdout}"
    );
    let out = Command::new(exe)
        .args(["pairs", &format!("addr={addr}"), "warmup=0", "secs=1"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success() && stdout.contains("pairs"), "{stdout}");
    let out = Command::new(exe).arg("nope").output().unwrap();
    assert!(!out.status.success());
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

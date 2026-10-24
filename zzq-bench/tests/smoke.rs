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

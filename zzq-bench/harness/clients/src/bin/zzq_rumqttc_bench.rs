//! rumqttc 3.1.1 client throughput against `zzq_fakebroker`.
//!
//! Usage: `zzq_rumqttc_bench <pub|sub> <port> <qos> <size> <n>`
//!
//! The client runs on a current-thread Tokio runtime with rumqttc's default
//! in-flight window of 100. The fake broker measures publishing; this
//! client measures subscribing.

use std::process::ExitCode;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};

const TOPIC: &str = "bench/t";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((publish, port, qos, size, n)) = parse(&args) else {
        eprintln!("usage: zzq_rumqttc_bench <pub|sub> <port> <qos> <size> <n>");
        return ExitCode::FAILURE;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let result = rt.map_err(|e| e.to_string()).and_then(|rt| {
        rt.block_on(async {
            let mut opts = MqttOptions::new(if publish { "pub" } else { "sub" }, "127.0.0.1", port);
            opts.set_keep_alive(Duration::from_secs(60));
            opts.set_max_packet_size(64 * 1024, 64 * 1024);
            let (client, el) = AsyncClient::new(opts, 1000);
            if publish {
                publish_all(&client, el, qos, size, n).await
            } else {
                subscribe(&client, el, qos, size, n).await
            }
        })
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn parse(args: &[String]) -> Option<(bool, u16, QoS, usize, usize)> {
    let [mode, port, qos, size, n] = args else {
        return None;
    };
    let publish = match mode.as_str() {
        "pub" => true,
        "sub" => false,
        _ => return None,
    };
    let qos = match qos.as_str() {
        "0" => QoS::AtMostOnce,
        "1" => QoS::AtLeastOnce,
        "2" => QoS::ExactlyOnce,
        _ => return None,
    };
    let n = n.parse().ok().filter(|&n: &usize| n > 1)?;
    Some((publish, port.parse().ok()?, qos, size.parse().ok()?, n))
}

/// Publishes `n` messages. The fake broker closes the connection once it
/// has them all, which ends the event loop.
async fn publish_all(
    client: &AsyncClient,
    mut el: EventLoop,
    qos: QoS,
    size: usize,
    n: usize,
) -> Result<(), String> {
    loop {
        if let Event::Incoming(Packet::ConnAck(_)) = el.poll().await.map_err(|e| e.to_string())? {
            break;
        }
    }
    let driver = tokio::spawn(async move { while el.poll().await.is_ok() {} });
    let payload = Bytes::from(vec![b'x'; size]);
    for _ in 0..n {
        client
            .publish_bytes(TOPIC, qos, false, payload.clone())
            .await
            .map_err(|e| e.to_string())?;
    }
    driver.await.map_err(|e| e.to_string())
}

/// Takes `n` messages and prints the rate from the first to the last.
async fn subscribe(
    client: &AsyncClient,
    mut el: EventLoop,
    qos: QoS,
    size: usize,
    n: usize,
) -> Result<(), String> {
    client
        .subscribe(TOPIC, qos)
        .await
        .map_err(|e| e.to_string())?;
    let mut count = 0;
    let mut t0 = Instant::now();
    while count < n {
        if let Event::Incoming(Packet::Publish(_)) = el.poll().await.map_err(|e| e.to_string())? {
            count += 1;
            if count == 1 {
                t0 = Instant::now();
            }
        }
    }
    let rate = (n - 1) as f64 / t0.elapsed().as_secs_f64();
    println!(
        "rumqttc-sub qos={} size={size:>5} n={n:>8}  {rate:>10.0} msg/s  {:>8.1} MB/s",
        qos as u8,
        rate * size as f64 / 1e6
    );
    Ok(())
}

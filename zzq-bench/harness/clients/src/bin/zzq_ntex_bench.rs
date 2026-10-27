//! ntex-mqtt v5 client throughput against `zzq_fakebroker`.
//!
//! Usage: `zzq_ntex_bench <pub|sub> <port> <qos> <size> <n> [window]`
//!
//! QoS 1/2 publishing runs `window` concurrent tasks (default 100), so up to
//! `window` messages are in flight. The fake broker measures publishing;
//! this client measures subscribing.

use std::cell::Cell;
use std::process::ExitCode;
use std::rc::Rc;
use std::time::Instant;

use ntex::time::Seconds;
use ntex::util::Bytes;
use ntex::{Pipeline, SharedCfg};
use ntex_mqtt::v5;

const TOPIC: &str = "bench/t";

/// What to run.
#[derive(Debug, Clone, Copy)]
struct Args {
    publish: bool,
    port: u16,
    qos: u8,
    size: usize,
    n: usize,
    window: usize,
}

#[ntex::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(args) = parse(&args) else {
        eprintln!("usage: zzq_ntex_bench <pub|sub> <port> <qos> <size> <n> [window]");
        return ExitCode::FAILURE;
    };
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn parse(args: &[String]) -> Option<Args> {
    let [mode, port, qos, size, n, rest @ ..] = args else {
        return None;
    };
    let window = match rest {
        [] => 100,
        [w] => w.parse().ok()?,
        _ => return None,
    };
    Some(Args {
        publish: match mode.as_str() {
            "pub" => true,
            "sub" => false,
            _ => return None,
        },
        port: port.parse().ok()?,
        qos: qos.parse().ok().filter(|&q| q <= 2)?,
        size: size.parse().ok()?,
        n: n.parse().ok().filter(|&n| n > 1)?,
        window,
    })
}

async fn run(args: Args) -> Result<(), String> {
    let client = Pipeline::new(SharedCfg::default(), v5::client::MqttConnector::new())
        .call(
            v5::client::Connect::new(format!("127.0.0.1:{}", args.port))
                .client_id("ntex")
                .keep_alive(Seconds(60)),
        )
        .await
        .map_err(|e| format!("{e:?}"))?;
    if args.publish {
        publish(client, args).await
    } else {
        subscribe(client, args).await
    }
}

/// Publishes `n` messages. The fake broker closes the connection after the
/// last one, which ends the client.
async fn publish(client: v5::client::Client, args: Args) -> Result<(), String> {
    let sink = client.sink();
    let driver = ntex::rt::spawn(client.start_default());
    let payload = Bytes::from(vec![b'x'; args.size]);
    if args.qos == 0 {
        for _ in 0..args.n {
            sink.publish(TOPIC)
                .send_at_most_once(payload.clone())
                .await
                .map_err(|e| format!("{e:?}"))?;
        }
    } else {
        let left = Rc::new(Cell::new(args.n));
        let mut tasks = Vec::new();
        for _ in 0..args.window {
            let (sink, left, payload) = (sink.clone(), left.clone(), payload.clone());
            tasks.push(ntex::rt::spawn(async move {
                while left.get() > 0 {
                    left.set(left.get() - 1);
                    let p = sink.publish(TOPIC);
                    if args.qos == 1 {
                        p.send_at_least_once(payload.clone())
                            .await
                            .map_err(|e| format!("{e:?}"))?;
                    } else {
                        let rec = p
                            .send_exactly_once(payload.clone())
                            .await
                            .map_err(|e| format!("{e:?}"))?;
                        rec.release().await.map_err(|e| format!("{e:?}"))?;
                    }
                }
                Ok::<_, String>(())
            }));
        }
        for t in tasks {
            t.await.map_err(|e| format!("{e:?}"))??;
        }
    }
    let _ = driver.await;
    Ok(())
}

/// Takes `n` messages and prints the rate from the first to the last.
async fn subscribe(client: v5::client::Client, args: Args) -> Result<(), String> {
    let sink = client.sink();
    let count = Rc::new(Cell::new(0usize));
    let t0 = Rc::new(Cell::new(Instant::now()));
    let (tx, rx) = ntex::channel::oneshot::channel();
    let tx = Rc::new(Cell::new(Some(tx)));
    {
        let (count, t0, tx, n) = (count.clone(), t0.clone(), tx.clone(), args.n);
        ntex::rt::spawn(
            client.start(async move |msg: v5::client::ProtocolMessage| match msg {
                v5::client::ProtocolMessage::Publish(publish) => {
                    let _ = publish.read_all().await;
                    let c = count.get() + 1;
                    count.set(c);
                    if c == 1 {
                        t0.set(Instant::now());
                    }
                    if c == n
                        && let Some(tx) = tx.take()
                    {
                        let _ = tx.send(t0.get().elapsed());
                    }
                    Ok::<_, ()>(publish.ack(v5::codec::PublishAckReason::Success))
                }
                other => Ok(other.ack()),
            }),
        );
    }
    let qos = match args.qos {
        0 => v5::codec::QoS::AtMostOnce,
        1 => v5::codec::QoS::AtLeastOnce,
        _ => v5::codec::QoS::ExactlyOnce,
    };
    sink.subscribe(None)
        .topic_filter(
            TOPIC.into(),
            v5::codec::SubscriptionOptions {
                qos,
                no_local: false,
                retain_as_published: false,
                retain_handling: v5::codec::RetainHandling::AtSubscribe,
            },
        )
        .send()
        .await
        .map_err(|e| format!("{e:?}"))?;
    let elapsed = rx.await.map_err(|e| format!("{e:?}"))?;
    let rate = (args.n - 1) as f64 / elapsed.as_secs_f64();
    println!(
        "ntex-sub qos={} size={:>5} n={:>8}  {rate:>10.0} msg/s  {:>8.1} MB/s",
        args.qos,
        args.size,
        args.n,
        rate * args.size as f64 / 1e6
    );
    sink.force_close();
    Ok(())
}

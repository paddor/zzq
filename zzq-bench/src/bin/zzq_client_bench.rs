//! zzq-client v5 throughput against `zzq_fakebroker`.
//!
//! Usage: `zzq_client_bench <pub|sub> <port> <qos> <size> <n> [window]`
//!
//! The caller runs on a current-thread Tokio runtime, the connection on one
//! IO lane. QoS 1/2 publishing runs `window` concurrent tasks (default 100),
//! so up to `window` messages are in flight. The fake broker measures
//! publishing; this client measures subscribing. `RT=mt` runs the caller on
//! a two-thread runtime instead.

use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Instant;

use bytes::Bytes;
use zzq_client::{Client, ClientOptions, Context, QoS};

const TOPIC: &str = "bench/t";

/// What to run.
#[derive(Debug)]
struct Args {
    publish: bool,
    port: u16,
    qos: QoS,
    size: usize,
    n: usize,
    window: usize,
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(args) = parse(&args) else {
        eprintln!("usage: zzq_client_bench <pub|sub> <port> <qos> <size> <n> [window]");
        return ExitCode::FAILURE;
    };
    let rt = match std::env::var("RT").as_deref() {
        Ok("mt") => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build(),
        _ => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
    };
    match rt
        .map_err(|e| e.to_string())
        .and_then(|rt| rt.block_on(run(&args)))
    {
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
        qos: QoS::try_from(qos.parse::<u8>().ok()?).ok()?,
        size: size.parse().ok()?,
        n: n.parse().ok().filter(|&n| n > 1)?,
        window,
    })
}

async fn run(args: &Args) -> Result<(), String> {
    let ctx = Context::new().map_err(|e| e.to_string())?;
    let uri = format!("tcp://127.0.0.1:{}", args.port);
    let client = Client::connect(&ctx, &uri, ClientOptions::new("zzq"))
        .await
        .map_err(|e| e.to_string())?;
    if args.publish {
        publish(client, args).await
    } else {
        subscribe(client, args).await
    }
}

/// Publishes `n` messages. The fake broker closes the connection after the
/// last one, which can fail the last few acks.
async fn publish(mut client: Client, args: &Args) -> Result<(), String> {
    let payload = Bytes::from(vec![b'x'; args.size]);
    if args.qos == QoS::AtMostOnce {
        for _ in 0..args.n {
            client
                .publish(TOPIC, payload.clone(), args.qos)
                .await
                .map_err(|e| e.to_string())?;
        }
        while client.recv().await.is_some() {}
        return Ok(());
    }
    let client = Arc::new(client);
    // Tasks take message numbers until all `n` are taken.
    let taken = Arc::new(AtomicUsize::new(0));
    let n = args.n;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..args.window {
        let (client, taken, payload, qos) =
            (client.clone(), taken.clone(), payload.clone(), args.qos);
        tasks.spawn(async move {
            while taken.fetch_add(1, Relaxed) < n {
                if client.publish(TOPIC, payload.clone(), qos).await.is_err() {
                    return;
                }
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}

/// Takes `n` messages and prints the rate from the first to the last.
async fn subscribe(mut client: Client, args: &Args) -> Result<(), String> {
    client
        .subscribe(TOPIC, args.qos)
        .await
        .map_err(|e| e.to_string())?;
    let mut t0 = Instant::now();
    for i in 1..=args.n {
        client
            .recv()
            .await
            .ok_or("closed before the last message")?;
        if i == 1 {
            t0 = Instant::now();
        }
    }
    let rate = (args.n - 1) as f64 / t0.elapsed().as_secs_f64();
    println!(
        " zzq-sub qos={} size={:>5} n={:>8}  {rate:>10.0} msg/s  {:>8.1} MB/s",
        args.qos as u8,
        args.size,
        args.n,
        rate * args.size as f64 / 1e6
    );
    Ok(())
}

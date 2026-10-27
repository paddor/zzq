//! Fake broker for one external client, v3.1.1 or v5.
//!
//! Usage: `zzq_fakebroker <sink|source> <port> <qos> <size> <n> [window]`
//!
//! A sink measures from the first to the `n`-th message it takes. A source
//! sends `n` messages on `bench/t` after SUBSCRIBE and measures until the
//! last write (QoS 0) or the last ack (QoS 1/2), with `window` (default 100)
//! in flight.

use std::net::TcpListener;
use std::process::ExitCode;

use zzq_bench::fake::{self, Fake, Role};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((port, fake)) = parse(&args) else {
        eprintln!("usage: zzq_fakebroker <sink|source> <port> <qos> <size> <n> [window]");
        return ExitCode::FAILURE;
    };
    let served = TcpListener::bind(("127.0.0.1", port)).and_then(|l| fake::serve(&l, &fake));
    let elapsed = match served {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::FAILURE;
        }
    };
    let rate = fake.n.saturating_sub(1) as f64 / elapsed.as_secs_f64();
    let role = if fake.role == Role::Sink {
        "sink"
    } else {
        "source"
    };
    println!(
        "fake-{role} qos={} size={:>5} n={:>8}  {rate:>10.0} msg/s  {:>8.1} MB/s",
        fake.qos,
        fake.size,
        fake.n,
        rate * fake.size as f64 / 1e6
    );
    ExitCode::SUCCESS
}

fn parse(args: &[String]) -> Option<(u16, Fake)> {
    let [role, port, qos, size, n, rest @ ..] = args else {
        return None;
    };
    let role = match role.as_str() {
        "sink" => Role::Sink,
        "source" => Role::Source,
        _ => return None,
    };
    let window = match rest {
        [] => 100,
        [w] => w.parse().ok()?,
        _ => return None,
    };
    let fake = Fake {
        role,
        qos: qos.parse().ok().filter(|&q| q <= 2)?,
        size: size.parse().ok()?,
        n: n.parse().ok().filter(|&n| n > 0)?,
        window,
    };
    Some((port.parse().ok()?, fake))
}

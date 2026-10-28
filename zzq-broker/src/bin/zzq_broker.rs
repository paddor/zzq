//! Standalone broker.
//!
//! Usage: `zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM]
//! [--client-ca PEM] [--data-dir PATH] [--state-file PATH]
//! [--max-state-mib N] [--busy-poll] [--spin-us N]`
//!
//! Without `--bind`, it binds `tcp://0.0.0.0:1883`. `--cert` and `--key`,
//! which go together, enable `tls://`, `wss://` and `quic://` binds.
//! `--client-ca` makes them require client certificates. `--data-dir`
//! keeps retained messages, sessions and their QoS 1/2 messages on disk,
//! and acks what goes there once it is (Linux only).
//! `--state-file` keeps sessions
//! across a restart: SIGTERM or SIGINT write it, and the next start loads
//! it. `--max-state-mib` sets `max_state_bytes`. `--spin-us` sets how long
//! a lane polls on after work before it sleeps. Prints each bound endpoint
//! on its own line, then runs until SIGTERM or SIGINT. With `--data-dir`,
//! it says on stderr when journals there could not make a new segment, as
//! on a full device, and when they can again, and when the state stops
//! growing for lack of room there, and when it has room again.

use std::process::ExitCode;
use std::time::Duration;

use zzq_broker::{Broker, BrokerOptions, Config, Context};

struct Args {
    lanes: usize,
    binds: Vec<String>,
    cert: Option<String>,
    key: Option<String>,
    client_ca: Option<String>,
    data_dir: Option<String>,
    state_file: Option<String>,
    max_state_mib: Option<usize>,
    busy_poll: bool,
    spin: Duration,
}

fn parse() -> Option<Args> {
    let mut a = Args {
        lanes: 1,
        binds: Vec::new(),
        cert: None,
        key: None,
        client_ca: None,
        data_dir: None,
        state_file: None,
        max_state_mib: None,
        busy_poll: false,
        spin: Config::default().spin,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--busy-poll" {
            a.busy_poll = true;
            continue;
        }
        let value = args.next()?;
        match flag.as_str() {
            "--lanes" => a.lanes = value.parse().ok()?,
            "--bind" => a.binds.push(value),
            "--cert" => a.cert = Some(value),
            "--key" => a.key = Some(value),
            "--client-ca" => a.client_ca = Some(value),
            "--data-dir" => a.data_dir = Some(value),
            "--state-file" => a.state_file = Some(value),
            "--max-state-mib" => a.max_state_mib = Some(value.parse().ok()?),
            "--spin-us" => a.spin = Duration::from_micros(value.parse().ok()?),
            _ => return None,
        }
    }
    if a.binds.is_empty() {
        a.binds.push("tcp://0.0.0.0:1883".to_owned());
    }
    Some(a)
}

fn options(a: &Args) -> Result<BrokerOptions, String> {
    let mut opts = BrokerOptions {
        data_dir: a.data_dir.as_ref().map(Into::into),
        state_file: a.state_file.as_ref().map(Into::into),
        ..BrokerOptions::default()
    };
    if let Some(mib) = a.max_state_mib {
        opts.max_state_bytes = mib.saturating_mul(1 << 20);
    }
    tls(a, &mut opts)?;
    Ok(opts)
}

#[cfg(feature = "tls")]
fn tls(a: &Args, opts: &mut BrokerOptions) -> Result<(), String> {
    let read = |p: &str| std::fs::read(p).map_err(|e| format!("{p}: {e}"));
    if a.cert.is_some() != a.key.is_some() || (a.client_ca.is_some() && a.cert.is_none()) {
        return Err("--cert and --key go together, and --client-ca needs them".to_owned());
    }
    if let (Some(cert), Some(key)) = (&a.cert, &a.key) {
        opts.transport.tls = Some(zzq_broker::TlsOptions {
            cert_chain_pem: Some(read(cert)?),
            key_pem: Some(read(key)?),
            client_ca_pem: a.client_ca.as_deref().map(read).transpose()?,
            ..Default::default()
        });
    }
    Ok(())
}

#[cfg(not(feature = "tls"))]
fn tls(a: &Args, _opts: &mut BrokerOptions) -> Result<(), String> {
    if a.cert.is_some() || a.key.is_some() || a.client_ca.is_some() {
        return Err("built without TLS support".to_owned());
    }
    Ok(())
}

/// An error with its causes, outermost first.
fn chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut cause = e.source();
    while let Some(c) = cause {
        out.push_str(": ");
        out.push_str(&c.to_string());
        cause = c.source();
    }
    out
}

async fn serve(a: Args, opts: BrokerOptions) -> Result<(), String> {
    // Before anything is bound, so no signal finds the default handler.
    let stopped = stop_signal().map_err(|e| format!("signals: {e}"))?;
    let config = Config {
        io_threads: a.lanes,
        name: "zzq".to_owned(),
        busy_poll: a.busy_poll,
        spin: a.spin,
    };
    let ctx = Context::with_config(&config).map_err(|e| format!("context: {}", chain(&e)))?;
    let max_state = opts.max_state_bytes;
    let broker = Broker::new(&ctx, opts)
        .await
        .map_err(|e| format!("broker: {}", chain(&e)))?;
    for uri in &a.binds {
        let ep = broker
            .bind(uri)
            .await
            .map_err(|e| format!("bind {uri}: {}", chain(&e)))?;
        println!("{ep}");
    }
    tokio::select! {
        () = stopped => {}
        () = watch(&broker, a.data_dir.as_deref(), max_state) => {}
    }
    broker
        .shutdown()
        .await
        .map_err(|e| format!("shutdown: {}", chain(&e)))
}

/// Says on stderr, checked each second, when journals in the data directory
/// `dir` could not make a new segment, and when they can again, and when the
/// state stops growing for lack of room there, below `max_state`, and when
/// it has room again. Ends never.
async fn watch(broker: &Broker, dir: Option<&str>, max_state: usize) {
    let Some(dir) = dir else {
        return std::future::pending().await;
    };
    let max = u64::try_from(max_state).unwrap_or(u64::MAX);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let (mut full, mut short) = (0, false);
    loop {
        tick.tick().await;
        // A broker that stopped has nothing more to say.
        let Ok(stats) = broker.stats().await else {
            return std::future::pending().await;
        };
        if let Some(line) = full_change(dir, full, stats.full_journals) {
            eprintln!("{line}");
        }
        full = stats.full_journals;
        let line;
        (short, line) = room_change(dir, short, stats.state_bytes, stats.state_limit, max);
        if let Some(line) = line {
            eprintln!("{line}");
        }
    }
}

/// What to say when the journals in `dir` that could not make a new
/// segment went from `was` to `now`.
fn full_change(dir: &str, was: u64, now: u64) -> Option<String> {
    match (was, now) {
        (0, 1..) => Some(format!(
            "data directory {dir} is full: {now} journals could not make a new segment"
        )),
        (1.., 0) => Some(format!("data directory {dir} has room again")),
        _ => None,
    }
}

/// Whether the state in `dir` is short of room, given whether it `was`, and
/// what to say if that changed: short once the `state` bytes reach a
/// `limit` the device set below `max`, and no longer once they are back
/// under half of it, or the device no longer sets it.
fn room_change(dir: &str, was: bool, state: u64, limit: u64, max: u64) -> (bool, Option<String>) {
    let by_device = limit < max;
    let short = by_device
        && if was {
            state > limit / 2
        } else {
            state >= limit
        };
    let line = match (was, short) {
        (false, true) => Some(format!(
            "data directory {dir} is short of room: the state stops growing at {} MiB",
            limit >> 20
        )),
        (true, false) => Some(format!("data directory {dir} has room for the state again")),
        _ => None,
    };
    (short, line)
}

/// Handles SIGTERM and SIGINT from now on, and returns the wait for either.
#[cfg(unix)]
fn stop_signal() -> std::io::Result<impl Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = term.recv() => {}
            _ = int.recv() => {}
        }
    })
}

#[cfg(not(unix))]
fn stop_signal() -> std::io::Result<impl Future<Output = ()>> {
    Ok(async {
        let _ = tokio::signal::ctrl_c().await;
    })
}

fn main() -> ExitCode {
    let Some(args) = parse() else {
        eprintln!(
            "usage: zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM] \
             [--client-ca PEM] [--data-dir PATH] [--state-file PATH] \
             [--max-state-mib N] [--busy-poll] [--spin-us N]"
        );
        return ExitCode::FAILURE;
    };
    let result = options(&args).and_then(|opts| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("runtime: {e}"))?;
        rt.block_on(serve(args, opts))
    });
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{full_change, room_change};

    #[test]
    fn a_full_data_dir_is_said_once_when_it_fills_and_once_when_it_empties() {
        let said = |was, now| full_change("d", was, now);
        assert_eq!(
            said(0, 2).as_deref(),
            Some("data directory d is full: 2 journals could not make a new segment")
        );
        assert_eq!(said(2, 3), None);
        assert_eq!(
            said(3, 0).as_deref(),
            Some("data directory d has room again")
        );
        assert_eq!(said(0, 0), None);
    }

    #[test]
    fn a_data_dir_short_of_room_is_said_once_until_the_state_is_half_of_it() {
        let mib = 1 << 20;
        let said = |was, state, limit| room_change("d", was, state, limit, u64::MAX);
        assert_eq!(
            said(false, 20 * mib, 15 * mib),
            (
                true,
                Some("data directory d is short of room: the state stops growing at 15 MiB".into())
            )
        );
        assert_eq!(said(true, 10 * mib, 15 * mib), (true, None));
        assert_eq!(
            said(true, 7 * mib, 15 * mib),
            (
                false,
                Some("data directory d has room for the state again".into())
            )
        );
        assert_eq!(said(false, 10 * mib, 15 * mib), (false, None));
        // A limit `max_state_bytes` sets is no lack of room.
        assert_eq!(
            room_change("d", false, 20 * mib, 15 * mib, 15 * mib),
            (false, None)
        );
    }
}

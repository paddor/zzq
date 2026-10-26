//! Standalone broker.
//!
//! Usage: `zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM]
//! [--client-ca PEM] [--retained-file PATH] [--state-file PATH]
//! [--max-state-mib N] [--busy-poll] [--spin-us N]`
//!
//! `--cert` and `--key` enable `tls://`, `wss://` and `quic://` binds.
//! `--client-ca` makes them require client certificates. `--retained-file`
//! keeps retained messages across restarts. `--state-file` keeps sessions
//! across a restart: SIGTERM or SIGINT write it, and the next start loads
//! it. `--max-state-mib` sets `max_state_bytes`. `--spin-us` sets how long
//! a lane polls on after work before it sleeps. Prints each bound endpoint
//! on its own line, then runs until SIGTERM or SIGINT.

use std::process::ExitCode;
use std::time::Duration;

use zzq_broker::{Broker, BrokerOptions, Config, Context};

struct Args {
    lanes: usize,
    binds: Vec<String>,
    cert: Option<String>,
    key: Option<String>,
    client_ca: Option<String>,
    retained_file: Option<String>,
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
        retained_file: None,
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
            "--retained-file" => a.retained_file = Some(value),
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
    let mut opts = BrokerOptions::default();
    if let Some(path) = &a.retained_file {
        let store = zzq_broker::FilePersistence::open(path).map_err(|e| format!("{path}: {e}"))?;
        opts.persistence = Some(std::sync::Arc::new(store));
    }
    opts.state_file = a.state_file.as_ref().map(Into::into);
    if let Some(mib) = a.max_state_mib {
        opts.max_state_bytes = mib.saturating_mul(1 << 20);
    }
    tls(a, &mut opts)?;
    Ok(opts)
}

#[cfg(feature = "tls")]
fn tls(a: &Args, opts: &mut BrokerOptions) -> Result<(), String> {
    let read = |p: &str| std::fs::read(p).map_err(|e| format!("{p}: {e}"));
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
    let config = Config {
        io_threads: a.lanes,
        name: "zzq".to_owned(),
        busy_poll: a.busy_poll,
        spin: a.spin,
    };
    let ctx = Context::with_config(&config).map_err(|e| format!("context: {}", chain(&e)))?;
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
    stopped().await.map_err(|e| format!("signals: {e}"))?;
    broker
        .shutdown()
        .await
        .map_err(|e| format!("shutdown: {}", chain(&e)))
}

/// Waits for SIGTERM or SIGINT.
#[cfg(unix)]
async fn stopped() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate())?;
    let mut int = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    Ok(())
}

#[cfg(not(unix))]
async fn stopped() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

fn main() -> ExitCode {
    let Some(args) = parse() else {
        eprintln!(
            "usage: zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM] \
             [--client-ca PEM] [--retained-file PATH] [--state-file PATH] \
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

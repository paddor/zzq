//! Standalone broker.
//!
//! Usage: `zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM]
//! [--client-ca PEM] [--retained-file PATH] [--busy-poll] [--spin-us N]`
//!
//! `--cert` and `--key` enable `tls://`, `wss://` and `quic://` binds.
//! `--client-ca` makes them require client certificates. `--retained-file`
//! keeps retained messages across restarts. `--spin-us` sets how long a lane
//! polls on after work before it sleeps. Prints each bound endpoint on
//! its own line, then runs until killed.

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
    std::future::pending::<()>().await;
    Ok(())
}

fn main() -> ExitCode {
    let Some(args) = parse() else {
        eprintln!(
            "usage: zzq_broker [--lanes N] [--bind URI]... [--cert PEM --key PEM] \
             [--client-ca PEM] [--retained-file PATH] [--busy-poll] [--spin-us N]"
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

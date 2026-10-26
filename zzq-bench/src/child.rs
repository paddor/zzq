//! Brokers in child processes, so tools measure them apart from the load.

use std::io::{self, BufRead, BufReader};
use std::process::{Child, Command, ExitCode, Stdio};

use zzq_broker::{Broker, BrokerOptions, Config, Context};

/// Runs a zzq broker on a free port of 127.0.0.1 until the process is
/// killed. Prints its endpoint first.
#[must_use]
pub fn serve(cfg: &Config, opts: BrokerOptions) -> ExitCode {
    let Ok(rt) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return ExitCode::FAILURE;
    };
    rt.block_on(async {
        let Ok(ctx) = Context::with_config(cfg) else {
            return ExitCode::FAILURE;
        };
        let Ok(broker) = Broker::new(&ctx, opts).await else {
            return ExitCode::FAILURE;
        };
        let Ok(ep) = broker.bind("tcp://127.0.0.1:0").await else {
            return ExitCode::FAILURE;
        };
        println!("{ep}");
        std::future::pending::<()>().await;
        ExitCode::SUCCESS
    })
}

/// Starts a broker that prints its endpoint first, as [`serve`] and
/// `zzq_broker` do. Returns it and its `host:port`.
///
/// # Errors
///
/// If it does not start or prints no endpoint.
pub fn spawn(mut cmd: Command) -> io::Result<(Child, String)> {
    let mut child = cmd.stdout(Stdio::piped()).spawn()?;
    let mut line = String::new();
    if let Some(out) = child.stdout.as_mut() {
        BufReader::new(out).read_line(&mut line)?;
    }
    match line.trim().strip_prefix("tcp://") {
        Some(addr) if !addr.is_empty() => Ok((child, addr.to_owned())),
        _ => {
            let _ = child.kill();
            let _ = child.wait();
            Err(io::Error::other("broker printed no endpoint"))
        }
    }
}

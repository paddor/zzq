//! Brokers in child processes, so tools measure them apart from the load.

use std::io::{self, BufRead, BufReader};
use std::net::TcpStream;
use std::process::{Child, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use zzq_broker::{BrokerOptions, Config, Context};

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
        let Ok(broker) = zzq_broker::Broker::new(&ctx, opts).await else {
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

/// A broker child process. Dropping it kills and reaps the process, also
/// on an early return or a panic.
#[derive(Debug)]
pub struct Broker {
    process: Child,
    addr: String,
}

impl Broker {
    /// Its `host:port`.
    #[must_use]
    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// Its process ID.
    #[must_use]
    pub fn id(&self) -> u32 {
        self.process.id()
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Starts a broker that prints its endpoint first, as [`serve`] and
/// `zzq_broker` do.
///
/// # Errors
///
/// If it does not start or prints no endpoint.
pub fn spawn(mut cmd: Command) -> io::Result<Broker> {
    let mut broker = Broker {
        process: cmd.stdout(Stdio::piped()).spawn()?,
        addr: String::new(),
    };
    let mut line = String::new();
    if let Some(out) = broker.process.stdout.as_mut() {
        BufReader::new(out).read_line(&mut line)?;
    }
    match line.trim().strip_prefix("tcp://") {
        Some(addr) if !addr.is_empty() => {
            addr.clone_into(&mut broker.addr);
            Ok(broker)
        }
        _ => Err(io::Error::other("broker printed no endpoint")),
    }
}

/// Starts a broker that prints nothing useful and waits until something
/// listens on `addr`, for at most `limit`.
///
/// # Errors
///
/// If it does not start or nothing listens on `addr` in time.
pub fn spawn_on(mut cmd: Command, addr: &str, limit: Duration) -> io::Result<Broker> {
    let broker = Broker {
        process: cmd.stdout(Stdio::null()).spawn()?,
        addr: addr.to_owned(),
    };
    let deadline = Instant::now() + limit;
    while TcpStream::connect(addr).is_err() {
        if Instant::now() > deadline {
            return Err(io::Error::other(format!("nothing listens on {addr}")));
        }
        thread::sleep(Duration::from_millis(50));
    }
    Ok(broker)
}

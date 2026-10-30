use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use zzq_broker::{Broker, BrokerOptions, Config, Context};
use zzq_client::{Client, ClientOptions, Message, QoS, TransportOptions, Version};

use super::resources::Monitor;

pub(super) const WAIT: Duration = Duration::from_secs(30);

pub(super) async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("operation made no progress for 30s")
}

pub(super) fn context(lanes: usize) -> Context {
    Context::with_config(&Config {
        io_threads: lanes,
        spin: Duration::ZERO,
        ..Config::default()
    })
    .unwrap()
}

pub(super) struct Run {
    pub dir: PathBuf,
    pub started: Instant,
    pub duration: Duration,
    pub monitor: Monitor,
    pub seed: u64,
}

impl Run {
    pub(super) fn new(name: &str) -> Self {
        let root = std::env::var_os("ZZQ_SOAK_DIR")
            .map_or_else(|| std::env::temp_dir().join("zzq-soak"), PathBuf::from);
        let dir = root.join(format!("{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let duration = std::env::var("ZZQ_SOAK_SECS").map_or(600, |s| s.parse::<u64>().unwrap());
        assert!(duration > 0);
        let seed = std::env::var("ZZQ_SOAK_SEED").map_or(0x5eed, |s| s.parse().unwrap());
        eprintln!(
            "soak={name} duration={duration}s seed={seed} artifacts={}",
            dir.display()
        );
        Self {
            monitor: Monitor::new(&dir),
            dir,
            started: Instant::now(),
            duration: Duration::from_secs(duration),
            seed,
        }
    }

    pub(super) fn running(&self) -> bool {
        self.started.elapsed() < self.duration
    }

    pub(super) fn progress(&self, messages: u64) {
        self.monitor.progress(messages);
    }

    pub(super) fn finish(self, cycles: u64) {
        assert!(
            cycles >= 2,
            "must complete at least two full cycles, got {cycles}"
        );
        eprintln!(
            "completed cycles={cycles} elapsed={:.1}s",
            self.started.elapsed().as_secs_f64()
        );
        self.monitor.finish();
    }
}

pub(super) struct Server {
    pub ctx: Context,
    pub broker: Broker,
    pub uri: String,
    pub transport: TransportOptions,
}

impl Server {
    pub(super) async fn new(run: &Run, scheme: &str, disk: bool, mut opts: BrokerOptions) -> Self {
        let ctx = context(2);
        if disk {
            opts.data_dir = Some(run.dir.join("data"));
        }
        opts.max_state_bytes = 128 << 20;
        opts.max_queued = 8192;
        opts.max_queued_bytes = 32 << 20;
        opts.sys_interval = None;
        let transport = if matches!(scheme, "tls" | "wss" | "quic") {
            let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
            let cert = key.cert.pem().into_bytes();
            opts.transport.tls = Some(zzq_broker::TlsOptions {
                cert_chain_pem: Some(cert.clone()),
                key_pem: Some(key.signing_key.serialize_pem().into_bytes()),
                ..Default::default()
            });
            TransportOptions {
                tls: Some(zzq_client::TlsOptions {
                    trust_pem: Some(cert),
                    server_name: Some("localhost".to_owned()),
                    ..Default::default()
                }),
            }
        } else {
            TransportOptions::default()
        };
        let broker = within(Box::pin(Broker::new(&ctx, opts))).await.unwrap();
        let bind = match scheme {
            "inproc" => "inproc://soak".to_owned(),
            "ipc" => format!("ipc://{}", run.dir.join("broker.sock").display()),
            _ => format!("{scheme}://127.0.0.1:0"),
        };
        let uri = within(broker.bind(&bind)).await.unwrap().to_string();
        Self {
            ctx,
            broker,
            uri,
            transport,
        }
    }

    pub(super) async fn client(&self, id: &str, version: Version, persistent: bool) -> Client {
        let opts = ClientOptions {
            version,
            clean_start: !persistent,
            session_expiry: if persistent { 7200 } else { 0 },
            transport: self.transport.clone(),
            inbox: 128,
            keep_alive: 10,
            ..ClientOptions::new(id)
        };
        within(Client::connect(&self.ctx, &self.uri, opts))
            .await
            .unwrap()
    }

    pub(super) async fn close(self) {
        within(self.broker.shutdown()).await.unwrap();
    }
}

pub(super) async fn next(c: &mut Client) -> Message {
    within(c.recv()).await.expect("subscriber closed")
}

pub(super) const fn qos(n: u64) -> QoS {
    match n % 3 {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

pub(super) fn payload(sequence: u64, size: usize, seed: u64) -> Vec<u8> {
    let mut out = vec![(sequence ^ seed) as u8; size.max(8)];
    out[..8].copy_from_slice(&sequence.to_le_bytes());
    out
}

pub(super) fn sequence(data: &[u8]) -> u64 {
    assert!(data.len() >= 8, "payload shorter than its sequence number");
    u64::from_le_bytes(data[..8].try_into().unwrap())
}

pub(super) fn same(actual: &[u8], expected: &[u8]) {
    assert!(
        actual == expected,
        "payload mismatch: actual seq={} bytes={}, expected seq={} bytes={}",
        sequence(actual),
        actual.len(),
        sequence(expected),
        expected.len()
    );
}

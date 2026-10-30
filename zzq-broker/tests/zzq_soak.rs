//! Real-clock soaks in the `zzq_soak` executable. Ordinary test runs skip them.
#![cfg(target_os = "linux")]
#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions fail loudly"
)]

#[path = "soak/lifecycle.rs"]
mod lifecycle;
#[path = "soak/process.rs"]
mod process;
#[path = "soak/proxy.rs"]
mod proxy;
#[path = "soak/recovery.rs"]
mod recovery;
#[path = "soak/resources.rs"]
mod resources;
#[path = "soak/sn.rs"]
mod sn;
#[path = "soak/support.rs"]
mod support;
#[path = "soak/traffic.rs"]
mod traffic;

macro_rules! transport {
    ($name:ident, $scheme:literal, $disk:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        #[ignore = "long-running soak"]
        async fn $name() {
            traffic::run(stringify!($name), $scheme, $disk).await;
        }
    };
}
transport!(tcp_ram, "tcp", false);
transport!(tcp_disk, "tcp", true);
transport!(tls_ram, "tls", false);
transport!(tls_disk, "tls", true);
transport!(ws_ram, "ws", false);
transport!(ws_disk, "ws", true);
transport!(wss_ram, "wss", false);
transport!(wss_disk, "wss", true);
transport!(quic_ram, "quic", false);
transport!(quic_disk, "quic", true);
transport!(ipc_ram, "ipc", false);
transport!(ipc_disk, "ipc", true);
transport!(inproc_ram, "inproc", false);
transport!(inproc_disk, "inproc", true);

macro_rules! scenario {
    ($name:ident, $call:path, $arg:expr) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        #[ignore = "long-running soak"]
        async fn $name() {
            $call(stringify!($name), $arg).await;
        }
    };
}
scenario!(sn_ram, sn::run, false);
scenario!(sn_disk, sn::run, true);
scenario!(churn_ram, lifecycle::churn, false);
scenario!(churn_disk, lifecycle::churn, true);
scenario!(backpressure_ram, lifecycle::backpressure, false);
scenario!(backpressure_disk, lifecycle::backpressure, true);
scenario!(disk_recovery, recovery::run, recovery::Kind::Disk);
scenario!(cluster_crash, recovery::run, recovery::Kind::Crash);
scenario!(cluster_isolation, recovery::run, recovery::Kind::Isolation);
scenario!(cluster_restart, recovery::run, recovery::Kind::Restart);

//! The failover suite: QoS 1 under steady load on a cluster of three, the
//! node the publishers use stopped mid-run, a fresh cluster per cell.

use std::time::Duration;

use super::{LOAD_CPUS, Suite};
use crate::brokers::cluster;
use crate::parse::{self, Row};
use crate::record::fields;
use crate::runner::Cmd;

/// How the node stops, the messages per second offered, and whether the
/// subscribers are away while it does.
const CELLS: [(&str, u64, bool); 4] = [
    ("KILL", 1000, false),
    ("KILL", 1000, true),
    ("KILL", 10_000, false),
    ("TERM", 1000, false),
];
const PUBS: u64 = 4;
const SUBS: u64 = 4;

impl Suite<'_> {
    /// Runs the failover cells against clusters of broker `name` in
    /// `config`.
    pub(crate) fn failover(&self, who: &Row, name: &str, config: &str) -> Result<(), String> {
        let cells = if self.smoke { &CELLS[..1] } else { &CELLS[..] };
        for &(signal, rate, offline) in cells {
            let cell = fields(&[
                ("kind", "failover".into()),
                ("signal", signal.into()),
                ("rate", rate.into()),
                ("offline", offline.into()),
                ("qos", 1.into()),
                ("pubs", PUBS.into()),
                ("subs", SUBS.into()),
            ]);
            if self.skip("failover", who, &cell) {
                continue;
            }
            let running = cluster::start(name, config, self.paths)?;
            let (secs, at) = if self.smoke { (6, 3) } else { (20, 8) };
            let cmd = Cmd::pinned(LOAD_CPUS, self.paths.tool("zzq_failover"))
                .arg(format!("uris={}", cluster::Cluster::uris()))
                .arg(format!("stop={}", running.stops(signal)))
                .arg(format!("rate={rate}"))
                .arg(format!("pubs={PUBS}"))
                .arg(format!("subs={SUBS}"))
                .arg(format!("secs={secs}"))
                .arg(format!("at={at}"))
                .arg(format!("offline={}", u8::from(offline)))
                .args(cluster::client_args(name));
            let ran = self.bench(std::slice::from_ref(&cmd), 0, Duration::from_secs(120))?;
            drop(running);
            let mut row = ran
                .out
                .lines()
                .rev()
                .find_map(parse::failover_row)
                .ok_or_else(|| format!("no result in: {cmd}"))?;
            row.insert("signal".into(), signal.into());
            self.cache.record("failover", who, row)?;
        }
        Ok(())
    }
}

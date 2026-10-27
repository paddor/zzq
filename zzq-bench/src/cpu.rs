//! Broker and load generator CPU from `/proc`.

use std::fmt;
use std::time::Instant;

/// Clock ticks per second in `/proc` times (`USER_HZ`, 100 on Linux).
const USER_HZ: f64 = 100.0;

/// CPU ticks and resident bytes of a process and all its descendants, so a
/// broker in a container or with helper processes counts in full.
fn tree_usage(root: u32) -> (u64, u64) {
    let mut procs = Vec::new();
    for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Ok(pid) = e.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        let Some((_, rest)) = stat.rsplit_once(") ") else {
            continue;
        };
        // Fields from 3 (state): ppid is 4, utime 14, stime 15, rss 24.
        let f: Vec<&str> = rest.split(' ').collect();
        let num = |i: usize| {
            f.get(i - 3)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        };
        procs.push((pid, num(4) as u32, num(14) + num(15), num(24) * 4096));
    }
    let mut keep = vec![root];
    let (mut ticks, mut rss) = (0, 0);
    let mut i = 0;
    while let Some(&p) = keep.get(i) {
        for &(pid, ppid, t, r) in &procs {
            if pid == p {
                ticks += t;
                rss += r;
            } else if ppid == p {
                keep.push(pid);
            }
        }
        i += 1;
    }
    (ticks, rss)
}

fn self_ticks() -> u64 {
    tree_usage(std::process::id()).0
}

/// Broker and load generator CPU over a measured window.
#[derive(Debug, Clone, Copy)]
pub struct CpuWindow {
    broker: Option<u32>,
    start: Instant,
    broker_ticks: u64,
    own_ticks: u64,
}

/// What a [`CpuWindow`] measured.
#[derive(Debug, Clone, Copy, Default)]
pub struct CpuUse {
    /// Broker CPU in percent of one core, if a broker PID was given.
    pub broker: Option<f64>,
    /// Broker resident memory at the end, in MiB.
    pub rss_mib: Option<f64>,
    /// Load generator CPU in percent of one core.
    pub own: f64,
}

impl CpuWindow {
    /// Starts a window. `broker` is the broker's PID, if known.
    pub fn start(broker: Option<u32>) -> CpuWindow {
        CpuWindow {
            broker,
            start: Instant::now(),
            broker_ticks: broker.map_or(0, |p| tree_usage(p).0),
            own_ticks: self_ticks(),
        }
    }

    /// CPU since [`CpuWindow::start`], and the broker's memory now.
    pub fn finish(&self) -> CpuUse {
        let secs = self.start.elapsed().as_secs_f64();
        let pct = |ticks: u64| ticks as f64 / USER_HZ / secs * 100.0;
        let usage = self.broker.map(tree_usage);
        CpuUse {
            broker: usage.map(|(t, _)| pct(t.saturating_sub(self.broker_ticks))),
            rss_mib: usage.map(|(_, r)| r as f64 / f64::from(1 << 20)),
            own: pct(self_ticks().saturating_sub(self.own_ticks)),
        }
    }
}

impl fmt::Display for CpuUse {
    /// `cpu 312% rss 45M gen 150%`, without the broker parts if unknown.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let (Some(cpu), Some(rss)) = (self.broker, self.rss_mib) {
            write!(f, "cpu {cpu:.0}% rss {rss:.0}M ")?;
        }
        write!(f, "gen {:.0}%", self.own)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_window_counts_its_own_process() {
        let w = CpuWindow::start(Some(std::process::id()));
        let busy = Instant::now();
        while busy.elapsed().as_millis() < 50 {
            std::hint::black_box(0);
        }
        let used = w.finish();
        assert!(used.rss_mib.is_some_and(|r| r > 0.0), "{used:?}");
        assert!(used.to_string().starts_with("cpu "), "{used}");
        let none = CpuUse::default().to_string();
        assert_eq!(none, "gen 0%");
    }
}

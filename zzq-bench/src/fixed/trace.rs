//! Where a slow window's time went: send lateness, run queue waits, steal,
//! and the slowest deliveries by send time.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use super::{SLOT, Shared, hist, steal};

/// Slots with a delivery slower than this, in ns, are reported.
const SLOW: u64 = 3_000_000;

/// Per thread of this process: name, CPU time and run queue wait (ns); per
/// CPU: steal and interrupt time (ticks).
#[derive(Debug)]
pub(crate) struct Sched {
    threads: BTreeMap<String, (String, u64, u64)>,
    cpus: Vec<(u64, u64)>,
}

impl Sched {
    pub(crate) fn now() -> Sched {
        let mut threads = BTreeMap::new();
        for e in std::fs::read_dir("/proc/self/task")
            .into_iter()
            .flatten()
            .flatten()
        {
            let p = e.path();
            let comm = std::fs::read_to_string(p.join("comm")).unwrap_or_default();
            let stat = std::fs::read_to_string(p.join("schedstat")).unwrap_or_default();
            let v: Vec<u64> = stat
                .split_whitespace()
                .filter_map(|x| x.parse().ok())
                .collect();
            if let [cpu, wait, ..] = v[..] {
                let tid = e.file_name().to_string_lossy().into_owned();
                threads.insert(tid, (comm.trim().to_owned(), cpu, wait));
            }
        }
        let stat = std::fs::read_to_string("/proc/stat").unwrap_or_default();
        let irq = steal::per_cpu(&stat, 6)
            .into_iter()
            .zip(steal::per_cpu(&stat, 7))
            .map(|(a, b)| a + b);
        let cpus = steal::per_cpu(&stat, 8).into_iter().zip(irq).collect();
        Sched { threads, cpus }
    }
}

/// Send lateness, run queue waits, steal and interrupt time from `a` to
/// `b`, and the worst runs of slots whose slowest delivery took over 3 ms,
/// by offset into the window, with how late the sends due in them were.
pub(crate) fn report(sh: &Shared, a: &Sched, b: &Sched) -> String {
    let mut out = String::new();
    let ([p50, p99, p999, max], n) =
        hist::quantiles(&sh.lat.late.counts(), [0.5, 0.99, 0.999, 1.0]);
    let _ = writeln!(
        out,
        "  sends late: p50 {p50:.0}us p99 {p99:.0}us p99.9 {p999:.0}us max {max:.0}us ({n})"
    );
    out += "  threads:";
    for (tid, (comm, cpu, wait)) in &b.threads {
        let Some((_, cpu0, wait0)) = a.threads.get(tid) else {
            continue;
        };
        let (cpu, wait) = (cpu.saturating_sub(*cpu0), wait.saturating_sub(*wait0));
        if cpu > 1_000_000 {
            let (cpu, wait) = (cpu / 1_000_000, wait / 1_000_000);
            let _ = write!(out, " {comm} cpu {cpu}ms wait {wait}ms;");
        }
    }
    out += "\n  steal/irq ms:";
    for (cpu, ((steal, irq), (steal0, irq0))) in b.cpus.iter().zip(&a.cpus).enumerate() {
        let _ = write!(
            out,
            " cpu{cpu} {}/{}",
            (steal - steal0) * 10,
            (irq - irq0) * 10
        );
    }
    out.push('\n');
    slow_runs(sh, &mut out);
    out
}

/// The 8 worst runs of slow slots.
fn slow_runs(sh: &Shared, out: &mut String) {
    let load = |v: &[AtomicU64]| v.iter().map(|s| s.load(Relaxed)).collect::<Vec<u64>>();
    let (slots, late) = (load(&sh.lat.slots), load(&sh.lat.late_slots));
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for (i, _) in slots.iter().enumerate().filter(|&(_, &v)| v > SLOW) {
        match runs.last_mut() {
            Some((_, end)) if *end + 1 == i => *end = i,
            _ => runs.push((i, i)),
        }
    }
    let max = |v: &[u64], (a, b): (usize, usize)| {
        v.get(a..=b)
            .and_then(|s| s.iter().copied().max())
            .unwrap_or(0)
    };
    runs.sort_by_key(|&r| Reverse(max(&slots, r)));
    let _ = writeln!(out, "  runs over 3 ms: {}", runs.len());
    let ms = |ns: u64| ns as f64 / 1e6;
    let secs = |slot: usize| (slot as u64 * SLOT) as f64 / 1e9;
    for &r in runs.iter().take(8) {
        let _ = writeln!(
            out,
            "    +{:.2}s..+{:.2}s delivery max {:.1}ms, sends late max {:.1}ms, steal ms{}",
            secs(r.0),
            secs(r.1 + 1),
            ms(max(&slots, r)),
            ms(max(&late, r)),
            steal_near(sh, r),
        );
    }
}

/// Steal per CPU from 30 ms before slots `r` were due to 30 ms after.
fn steal_near(sh: &Shared, (a, b): (usize, usize)) -> String {
    let lo = sh.window().0;
    let from = (lo + a as u64 * SLOT).saturating_sub(30_000_000);
    let to = lo + (b as u64 + 1) * SLOT + 30_000_000;
    let Some(ticks) = sh.steal.within(from, to) else {
        return " ?".into();
    };
    let mut out = String::new();
    for (cpu, t) in ticks.iter().enumerate().filter(|&(_, &t)| t > 0) {
        let _ = write!(out, " cpu{cpu} {}", t * 10);
    }
    if out.is_empty() { " none".into() } else { out }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::super::{Fixed, Plan, Scenario};
    use super::*;

    #[test]
    fn the_report_lists_slow_runs_with_the_steal_around_them() {
        let plan = Plan::new(Scenario::P2p, &Fixed::default());
        let sh = Shared::new(Arc::new(plan));
        sh.window.0.store(0, Relaxed);
        sh.window.1.store(1_000_000_000, Relaxed);
        for (slot, ns) in [(3, 4_000_000), (4, 9_000_000), (7, 5_000_000)] {
            sh.lat.slots[slot].store(ns, Relaxed);
        }
        sh.lat.late_slots[4].store(2_000_000, Relaxed);
        sh.steal.push(0, vec![0, 0]);
        sh.steal.push(50_000_000, vec![0, 3]);
        let a = Sched::now();
        let out = report(&sh, &a, &Sched::now());
        assert!(out.contains("runs over 3 ms: 2"), "{out}");
        let runs: Vec<&str> = out.lines().filter(|l| l.starts_with("    +")).collect();
        assert_eq!(runs.len(), 2, "{out}");
        assert!(
            runs[0].starts_with("    +0.03s..+0.05s delivery max 9.0ms"),
            "{out}"
        );
        assert!(
            runs[0].contains("sends late max 2.0ms, steal ms cpu1 30"),
            "{out}"
        );
        assert!(runs[1].ends_with("steal ms none"), "{out}");
    }
}

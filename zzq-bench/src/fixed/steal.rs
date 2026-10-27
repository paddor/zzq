//! Host CPU steal per CPU over a run, from `/proc/stat`.

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::Duration;

use super::Clock;

/// Time between samples.
const EVERY: Duration = Duration::from_millis(10);

/// Steal ticks per CPU, by ns on the run's clock.
#[derive(Debug, Default)]
pub(crate) struct Steal {
    samples: Mutex<Vec<(u64, Vec<u64>)>>,
}

/// Samples steal every `EVERY` until `stop`. A run lasts a minute or two,
/// so the samples stay small.
pub(crate) fn sample(steal: &Steal, clock: &Clock, stop: &AtomicBool) {
    while !stop.load(Relaxed) {
        let stat = std::fs::read_to_string("/proc/stat").unwrap_or_default();
        let ticks = per_cpu(&stat, 8);
        let now = clock.now();
        steal.lock().push((now, ticks));
        thread::sleep(EVERY);
    }
}

/// Field `field` of each `cpuN` line of `/proc/stat`: 6 is irq, 7 softirq,
/// 8 steal.
pub(crate) fn per_cpu(stat: &str, field: usize) -> Vec<u64> {
    stat.lines()
        .filter(|l| l.starts_with("cpu") && !l.starts_with("cpu "))
        .map(|l| {
            l.split_whitespace()
                .nth(field)
                .and_then(|x| x.parse().ok())
                .unwrap_or(0)
        })
        .collect()
}

impl Steal {
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(u64, Vec<u64>)>> {
        self.samples.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Steal ticks on the worst CPU from `from` to `to`, by the samples
    /// just outside them.
    pub(crate) fn worst(&self, from: u64, to: u64) -> u64 {
        let samples = self.lock();
        let a = samples.iter().rev().find(|(t, _)| *t <= from);
        let b = samples.iter().find(|(t, _)| *t >= to).or(samples.last());
        let (Some(a), Some(b)) = (a, b) else { return 0 };
        a.1.iter()
            .zip(&b.1)
            .map(|(x, y)| y.saturating_sub(*x))
            .max()
            .unwrap_or(0)
    }

    /// Steal ticks per CPU between the first and the last sample in
    /// `from..to`, or `None` without a sample there.
    pub(crate) fn within(&self, from: u64, to: u64) -> Option<Vec<u64>> {
        let samples = self.lock();
        let mut inside = samples.iter().filter(|(t, _)| (from..to).contains(t));
        let first = inside.next()?;
        let last = inside.next_back().unwrap_or(first);
        Some(
            first
                .1
                .iter()
                .zip(&last.1)
                .map(|(x, y)| y.saturating_sub(*x))
                .collect(),
        )
    }

    #[cfg(test)]
    pub(crate) fn push(&self, at: u64, ticks: Vec<u64>) {
        self.lock().push((at, ticks));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAT: &str = "cpu  1 2 3 4 5 6 7 99 0 0\n\
                        cpu0 1 2 3 4 5 6 7 8 0 0\n\
                        cpu1 1 2 3 4 5 9 1 3 0 0\n\
                        intr 1 2 3\n";

    #[test]
    fn per_cpu_reads_cpu_lines_only() {
        assert_eq!(per_cpu(STAT, 8), [8, 3]);
        assert_eq!(per_cpu(STAT, 6), [6, 9]);
        assert_eq!(per_cpu("", 8), []);
    }

    #[test]
    fn steal_is_measured_by_the_samples_around_a_span() {
        let s = Steal::default();
        assert_eq!(s.worst(0, 10), 0);
        assert_eq!(s.within(0, 10), None);
        s.push(0, vec![0, 0]);
        s.push(10, vec![1, 0]);
        s.push(20, vec![1, 5]);
        s.push(30, vec![4, 5]);
        assert_eq!(s.worst(5, 15), 5);
        assert_eq!(s.worst(20, 30), 3);
        assert_eq!(s.within(10, 31), Some(vec![3, 5]));
        assert_eq!(s.within(10, 11), Some(vec![0, 0]));
    }
}

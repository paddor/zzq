//! Latency histogram shared by all connections.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Buckets per power of two of nanoseconds, about 3% apart.
const SUB: u32 = 32;

/// Latency counts in buckets. Counters are statistics only, so `Relaxed`.
#[derive(Debug)]
pub(crate) struct Hist {
    buckets: Box<[AtomicU64]>,
}

impl Hist {
    pub(crate) fn new() -> Hist {
        Hist {
            buckets: (0..64 * SUB).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.buckets.len()
    }

    pub(crate) fn record(&self, ns: u64) {
        if let Some(b) = self.buckets.get(index(ns)) {
            b.fetch_add(1, Relaxed);
        }
    }

    pub(crate) fn clear(&self) {
        for b in &self.buckets {
            b.store(0, Relaxed);
        }
    }

    pub(crate) fn counts(&self) -> Vec<u64> {
        self.buckets.iter().map(|b| b.load(Relaxed)).collect()
    }
}

fn index(ns: u64) -> usize {
    if ns < u64::from(SUB) {
        return ns as usize;
    }
    let msb = ns.ilog2();
    let shift = msb - SUB.trailing_zeros();
    ((shift + 1) * SUB + ((ns >> shift) as u32 & (SUB - 1))) as usize
}

/// The lowest value of bucket `i`.
fn value(i: usize) -> u64 {
    let i = i as u32;
    if i < SUB {
        return u64::from(i);
    }
    let shift = i / SUB - 1;
    u64::from(SUB + i % SUB) << shift
}

/// Microseconds at quantiles `qs` over bucket `counts`, and the count.
pub(crate) fn quantiles<const N: usize>(counts: &[u64], qs: [f64; N]) -> ([f64; N], u64) {
    let total: u64 = counts.iter().sum();
    let at = |q: f64| {
        let rank = ((total as f64 * q).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, &c) in counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return value(i) as f64 / 1e3;
            }
        }
        0.0
    };
    (qs.map(at), total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_keep_values_within_three_percent() {
        for ns in [0, 31, 32, 1_000, 123_456, 10_000_000_000, u64::MAX] {
            let low = value(index(ns));
            assert!(low <= ns, "{ns}: {low}");
            assert!(ns - low <= ns / 32, "{ns}: {low}");
        }
    }

    #[test]
    fn quantiles_read_the_recorded_values() {
        let h = Hist::new();
        for us in 1..=100 {
            h.record(us * 1_000);
        }
        let ([p50, max], n) = quantiles(&h.counts(), [0.5, 1.0]);
        assert_eq!(n, 100);
        assert!((48.0..=50.0).contains(&p50), "{p50}");
        assert!((97.0..=100.0).contains(&max), "{max}");
        h.clear();
        assert_eq!(quantiles(&h.counts(), [0.5]), ([0.0], 0));
    }
}

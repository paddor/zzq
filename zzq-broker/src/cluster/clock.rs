//! Cluster time: this node's Unix time, moved to the time most of the
//! three clocks agree on once the node's samples show it. Journals write
//! deadlines in it and followers read them in it, so one wrong clock moves
//! none of them.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant};

use crate::state::Clock;

#[derive(Debug)]
pub(crate) struct ClusterClock {
    /// Tokio's clock at the node's `now` zero.
    origin: Instant,
    /// This node's Unix time then, in milliseconds.
    origin_millis: u64,
    /// The majority's clock minus this one, in milliseconds.
    correction: AtomicI64,
    /// The samples showed a majority since the start.
    read: AtomicBool,
    tolerance_millis: u64,
}

impl ClusterClock {
    /// This host's clock from now, moved by `offset_millis`.
    pub(crate) fn new(offset_millis: i64, tolerance_millis: u64) -> ClusterClock {
        let origin = crate::time::now();
        let own = millis(crate::time::unix_now());
        ClusterClock {
            origin,
            origin_millis: own.saturating_add_signed(offset_millis),
            correction: AtomicI64::new(0),
            read: AtomicBool::new(false),
            tolerance_millis,
        }
    }

    pub(crate) const fn origin_millis(&self) -> u64 {
        self.origin_millis
    }

    /// The node's time now: since the origin.
    pub(crate) fn since_origin(&self) -> Duration {
        crate::time::now().saturating_duration_since(self.origin)
    }

    /// Both clocks now, Unix time in cluster time.
    pub(crate) fn clock(&self) -> Clock {
        self.with(self.correction())
    }

    /// Both clocks now, Unix time this node's moved by `correction`.
    pub(crate) fn with(&self, correction: i64) -> Clock {
        let now = crate::time::now();
        let own = self
            .origin_millis
            .saturating_add(millis(now.saturating_duration_since(self.origin)));
        Clock::at(now, own.saturating_add_signed(correction))
    }

    // Relaxed: the correction and the flag are each read alone. A stale
    // correction moves one record's deadlines by what the last sample
    // changed, within the tolerance but at the first reading; a stale flag
    // holds expiry a sweep longer.
    pub(crate) fn correction(&self) -> i64 {
        self.correction.load(Ordering::Relaxed)
    }

    /// Whether the samples showed a majority since the start.
    pub(crate) fn read(&self) -> bool {
        self.read.load(Ordering::Relaxed)
    }

    /// Takes the majority's offset from the samples, if they show one.
    /// Without one, the last stays: it was right when a majority agreed.
    pub(crate) fn observe(&self, majority: Option<i64>) {
        if let Some(m) = majority {
            self.correction.store(m, Ordering::Relaxed);
            self.read.store(true, Ordering::Relaxed);
        }
    }

    /// Whether deadlines read with `correction` are off cluster time by
    /// more than the tolerance.
    pub(crate) fn moved_from(&self, correction: i64) -> bool {
        self.correction().abs_diff(correction) > self.tolerance_millis
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cluster_time_is_this_clock_moved_by_the_majority() {
        let c = ClusterClock::new(-5000, 1000);
        let own = millis(crate::time::unix_now()) - 5000;
        assert_eq!(c.origin_millis(), own);
        assert!(!c.read());
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(c.since_origin(), Duration::from_secs(2));
        let ms = Clock::wall_ms;
        assert_eq!(ms(c.clock()), own + 2000);
        c.observe(None);
        assert!(!c.read(), "no majority yet");
        c.observe(Some(5000));
        assert!(c.read());
        assert_eq!(ms(c.clock()), own + 7000);
        c.observe(None);
        assert_eq!(c.correction(), 5000, "the last majority stays");
        assert!(c.moved_from(0));
        assert!(!c.moved_from(4500));
    }
}

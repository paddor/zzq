//! Whether this node's wall clock agrees with the other two, and what the
//! majority's clock reads. No I/O: the owner supplies each sample and its
//! own wall time. Monotonic time alone decides which samples still count.

use std::time::Duration;

use crate::NodeId;

/// How often a node sends its wall time to each peer.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// How long a received sample counts.
pub const SAMPLE_FRESH: Duration = Duration::from_secs(5);

/// A node's wall clock, as its owner supplies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockConfig {
    /// Unix time in milliseconds at `now` zero. The node reads no clock:
    /// its wall time is this plus `now`.
    pub origin_millis: u64,
    /// The most two clocks may differ and still agree, in milliseconds.
    pub tolerance_millis: u64,
}

/// What a node's samples say about its clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockStatus {
    /// Each peer's fresh offset in milliseconds, peer minus this node.
    pub offsets: [(NodeId, Option<i64>); 2],
    /// The majority's clock minus this node's: see [`ClockGuard::majority`].
    pub majority: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    offset_millis: i64,
    arrived: Duration,
}

/// The latest sample of each of the two peers. A delayed sample looks like
/// a slow peer, so it can withhold a majority but never make one of a
/// wrong clock: a majority needs two of the three clocks to agree.
#[derive(Debug, Clone)]
pub struct ClockGuard {
    tolerance_millis: u64,
    peers: [(NodeId, Option<Sample>); 2],
}

impl ClockGuard {
    /// Compare with `peers`, allowing `tolerance_millis` of disagreement.
    pub const fn new(peers: [NodeId; 2], tolerance_millis: u64) -> Self {
        let [a, b] = peers;
        Self {
            tolerance_millis,
            peers: [(a, None), (b, None)],
        }
    }

    /// Keep `peer`'s sample: its wall time as sent, the local wall time at
    /// receipt, and the local monotonic time. Unconfigured peers are ignored.
    pub fn observe(
        &mut self,
        peer: NodeId,
        peer_wall_millis: u64,
        local_wall_millis: u64,
        now: Duration,
    ) {
        let difference = i128::from(peer_wall_millis) - i128::from(local_wall_millis);
        let offset_millis =
            i64::try_from(difference).unwrap_or(if difference < 0 { i64::MIN } else { i64::MAX });
        if let Some((_, slot)) = self.peers.iter_mut().find(|(id, _)| *id == peer) {
            *slot = Some(Sample {
                offset_millis,
                arrived: now,
            });
        }
    }

    /// Each peer's fresh offset in milliseconds, peer minus local.
    pub fn offsets(&self, now: Duration) -> impl Iterator<Item = (NodeId, Option<i64>)> + '_ {
        self.peers.iter().map(move |(peer, sample)| {
            let fresh = sample.filter(|sample| now.saturating_sub(sample.arrived) <= SAMPLE_FRESH);
            (*peer, fresh.map(|sample| sample.offset_millis))
        })
    }

    /// The majority's clock as an offset from this one, in milliseconds:
    /// zero when this clock is part of the majority, else the median
    /// clock. `None` without a majority: two of the three clocks, fresh
    /// ones only, within the tolerance of their median.
    pub fn majority(&self, now: Duration) -> Option<i64> {
        let mut clocks = [0; 3];
        let mut n = 1;
        for offset in self.offsets(now).filter_map(|(_, offset)| offset) {
            if let Some(slot) = clocks.get_mut(n) {
                *slot = offset;
                n += 1;
            }
        }
        let clocks = clocks.get_mut(..n)?;
        clocks.sort_unstable();
        let median = *clocks.get((n - 1) / 2)?;
        let agreeing = clocks
            .iter()
            .filter(|&&clock| clock.abs_diff(median) <= self.tolerance_millis)
            .count();
        if agreeing < 2 {
            return None;
        }
        Some(if median.unsigned_abs() <= self.tolerance_millis {
            0
        } else {
            median
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: u64 = 3_600_000;
    const WALL: u64 = 10 * HOUR;
    const TOLERANCE: u64 = 1000;

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    /// A guard whose two peers sent their clocks `offsets` from ours.
    fn guard(offsets: [Option<i64>; 2]) -> ClockGuard {
        let mut guard = ClockGuard::new([node(1), node(2)], TOLERANCE);
        for (peer, offset) in [node(1), node(2)].into_iter().zip(offsets) {
            if let Some(offset) = offset {
                let wall = WALL.checked_add_signed(offset).unwrap();
                guard.observe(peer, wall, WALL, Duration::ZERO);
            }
        }
        guard
    }

    #[test]
    fn clocks_within_the_tolerance_need_no_correction() {
        for offsets in [
            [Some(5), Some(-20)],
            [Some(999), Some(-999)],
            [Some(3), None],
        ] {
            assert_eq!(
                guard(offsets).majority(Duration::ZERO),
                Some(0),
                "{offsets:?}"
            );
        }
    }

    #[test]
    fn a_clock_off_from_both_peers_takes_their_time() {
        let off = -i64::try_from(HOUR).unwrap();
        let majority = guard([Some(off), Some(off + 7)]).majority(Duration::ZERO);
        assert_eq!(majority, Some(off + 7), "the median clock");
    }

    #[test]
    fn one_wrong_peer_cannot_move_this_clock() {
        let late = i64::try_from(HOUR).unwrap();
        assert_eq!(
            guard([Some(late), Some(4)]).majority(Duration::ZERO),
            Some(0)
        );
        assert_eq!(
            guard([Some(-late), Some(-4)]).majority(Duration::ZERO),
            Some(0)
        );
    }

    #[test]
    fn without_a_majority_there_is_no_reading() {
        let cases = [
            [None, None],
            [Some(5000), None],
            [Some(-5000), None],
            [Some(-5000), Some(5000)],
            [Some(-1200), Some(1200)],
        ];
        for offsets in cases {
            assert_eq!(guard(offsets).majority(Duration::ZERO), None, "{offsets:?}");
        }
    }

    #[test]
    fn samples_lapse_after_the_fresh_window() {
        let off = -i64::try_from(HOUR).unwrap();
        let guard = guard([Some(off), Some(off)]);
        assert_eq!(guard.majority(SAMPLE_FRESH), Some(off));
        let later = SAMPLE_FRESH + Duration::from_millis(1);
        assert_eq!(guard.majority(later), None);
        assert!(guard.offsets(later).all(|(_, offset)| offset.is_none()));
    }

    #[test]
    fn unknown_peers_are_ignored_and_offsets_saturate() {
        let mut guard = ClockGuard::new([node(1), node(2)], TOLERANCE);
        guard.observe(node(9), WALL, WALL, Duration::ZERO);
        assert!(guard.offsets(Duration::ZERO).all(|(_, o)| o.is_none()));
        guard.observe(node(1), u64::MAX, 0, Duration::ZERO);
        guard.observe(node(2), 0, u64::MAX, Duration::ZERO);
        let offsets: Vec<_> = guard.offsets(Duration::ZERO).collect();
        assert_eq!(
            offsets,
            [(node(1), Some(i64::MAX)), (node(2), Some(i64::MIN))]
        );
    }
}

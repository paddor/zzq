//! The broker's clocks. Deadlines read Tokio's clock, so a paused runtime,
//! as in a simulation, drives keep alive, expiry and will delays. Unix
//! time is the wall clock at the first reading, moved on by the same.

use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The wall clock and Tokio's at the first reading.
static ANCHOR: OnceLock<(Instant, Duration)> = OnceLock::new();

/// Now, on Tokio's clock.
pub(crate) fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Unix time now.
pub(crate) fn unix_now() -> Duration {
    let (at, wall) = *ANCHOR.get_or_init(|| {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (now(), wall)
    });
    wall.saturating_add(now().saturating_duration_since(at))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_paused_runtime_moves_both_clocks_together() {
        let (t0, u0) = (now(), unix_now());
        tokio::time::advance(Duration::from_secs(90)).await;
        assert_eq!(now() - t0, Duration::from_secs(90));
        assert_eq!(unix_now().saturating_sub(u0), Duration::from_secs(90));
    }
}

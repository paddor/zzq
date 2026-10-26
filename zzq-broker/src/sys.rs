//! `$SYS/broker/...` status topics, published as retained messages.

use std::sync::Arc;
use std::time::{Duration, Instant};

use zzq_proto::QoS;

use crate::broker::{Shared, Stats, collect, publish};
use crate::msg::Msg;

/// Publishes the status topics every `every`, each only when it changed.
pub(crate) async fn run(shared: Arc<Shared>, every: Duration) {
    let started = Instant::now();
    let mut last: Vec<String> = Vec::new();
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Ok(stats) = collect(&shared).await else {
            return;
        };
        let values = values(&stats, started.elapsed());
        for (i, (topic, value)) in values.iter().enumerate() {
            if last.get(i) == Some(value) {
                continue;
            }
            let msg = Msg::new(topic, value.as_bytes(), &[], QoS::AtMostOnce, true);
            if publish(&shared, msg).is_err() {
                return;
            }
        }
        last = values.into_iter().map(|(_, v)| v).collect();
    }
}

fn values(s: &Stats, uptime: Duration) -> [(&'static str, String); 11] {
    let dropped = s.dropped_buffer_full
        + s.dropped_offline
        + s.dropped_queue_full
        + s.dropped_too_large
        + s.dropped_expired
        + s.dropped_session_ended
        + s.dropped_state_full;
    [
        (
            "$SYS/broker/version",
            format!("zzq {}", env!("CARGO_PKG_VERSION")),
        ),
        (
            "$SYS/broker/uptime",
            format!("{} seconds", uptime.as_secs()),
        ),
        ("$SYS/broker/clients/connected", s.connections.to_string()),
        ("$SYS/broker/sessions/count", s.sessions.to_string()),
        (
            "$SYS/broker/subscriptions/count",
            s.subscriptions.to_string(),
        ),
        (
            "$SYS/broker/retained messages/count",
            s.retained.to_string(),
        ),
        ("$SYS/broker/messages/received", s.publishes.to_string()),
        ("$SYS/broker/messages/sent", s.deliveries.to_string()),
        ("$SYS/broker/messages/dropped", dropped.to_string()),
        (
            "$SYS/broker/messages/expired",
            s.dropped_expired.to_string(),
        ),
        ("$SYS/broker/state/bytes", s.state_bytes.to_string()),
    ]
}

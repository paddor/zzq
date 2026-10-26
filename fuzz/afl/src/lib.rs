//! Fuzz target bodies and their seeds. The bodies have no AFL dependency,
//! so `tests/targets.rs` replays the seeds through them.

pub mod seeds;

use zzq_proto::encode::{self, PublishHeader};
use zzq_proto::trie::TopicTrie;
use zzq_proto::{Packet, Version, decode, sn, topic};

/// A fuzz target body.
pub type Target = fn(&[u8]);

/// Every target, by name.
pub const TARGETS: [(&str, Target); 7] = [
    ("mqtt", mqtt),
    ("mqtt_sn", mqtt_sn),
    ("websocket", websocket),
    ("retained_log", retained_log),
    ("state_image", state_image),
    ("endpoint", endpoint),
    ("topics", topics),
];

/// Decodes packets as both versions. A decoded PUBLISH encodes back to a
/// packet that decodes to the same fields.
///
/// # Panics
///
/// When it finds a bug.
pub fn mqtt(data: &[u8]) {
    for version in [Version::V311, Version::V5] {
        let mut buf = data;
        while let Ok(Some((packet, n))) = decode(buf, version, usize::MAX) {
            if let Packet::Publish(p) = packet {
                let h = PublishHeader {
                    version,
                    dup: p.dup,
                    qos: p.qos,
                    retain: p.retain,
                    topic: p.topic,
                    pkid: p.pkid,
                    properties: p.properties.raw(),
                };
                let mut out = Vec::new();
                encode::publish(&h, p.payload, &mut out);
                let again = decode(&out, version, usize::MAX);
                assert!(
                    matches!(again, Ok(Some((Packet::Publish(q), m))) if q == p && m == out.len()),
                    "PUBLISH does not round-trip: {p:?}"
                );
            }
            buf = &buf[n..];
        }
    }
}

/// Decodes a datagram. A decoded message encodes back to one that decodes
/// to the same message.
///
/// # Panics
///
/// When it finds a bug.
pub fn mqtt_sn(data: &[u8]) {
    let Ok((packet, _)) = sn::decode(data) else {
        return;
    };
    let mut out = Vec::new();
    sn::encode(&packet, &mut out);
    let again = sn::decode(&out).map(|(p, _)| p);
    assert_eq!(again, Ok(packet), "MQTT-SN message does not round-trip");
}

/// A WebSocket upgrade request and frames, as the server reads them.
pub fn websocket(data: &[u8]) {
    zzq_tokio::fuzzing::websocket_server(data);
}

/// A retained message log, as the broker replays it on start.
pub fn retained_log(data: &[u8]) {
    zzq_broker::fuzzing::retained_log(data);
}

/// A state image, as the broker loads a dump. Restored sessions encode
/// back to an image that decodes.
///
/// # Panics
///
/// When it finds a bug.
pub fn state_image(data: &[u8]) {
    zzq_broker::fuzzing::state_image(data);
}

/// An endpoint URI that parses prints back to one that parses the same.
///
/// # Panics
///
/// When it finds a bug.
pub fn endpoint(data: &[u8]) {
    let Ok(uri) = std::str::from_utf8(data) else {
        return;
    };
    let Ok(e) = uri.parse::<zzq_tokio::Endpoint>() else {
        return;
    };
    let again = e.to_string().parse::<zzq_tokio::Endpoint>();
    assert!(
        again.as_ref().is_ok_and(|a| *a == e),
        "{uri:?} prints as {e} and parses as {again:?}"
    );
}

/// A filter and a topic, one per line. The trie and `topic::matches`
/// agree on valid ones.
///
/// # Panics
///
/// When it finds a bug.
pub fn topics(data: &[u8]) {
    let Ok(text) = std::str::from_utf8(data) else {
        return;
    };
    let Some((filter, name)) = text.split_once('\n') else {
        return;
    };
    if topic::validate_filter(filter).is_err() || topic::validate_name(name).is_err() {
        return;
    }
    // The trie holds plain filters; shared ones match on their inner filter.
    if topic::is_shared(filter) {
        return;
    }
    let mut trie = TopicTrie::new();
    trie.insert(filter, ());
    let mut hit = false;
    trie.for_each_match(name, |()| hit = true);
    assert_eq!(hit, topic::matches(filter, name), "{filter:?} vs {name:?}");
}

//! Entry points for the fuzz targets in `fuzz/afl`. Not part of the API.

use crate::budget::LaneTotal;

/// Replays `data` as a retained message log.
pub fn retained_log(data: &[u8]) {
    let _ = crate::persist::parse_log(data);
}

/// Decodes `data` as a state image, and as the body of a record with a
/// valid checksum, which the checksum would otherwise keep mutations
/// from. Sessions it holds restore and encode to an image that decodes
/// again.
///
/// # Panics
///
/// When it finds a bug.
pub fn state_image(data: &[u8]) {
    state_records(data);
    state_records(&crate::state::framed(data));
}

fn state_records(data: &[u8]) {
    use crate::state::{Clock, ImageWriter, Record, read_image};
    let clock = Clock::now();
    let Ok(records) = read_image(data, clock) else {
        return;
    };
    let Ok(mut w) = ImageWriter::new(Vec::new(), clock) else {
        return;
    };
    for r in records {
        let written = match r {
            Record::Session(st) => {
                let (s, _) = crate::session::Session::restore(*st, LaneTotal::default(), 2);
                w.session(&s)
            }
            Record::Retained(m) => w.retained(&m),
        };
        assert!(written.is_ok());
    }
    let image = w.finish().unwrap_or_default();
    assert!(
        read_image(&image, clock).is_ok(),
        "restored state does not decode"
    );
}

/// Seeds for [`state_image`]: an image with a session that holds
/// messages and a retained message, and the session record's body.
#[must_use]
pub fn state_image_seeds() -> Vec<Vec<u8>> {
    let image = state_image_seed();
    // Header, then the first record's length and checksum.
    let body = image.get(10..18).and_then(|len| {
        let len = usize::try_from(u64::from_be_bytes(len.try_into().ok()?)).ok()?;
        image.get(26..26 + len)
    });
    let body = body.map(<[u8]>::to_vec).unwrap_or_default();
    vec![image, body]
}

fn state_image_seed() -> Vec<u8> {
    use zzq_proto::{QoS, SubscriptionOptions, Version};

    use crate::index::SubIds;
    use crate::msg::Msg;
    use crate::session::{QueueLimit, Queued, Session};
    use crate::state::{Clock, ImageWriter};

    let mut s = Session::new("dev".into(), Version::V5, LaneTotal::default());
    s.expiry = 60;
    s.username = Some("user".into());
    s.subscribe("a/+", SubscriptionOptions::from(QoS::AtLeastOnce), 3, 1);
    s.subscribe(
        "$share/g/b/#",
        SubscriptionOptions::from(QoS::ExactlyOnce),
        0,
        1,
    );
    s.awaiting_rel.insert(9);
    let limit = QueueLimit {
        count: usize::MAX,
        bytes: usize::MAX,
        state_full: false,
    };
    for (i, qos) in [QoS::AtLeastOnce, QoS::ExactlyOnce].into_iter().enumerate() {
        let item = Queued {
            msg: Msg::new("a/b", &[b'0' + i as u8], &[1, 1], qos, false),
            qos,
            retain: false,
            sub_id: SubIds::one(3),
        };
        s.deliver_qos12(item, limit);
    }
    let retained = Msg::new("r", b"v", &[], QoS::AtLeastOnce, true);
    let Ok(mut w) = ImageWriter::new(Vec::new(), Clock::now()) else {
        return Vec::new();
    };
    let _ = w.session(&s);
    let _ = w.retained(&retained);
    w.finish().unwrap_or_default()
}

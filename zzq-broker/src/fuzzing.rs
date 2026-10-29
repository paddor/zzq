//! Entry points for the fuzz targets in `fuzz/afl`. Not part of the API.

use crate::budget::LaneTotal;

/// Replays `data` as a record of a retained message journal: a kind
/// byte, then the body.
pub fn retained_record(data: &[u8]) {
    let _ = crate::retain::replay_one(data);
}

/// Seeds for [`retained_record`]: a message set, a topic cleared, and a
/// stripe cleared.
#[must_use]
pub fn retained_record_seeds() -> Vec<Vec<u8>> {
    crate::retain::record_seeds()
}

/// Replays `data` as records of a lane journal: each a kind byte, a big
/// endian `u16` length and the body.
pub fn lane_records(data: &[u8]) {
    let _ = crate::state::journal::replay_records(data);
}

/// Seeds for [`lane_records`]: a session's life in records, and each
/// record alone.
#[must_use]
pub fn lane_records_seeds() -> Vec<Vec<u8>> {
    use crate::state::journal::{framed, record_seeds};
    let records = record_seeds();
    let mut seeds = vec![framed(&records)];
    seeds.extend(records.chunks(1).map(framed));
    seeds
}

/// Decodes `data` as a state image, whole and in pieces, and as the body
/// of a record with a valid checksum, which the checksum would otherwise
/// keep mutations from. Sessions it holds restore and encode to an image
/// that decodes again.
///
/// # Panics
///
/// When it finds a bug.
pub fn state_image(data: &[u8]) {
    state_records(data);
    state_records(&crate::state::framed(data));
}

fn state_records(data: &[u8]) {
    use crate::state::{Clock, ImageReader, ImageWriter, Record, read_image};
    let clock = Clock::now();
    let whole = read_image(data, clock);
    let mut pieces = ImageReader::new(clock);
    let read = data
        .chunks(1 + data.len() % 7)
        .try_for_each(|p| pieces.read(p))
        .and_then(|()| pieces.finish());
    assert_eq!(
        whole.as_ref().map(Vec::len).ok(),
        read.map(|r| r.len()).ok(),
        "pieces decode otherwise"
    );
    let Ok(records) = whole else {
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
    use crate::session::{Kept, QueueLimit, Queued, Session};
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
    s.inbound.awaiting_rel.insert(9);
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
        s.deliver_qos12(Kept::own(item), limit);
    }
    let retained = Msg::new("r", b"v", &[], QoS::AtLeastOnce, true);
    let Ok(mut w) = ImageWriter::new(Vec::new(), Clock::now()) else {
        return Vec::new();
    };
    let _ = w.session(&s);
    let _ = w.retained(&retained);
    w.finish().unwrap_or_default()
}

/// Reads `data` as a cluster node reads what its peers send and its exit
/// dump: an op of journal records, an image, and a dump. A state an image
/// rebuilt cuts an image that rebuilds it again.
///
/// # Panics
///
/// When it finds a bug.
pub fn cluster_state(data: &[u8]) {
    crate::cluster::fuzzing::state(data);
}

/// Seeds for [`cluster_state`]: ops of lane and retained records, an
/// image of the state they leave, and a dump of it.
#[must_use]
pub fn cluster_state_seeds() -> Vec<Vec<u8>> {
    crate::cluster::fuzzing::seeds()
}

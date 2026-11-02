//! What a node reads from its peers and its exit dump, for the fuzz
//! targets: ops of journal records, images, and dumps.

use bytes::Bytes;
use zzq_cluster::OpNumber;

use super::app::Accumulators;
use super::clock::ClusterClock;
use super::{GroupWriter, Journal, dump, image};
use crate::disk::Append;

const LANES: usize = 2;
const LEVELS: usize = 8;

fn clock() -> ClusterClock {
    ClusterClock::new(0, 1000)
}

/// Applies `data` as an op, and reads it as an image and as a dump. A
/// state an image rebuilt cuts an image that rebuilds it again, also read
/// in a cluster time an hour away.
///
/// # Panics
///
/// When it finds a bug.
pub(crate) fn state(data: &[u8]) {
    let bytes = Bytes::copy_from_slice(data);
    let clock = clock();
    let _ = Accumulators::new(LANES, LEVELS, &clock).apply(OpNumber(1), data, &clock);
    install(&bytes);
    if let Ok(d) = dump::decode(&bytes) {
        install(&d.image);
    }
}

fn install(bytes: &Bytes) {
    let Ok(parts) = image::decode(bytes) else {
        return;
    };
    let clock = clock();
    let Ok(acc) = Accumulators::from_image(LANES, LEVELS, &parts, &clock) else {
        return;
    };
    let again = image::decode(&image::encode(&acc.image(OpNumber(1), &clock)));
    assert!(
        again.is_ok_and(|p| Accumulators::from_image(LANES, LEVELS, &p, &clock).is_ok()),
        "an image of a rebuilt state rebuilds it"
    );
    clock.observe(Some(-3_600_000));
    assert!(
        acc.anchored(OpNumber(1), &clock).is_ok(),
        "a rebuilt state reads in another time"
    );
}

/// An op with a session's life in records, one with retained messages,
/// an image of the state they leave, and a dump of it.
pub(crate) fn seeds() -> Vec<Vec<u8>> {
    let lane = crate::broker::lane_for("c", LANES);
    let mut sessions = GroupWriter::new(Journal::Lane(u16::try_from(lane).unwrap_or(0)));
    for r in crate::state::journal::record_seeds() {
        let _ = sessions.append(r[0], &[&r[1..]]);
    }
    let stripe = crate::retain::stripe_of("a/b");
    let mut retained = GroupWriter::new(Journal::Retained(u8::try_from(stripe).unwrap_or(0)));
    for r in crate::retain::record_seeds() {
        let _ = retained.append(r[0], &[&r[1..]]);
    }
    let ops = [sessions.finish(), retained.finish()];
    let clock = clock();
    let mut acc = Accumulators::new(LANES, LEVELS, &clock);
    for (n, op) in (1..).zip(&ops) {
        let _ = acc.apply(OpNumber(n), op, &clock);
    }
    let image = image::encode(&acc.image(OpNumber(2), &clock));
    let dump = dump::encode(b"node", &image);
    let mut seeds: Vec<Vec<u8>> = ops.iter().map(|b| b.to_vec()).collect();
    seeds.push(image.to_vec());
    seeds.push(dump.to_vec());
    seeds
}

use std::time::Duration;

use zzq_proto::QoS;

use super::*;

fn replay(records: &[Vec<u8>]) -> Result<Vec<SessionState>, &'static str> {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    for r in records {
        sessions.apply(r[0], &r[1..], clock, |_| true)?;
    }
    Ok(sessions.finish(crate::time::now()))
}

fn seeds() -> Vec<Vec<u8>> {
    record_seeds()
}

#[derive(Default)]
struct Records(Vec<(u8, Vec<u8>)>);

impl crate::disk::Append for Records {
    fn append(&mut self, kind: u8, parts: &[&[u8]]) -> std::io::Result<u64> {
        self.0.push((kind, parts.concat()));
        Ok(self.0.len() as u64)
    }
}

/// The body of a handoff of session `c`, for packet ID 3.
fn pending_body(clock: Clock) -> Vec<u8> {
    use crate::state::handoff::{Pending, Target};

    let pending = Pending {
        seq: 9,
        ready: false.into(),
        congested: std::sync::OnceLock::new(),
        publisher: "c".into(),
        birth: 0,
        pkid: 3,
        msg: crate::msg::Msg::new("a/b", b"once", &[], QoS::ExactlyOnce, false),
        targets: vec![Target {
            lane: 1,
            client: "target".into(),
            birth: 7,
            qos: QoS::ExactlyOnce,
            retain: false,
            sub_id: SubIds::one(5),
        }],
    };
    let mut body = Vec::new();
    put_pending(&mut body, &pending, clock);
    body
}

#[test]
fn a_handoff_snapshot_preserves_receipts_and_does_not_revive_a_released_packet_id() {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    let s = seeds();
    sessions
        .apply(SESSION, &s[0][1..], clock, |_| true)
        .unwrap();
    let mut body = pending_body(clock);
    sessions.apply(HANDOFF, &body, clock, |_| true).unwrap();
    body.clear();
    inbound(&mut body, "c", 3);
    sessions
        .apply(INBOUND_DONE, &body, clock, |_| true)
        .unwrap();
    body.clear();
    body.extend_from_slice(&1u16.to_be_bytes());
    body.extend_from_slice(&8u64.to_be_bytes());
    sessions.apply(RECEIVED, &body, clock, |_| true).unwrap();
    let mut image = Records::default();
    sessions.image(&mut image, clock).unwrap();
    let mut rebuilt = Sessions::default();
    for (kind, body) in image.0 {
        rebuilt.apply(kind, &body, clock, |_| true).unwrap();
    }
    let lane = rebuilt.into_lane(crate::time::now());
    assert_eq!(lane.sessions[0].awaiting_rel, [] as [u16; 0]);
    assert_eq!(lane.handoffs.received(1), 8);
    assert_eq!(lane.handoffs.serial, 9);
    assert_eq!(lane.handoffs.pending[&9].targets[0].birth, 7);
    assert_eq!(lane.handoffs.pending[&9].msg.payload(), b"once");
}

/// The snapshot before it never ended: the lane went on without it, and the
/// handoff is part of what the lane did since.
#[test]
fn a_live_handoff_after_a_cut_off_snapshot_counts() {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    let s = seeds();
    sessions
        .apply(SESSION, &s[0][1..], clock, |_| true)
        .unwrap();
    sessions.apply(CLEAR, &[], clock, |_| true).unwrap();
    let image = s.iter().find(|r| r[0] == IMAGE).unwrap();
    sessions.apply(IMAGE, &image[1..], clock, |_| true).unwrap();
    let mut body = pending_body(clock);
    sessions.apply(HANDOFF, &body, clock, |_| true).unwrap();
    body.clear();
    client(&mut body, "c");
    sessions.apply(WILL_SENT, &body, clock, |_| true).unwrap();
    let lane = sessions.into_lane(crate::time::now());
    assert_eq!(lane.handoffs.pending[&9].msg.payload(), b"once");
    assert_eq!(lane.sessions[0].awaiting_rel, [3]);
}

/// A record long enough to go in parts, as [`Sessions::apply`] gets them.
fn long_body() -> Vec<u8> {
    let mut body = Vec::new();
    client(&mut body, "c");
    body.resize(body.len() + 2 * PART_BYTES, 0);
    body
}

#[test]
fn a_long_record_goes_in_parts_and_replays_whole() {
    let mut out = Records::default();
    let body = long_body();
    let (head, tail) = body.split_at(PART_BYTES / 2);
    let n = append_record(&mut out, SUB, &[head, tail]).unwrap();
    assert_eq!(n, out.0.len() as u64);
    let kinds: Vec<u8> = out.0.iter().map(|r| r.0).collect();
    assert_eq!(kinds, [PART, PART, PART_END]);
    let mut sessions = Sessions::default();
    for (kind, body) in &out.0[..2] {
        sessions.apply(*kind, body, Clock::now(), |_| true).unwrap();
    }
    assert_eq!(sessions.parts.len(), body.len() - out.0[2].1.len() + 1);
    // It reached the session records whole, and names no session here.
    let (kind, last) = &out.0[2];
    let refused = sessions.apply(*kind, last, Clock::now(), |_| true);
    assert_eq!(refused, Err("record of an unknown session"));
    assert_eq!(sessions.parts, [] as [u8; 0]);
}

#[test]
fn a_short_record_goes_as_it_is() {
    let mut out = Records::default();
    append_record(&mut out, SUB, &[b"ab", b"c"]).unwrap();
    assert_eq!(out.0, [(SUB, b"abc".to_vec())]);
}

#[test]
fn what_a_failed_write_left_of_a_long_record_is_dropped() {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    sessions.apply(PART, &[1, 9, 9], clock, |_| true).unwrap();
    sessions.apply(CLEAR, &[], clock, |_| true).unwrap();
    assert_eq!(sessions.parts, [] as [u8; 0]);
    sessions.apply(PART, &[1, 9, 9], clock, |_| true).unwrap();
    let whole = pending_body(clock);
    let (head, tail) = whole.split_at(5);
    let mut part = vec![1];
    part.extend_from_slice(head);
    sessions.apply(PART, &part, clock, |_| true).unwrap();
    let mut end = vec![HANDOFF];
    end.extend_from_slice(tail);
    sessions.apply(PART_END, &end, clock, |_| true).unwrap();
    let lane = sessions.into_lane(crate::time::now());
    assert_eq!(lane.handoffs.pending[&9].msg.payload(), b"once");
}

#[test]
fn malformed_parts_are_refused() {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    assert!(sessions.apply(PART, &[], clock, |_| true).is_err());
    assert!(sessions.apply(PART_END, &[], clock, |_| true).is_err());
    // A piece cannot end as a piece: it is no record of its own.
    assert!(
        sessions
            .apply(PART_END, &[PART, 1], clock, |_| true)
            .is_err()
    );
    let mut nested = vec![PART, 0, 0, 0, 1, 1];
    assert!(
        sessions
            .apply(TRANSACTION, &nested, clock, |_| true)
            .is_err()
    );
    nested[0] = HANDOFF_KEPT;
    assert!(
        sessions
            .apply(TRANSACTION, &nested, clock, |_| true)
            .is_err()
    );
}

#[test]
fn a_replica_image_keeps_a_long_record_that_has_not_ended() {
    let clock = Clock::now();
    let whole = pending_body(clock);
    let (head, tail) = whole.split_at(5);
    let mut sessions = Sessions::default();
    let mut part = vec![1];
    part.extend_from_slice(head);
    sessions.apply(PART, &part, clock, |_| true).unwrap();
    let mut image = Records::default();
    sessions.image(&mut image, clock).unwrap();
    let mut rebuilt = Sessions::default();
    for (kind, body) in image.0 {
        rebuilt.apply(kind, &body, clock, |_| true).unwrap();
    }
    let mut end = vec![HANDOFF];
    end.extend_from_slice(tail);
    rebuilt.apply(PART_END, &end, clock, |_| true).unwrap();
    let lane = rebuilt.into_lane(crate::time::now());
    assert_eq!(lane.handoffs.pending[&9].msg.payload(), b"once");
}

#[test]
fn nested_or_torn_transactions_are_refused() {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    for body in [&[TRANSACTION, 0, 0, 0, 0][..], &[RECEIVED, 0, 0, 0, 10, 0]] {
        assert!(sessions.apply(TRANSACTION, body, clock, |_| true).is_err());
    }
    let mut body = vec![RECEIVED, 0, 0, 0, 10, 0, 2];
    body.extend_from_slice(&5u64.to_be_bytes());
    sessions.apply(TRANSACTION, &body, clock, |_| true).unwrap();
    assert_eq!(
        sessions.into_lane(crate::time::now()).handoffs.received(2),
        5
    );
}

#[test]
fn session_records_without_birth_ids_still_replay() {
    let s = seeds();
    for kind in [0, 3] {
        let old = &s[kind];
        let mut replay = Sessions::default();
        replay
            .apply(old[0], &old[1..old.len() - 8], Clock::now(), |_| true)
            .unwrap();
        assert_eq!(replay.finish(crate::time::now())[0].birth, 0);
    }
}

#[test]
fn a_connected_session_lost_its_connection_at_the_restart() {
    let s = seeds();
    let got = replay(&s[..2]).unwrap();
    assert_eq!(got.len(), 1);
    let st = &got[0];
    assert_eq!((&*st.client_id, st.username.as_deref()), ("c", Some("u")));
    assert_eq!((st.version, st.expiry), (Version::V5, 60));
    assert_eq!(st.subs.len(), 1);
    assert!(st.will.is_none(), "a lost connection has no will left");
    let (due, will) = st.delayed_will.as_ref().unwrap();
    assert_eq!(will.topic(), "will/t");
    let delay = due.saturating_duration_since(crate::time::now());
    assert!(delay > Duration::from_secs(3) && delay <= Duration::from_secs(5));
    let left = st.expires_at.unwrap() - crate::time::now();
    assert!(left > Duration::from_secs(58), "{left:?}");
}

#[test]
fn records_follow_the_session_through_its_life() {
    let s = seeds();
    // Subscribed, unsubscribed, then down with a delayed will.
    let got = replay(&[s[0].clone(), s[1].clone(), s[2].clone(), s[4].clone()]).unwrap();
    assert_eq!(got[0].subs.len(), 0);
    assert!(got[0].delayed_will.is_some() && got[0].will.is_none());
    let left = got[0].expires_at.unwrap() - crate::time::now();
    assert!(left > Duration::from_secs(50), "{left:?}");
    // The delayed will went out.
    let got = replay(&[s[0].clone(), s[4].clone(), s[5].clone()]).unwrap();
    assert!(got[0].delayed_will.is_none());
    // A new connection keeps the subscriptions.
    let got = replay(&[s[0].clone(), s[1].clone(), s[4].clone(), s[0].clone()]).unwrap();
    assert_eq!(got[0].subs.len(), 1);
    // The session ended; a clear drops all.
    assert!(replay(&[s[0].clone(), s[6].clone()]).unwrap().is_empty());
    assert!(
        replay(&[s[0].clone(), s[7].clone(), s[15].clone()])
            .unwrap()
            .is_empty()
    );
    // An end of a session the journal never held changes nothing.
    assert!(replay(&[s[6].clone()]).unwrap().is_empty());
}

#[test]
fn a_snapshot_counts_once_it_ends() {
    let s = seeds();
    let (session, image, clear, cleared) = (&s[0], &s[3], &s[7], &s[15]);
    let ids = |records: &[&Vec<u8>]| {
        let owned: Vec<Vec<u8>> = records.iter().map(|r| (*r).clone()).collect();
        replay(&owned).unwrap().len()
    };
    // A finished snapshot without the session ends it.
    assert_eq!(ids(&[session, clear, cleared]), 0);
    assert_eq!(ids(&[session, clear, image, cleared]), 1);
    // A cut off one counts for nothing; the lane went on after it.
    assert_eq!(ids(&[session, clear]), 1);
    assert_eq!(ids(&[clear, image, session]), 1);
    assert_eq!(ids(&[clear, image]), 0);
}

#[test]
fn an_image_restores_a_whole_session() {
    let s = seeds();
    let got = replay(&[s[7].clone(), s[3].clone(), s[15].clone()]).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].subs.len(), 1);
    // Not connected when imaged: its will stays as it was.
    assert!(got[0].will.is_some());
}

#[test]
fn records_that_no_lane_wrote_are_refused() {
    let s = seeds();
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    let refused = [
        (SUB, s[1][1..].to_vec()),
        (DOWN, s[4][1..].to_vec()),
        (WILL_SENT, s[5][1..].to_vec()),
        (99, s[5][1..].to_vec()),
        (CLEAR, b"x".to_vec()),
        (SESSION, [&s[0][1..], b"x"].concat()),
    ];
    for (kind, body) in refused {
        let r = sessions.apply(kind, &body, clock, |_| true);
        assert!(r.is_err(), "kind {kind}: {r:?}");
    }
    let r = sessions.apply(SESSION, &s[0][1..], clock, |_| false);
    assert_eq!(r, Err("session in the journal of another lane"));
    let mut bad = vec![];
    client(&mut bad, "c");
    bad.extend_from_slice(&[0, 9, 0, 0, 0, 0, 0]);
    assert_eq!(
        sessions.apply(SESSION, &bad, clock, |_| true),
        Err("invalid protocol level")
    );
    let mut sub_bad = vec![];
    sub(
        &mut sub_bad,
        "c",
        "a/#/b",
        SubscriptionOptions::from(QoS::AtMostOnce),
        0,
    );
    sessions
        .apply(SESSION, &s[0][1..], clock, |_| true)
        .unwrap();
    assert_eq!(
        sessions.apply(SUB, &sub_bad, clock, |_| true),
        Err("invalid filter")
    );
}

#[test]
fn records_replay_from_one_buffer() {
    assert_eq!(replay_records(&framed(&seeds()[..2])), Ok(1));
    assert_eq!(replay_records(&[SUB, 0, 9]), Err("torn"));
}

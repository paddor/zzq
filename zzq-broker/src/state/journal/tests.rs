use std::time::{Duration, Instant};

use zzq_proto::QoS;

use super::*;

fn replay(records: &[Vec<u8>]) -> Result<Vec<SessionState>, &'static str> {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    for r in records {
        sessions.apply(r[0], &r[1..], clock, |_| true)?;
    }
    Ok(sessions.finish(Instant::now()))
}

fn seeds() -> Vec<Vec<u8>> {
    record_seeds()
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
    let delay = due.saturating_duration_since(Instant::now());
    assert!(delay > Duration::from_secs(3) && delay <= Duration::from_secs(5));
    let left = st.expires_at.unwrap() - Instant::now();
    assert!(left > Duration::from_secs(58), "{left:?}");
}

#[test]
fn records_follow_the_session_through_its_life() {
    let s = seeds();
    // Subscribed, unsubscribed, then down with a delayed will.
    let got = replay(&[s[0].clone(), s[1].clone(), s[2].clone(), s[4].clone()]).unwrap();
    assert_eq!(got[0].subs.len(), 0);
    assert!(got[0].delayed_will.is_some() && got[0].will.is_none());
    let left = got[0].expires_at.unwrap() - Instant::now();
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

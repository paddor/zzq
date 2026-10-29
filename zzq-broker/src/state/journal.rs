//! Records of the lane journals: changes to the sessions that outlive
//! their connection, and to the QoS 1/2 messages they hold, in the order
//! their lane made them. Bodies use the fields of a state image:
//!
//! ```text
//! session   = client_id:str username:opt(str) version:u8 expiry:u32
//!             will:opt(delay:u32 msg)
//! down      = client_id:str expiry:u32 expires:time
//!             delayed_will:opt(due:time msg)
//! end       = client_id:str
//! sub       = client_id:str filter:str options:u8 sub_id:u32
//! unsub     = client_id:str filter:str
//! will_sent = client_id:str
//! clear     = (empty)
//! image     = connected:u8 session last_seq:u64 count:u32 seq:u64*
//! cleared   = (empty)
//! msg       = msg
//! queued    = client_id:str seq:u64 qos:u8 retain:u8 sub_ids:u32[4]
//! done      = client_id:str seq:u64
//! sent      = client_id:str seq:u64 pkid:u16
//! released  = client_id:str seq:u64
//! inbound   = client_id:str pkid:u16
//! ```
//!
//! `session` comes with each connection, `down` when it ends. A session
//! still connected when the journal ends lost its connection at the
//! restart. A message goes in once per lane: each `queued` after it hands
//! it to a session, which numbers its deliveries. `image` names the
//! numbers of the inflight deliveries, then the queued ones. A snapshot
//! is `clear`, an `image` per session, then `cleared`: it counts only
//! once `cleared` is there.

use zzq_proto::{QoS, SubscriptionOptions, Version};

use crate::index::SubIds;
use crate::msg::Publication;
use crate::session::Session;

use super::codec::{put_bytes, put_count, put_msg, put_session, put_u16, put_u32, put_u64};
use super::{Clock, SessionState};

mod replay;

pub(crate) use replay::Sessions;

/// A connection attached to a session.
pub(crate) const SESSION: u8 = 1;
/// A session's connection ended.
pub(crate) const DOWN: u8 = 2;
/// A session ended.
pub(crate) const END: u8 = 3;
pub(crate) const SUB: u8 = 4;
pub(crate) const UNSUB: u8 = 5;
/// A session's delayed will went out.
pub(crate) const WILL_SENT: u8 = 6;
/// Starts a snapshot: the images after it hold all the lane's sessions.
pub(crate) const CLEAR: u8 = 7;
/// A whole session.
pub(crate) const IMAGE: u8 = 8;
/// A message for the deliveries after it.
pub(crate) const MSG: u8 = 9;
/// A delivery of the last message to a session.
pub(crate) const QUEUED: u8 = 10;
/// A delivery is done: acked, or dropped.
pub(crate) const DONE: u8 = 11;
/// A QoS 2 delivery went out under a packet ID.
pub(crate) const SENT: u8 = 12;
/// The client has a QoS 2 delivery: PUBREC came.
pub(crate) const RELEASED: u8 = 13;
/// An inbound QoS 2 packet ID waits for PUBREL.
pub(crate) const INBOUND: u8 = 14;
/// PUBREL came for an inbound QoS 2 packet ID.
pub(crate) const INBOUND_DONE: u8 = 15;
/// Ends a snapshot, which takes the place of the sessions before it.
pub(crate) const CLEARED: u8 = 16;

/// The body of a [`SESSION`] record for `s`.
pub(crate) fn session(b: &mut Vec<u8>, s: &Session, clock: Clock) {
    put_bytes(b, s.client_id.as_bytes());
    match &s.username {
        Some(u) => {
            b.push(1);
            put_bytes(b, u.as_bytes());
        }
        None => b.push(0),
    }
    b.push(s.version.level());
    put_u32(b, s.expiry);
    match &s.will {
        Some(w) => {
            b.push(1);
            put_u32(b, w.delay);
            put_msg(b, &w.msg, clock);
        }
        None => b.push(0),
    }
}

/// The body of a [`DOWN`] record for `s`.
pub(crate) fn down(b: &mut Vec<u8>, s: &Session, clock: Clock) {
    put_bytes(b, s.client_id.as_bytes());
    put_u32(b, s.expiry);
    put_u64(b, clock.unix_ms(s.expires_at));
    match &s.delayed_will {
        Some(w) => {
            b.push(1);
            put_u64(b, clock.unix_ms(Some(w.due)));
            put_msg(b, &w.msg, clock);
        }
        None => b.push(0),
    }
}

/// The body of an [`END`] or [`WILL_SENT`] record.
pub(crate) fn client(b: &mut Vec<u8>, client_id: &str) {
    put_bytes(b, client_id.as_bytes());
}

/// The body of a [`SUB`] record.
pub(crate) fn sub(
    b: &mut Vec<u8>,
    client_id: &str,
    filter: &str,
    opts: SubscriptionOptions,
    sub_id: u32,
) {
    put_bytes(b, client_id.as_bytes());
    put_bytes(b, filter.as_bytes());
    b.push(opts.to_byte(Version::V5));
    put_u32(b, sub_id);
}

/// The body of an [`UNSUB`] record.
pub(crate) fn unsub(b: &mut Vec<u8>, client_id: &str, filter: &str) {
    put_bytes(b, client_id.as_bytes());
    put_bytes(b, filter.as_bytes());
}

/// The body of an [`IMAGE`] record for `s`.
pub(crate) fn image(b: &mut Vec<u8>, s: &Session, clock: Clock) {
    image_of(b, s, s.conn.is_some(), clock);
}

fn image_of(b: &mut Vec<u8>, s: &Session, connected: bool, clock: Clock) {
    b.push(u8::from(connected));
    put_session(b, s, clock);
    put_u64(b, s.flow.last_seq);
    put_count(b, s.inflight().len() + s.queue().len());
    for (_, k, _) in s.inflight() {
        put_u64(b, k.seq);
    }
    for k in s.queue() {
        put_u64(b, k.seq);
    }
}

/// The head of a [`MSG`] record for `p`. Its payload follows as it is.
pub(crate) fn msg_head(b: &mut Vec<u8>, p: &Publication<'_>, clock: Clock) {
    super::codec::publication_head(b, p, clock);
}

/// The body of a [`QUEUED`] record.
pub(crate) fn queued(
    b: &mut Vec<u8>,
    client_id: &str,
    seq: u64,
    (qos, retain, sub_id): (QoS, bool, SubIds),
) {
    put_bytes(b, client_id.as_bytes());
    put_u64(b, seq);
    b.push(qos as u8);
    b.push(u8::from(retain));
    for slot in sub_id.slots() {
        put_u32(b, slot);
    }
}

/// The body of a [`DONE`] or [`RELEASED`] record.
pub(crate) fn delivery(b: &mut Vec<u8>, client_id: &str, seq: u64) {
    put_bytes(b, client_id.as_bytes());
    put_u64(b, seq);
}

/// The body of a [`SENT`] record.
pub(crate) fn sent(b: &mut Vec<u8>, client_id: &str, seq: u64, pkid: u16) {
    delivery(b, client_id, seq);
    put_u16(b, pkid);
}

/// The body of an [`INBOUND`] or [`INBOUND_DONE`] record.
pub(crate) fn inbound(b: &mut Vec<u8>, client_id: &str, pkid: u16) {
    put_bytes(b, client_id.as_bytes());
    put_u16(b, pkid);
}

/// The body of an [`IMAGE`] record for a session as a journal left it,
/// with a connection or without.
pub(crate) fn image_state(b: &mut Vec<u8>, st: SessionState, connected: bool, clock: Clock) {
    let (mut s, will) = Session::restore(st, crate::budget::LaneTotal::default(), 1);
    s.delayed_will = will.map(|(due, msg)| crate::session::DelayedWill { timer: 0, due, msg });
    image_of(b, &s, connected, clock);
}

/// A message, for the wills of tests and seeds.
#[cfg(any(test, feature = "fuzzing"))]
fn will_msg() -> crate::msg::Msg {
    crate::msg::Msg::new("will/t", b"gone", &[], zzq_proto::QoS::AtLeastOnce, false)
}

/// Frames records of [`record_seeds`] for [`replay_records`].
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn framed(records: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in records {
        out.push(r[0]);
        let len = u16::try_from(r.len() - 1).unwrap_or(u16::MAX);
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&r[1..]);
    }
    out
}

/// A record of each kind for a session `c`, kind byte first.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn record_seeds() -> Vec<Vec<u8>> {
    use std::time::Duration;

    use crate::budget::LaneTotal;
    use crate::session::{DelayedWill, Will};

    let clock = Clock::now();
    let mut s = Session::new("c".into(), Version::V5, LaneTotal::default());
    s.expiry = 60;
    s.username = Some("u".into());
    s.will = Some(Will {
        msg: will_msg(),
        delay: 5,
    });
    let opts = SubscriptionOptions::from(zzq_proto::QoS::AtLeastOnce);
    s.subscribe("a/+", opts, 7, 1);
    let mut out = Vec::new();
    let mut add = |kind, f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = vec![kind];
        f(&mut b);
        out.push(b);
    };
    add(SESSION, &|b| session(b, &s, clock));
    add(SUB, &|b| sub(b, "c", "a/+", opts, 7));
    add(UNSUB, &|b| unsub(b, "c", "a/+"));
    add(IMAGE, &|b| image(b, &s, clock));
    s.will = None;
    s.expires_at = Some(crate::time::now() + Duration::from_secs(60));
    s.delayed_will = Some(DelayedWill {
        timer: 1,
        due: crate::time::now() + Duration::from_secs(5),
        msg: will_msg(),
    });
    add(DOWN, &|b| down(b, &s, clock));
    add(WILL_SENT, &|b| client(b, "c"));
    add(END, &|b| client(b, "c"));
    add(CLEAR, &|_| {});
    add(MSG, &|b| put_msg(b, &will_msg(), clock));
    let grant = (QoS::ExactlyOnce, false, SubIds::one(7));
    add(QUEUED, &|b| queued(b, "c", 1, grant));
    add(SENT, &|b| sent(b, "c", 1, 9));
    add(RELEASED, &|b| delivery(b, "c", 1));
    add(DONE, &|b| delivery(b, "c", 1));
    add(INBOUND, &|b| inbound(b, "c", 3));
    add(INBOUND_DONE, &|b| inbound(b, "c", 3));
    add(CLEARED, &|_| {});
    out
}

/// Replays `data` as records of one lane journal: each a kind byte, a
/// big endian `u16` length and that many body bytes. Returns the sessions
/// left.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn replay_records(data: &[u8]) -> Result<usize, &'static str> {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    let mut rest = data;
    while let [kind, a, b, tail @ ..] = rest {
        let len = usize::from(u16::from_be_bytes([*a, *b]));
        let (body, r) = tail.split_at_checked(len).ok_or("torn")?;
        sessions.apply(*kind, body, clock, |_| true)?;
        rest = r;
    }
    Ok(sessions.finish(crate::time::now()).len())
}

#[cfg(test)]
mod tests;

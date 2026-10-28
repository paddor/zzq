//! Fields of state records: integers, strings, messages and sessions,
//! big endian, as the module docs lay out.

use zzq_proto::topic::{validate_filter, validate_name};
use zzq_proto::{Properties, QoS, SubscriptionOptions, Version};

use crate::index::SubIds;
use crate::msg::{Msg, Publication};
use crate::session::{Kept, Queued, Session, Will};

use super::{Clock, Sent, SessionState, Sub};

/// Smallest encodings, which bound counts by the bytes left.
const MIN_MSG: usize = 4 + 1 + 1 + 8 + 4 + 4;
const MIN_ITEM: usize = 1 + 1 + 16 + MIN_MSG;
const MIN_SUB: usize = 4 + 1 + 4;

pub(super) fn put_u16(b: &mut Vec<u8>, v: u16) {
    b.extend_from_slice(&v.to_be_bytes());
}

pub(super) fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_be_bytes());
}

pub(super) fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_be_bytes());
}

pub(super) fn put_count(b: &mut Vec<u8>, n: usize) {
    put_u32(b, u32::try_from(n).unwrap_or(u32::MAX));
}

pub(super) fn put_bytes(b: &mut Vec<u8>, data: &[u8]) {
    put_count(b, data.len());
    b.extend_from_slice(data);
}

pub(super) fn put_session(b: &mut Vec<u8>, s: &Session, clock: Clock) {
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
    put_u64(b, clock.unix_ms(s.expires_at));
    match &s.will {
        Some(w) => {
            b.push(1);
            put_u32(b, w.delay);
            put_msg(b, &w.msg, clock);
        }
        None => b.push(0),
    }
    match &s.delayed_will {
        Some(w) => {
            b.push(1);
            put_u64(b, clock.unix_ms(Some(w.due)));
            put_msg(b, &w.msg, clock);
        }
        None => b.push(0),
    }
    put_u16(b, s.next_pkid());
    let mut subs: Vec<_> = s.subs.iter().collect();
    subs.sort_unstable_by(|a, b| a.0.cmp(b.0));
    put_count(b, subs.len());
    for (filter, (opts, sub_id)) in subs {
        put_bytes(b, filter.as_bytes());
        b.push(opts.to_byte(Version::V5));
        put_u32(b, *sub_id);
    }
    let mut rel: Vec<u16> = s.inbound.awaiting_rel.iter().copied().collect();
    rel.sort_unstable();
    put_count(b, rel.len());
    for pkid in rel {
        put_u16(b, pkid);
    }
    put_count(b, s.inflight().len());
    for (pkid, item, released) in s.inflight() {
        put_u16(b, pkid);
        b.push(u8::from(released));
        put_item(b, item, clock);
    }
    put_count(b, s.queue().len());
    for item in s.queue() {
        put_item(b, item, clock);
    }
}

pub(super) fn put_item(b: &mut Vec<u8>, q: &Kept, clock: Clock) {
    b.push(q.qos as u8);
    b.push(u8::from(q.retain));
    for slot in q.sub_id.slots() {
        put_u32(b, slot);
    }
    put_msg(b, &q.msg, clock);
}

pub(super) fn put_msg(b: &mut Vec<u8>, m: &Msg, clock: Clock) {
    msg_head(b, m, clock);
    b.extend_from_slice(m.payload());
}

/// Encodes `m` as an image does, up to its payload's bytes, which follow
/// as they are. For journals, which take a record in parts.
pub(crate) fn msg_head(b: &mut Vec<u8>, m: &Msg, clock: Clock) {
    put_bytes(b, m.topic().as_bytes());
    b.push(m.qos as u8);
    b.push(u8::from(m.retain));
    put_u64(b, clock.unix_ms(m.expires_at));
    put_bytes(b, m.props());
    put_count(b, m.payload().len());
}

/// [`msg_head`] for a publication, which is not a message yet.
pub(crate) fn publication_head(b: &mut Vec<u8>, p: &Publication<'_>, clock: Clock) {
    put_bytes(b, p.topic.as_bytes());
    b.push(p.qos as u8);
    b.push(u8::from(p.retain));
    let expires_at = p.expires_at.or_else(|| crate::msg::expiry(p.props));
    put_u64(b, clock.unix_ms(expires_at));
    put_bytes(b, p.props);
    put_count(b, p.payload.len());
}

/// Decodes a message [`msg_head`] and its payload encoded, and nothing
/// after it.
pub(crate) fn decode_msg(body: &[u8], clock: Clock) -> Result<Msg, &'static str> {
    let mut d = Decoder(body);
    let m = d.msg(clock)?;
    d.end()?;
    Ok(m)
}

/// Reads one record body.
pub(super) struct Decoder<'a>(pub(super) &'a [u8]);

impl<'a> Decoder<'a> {
    pub(super) fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let (head, rest) = self.0.split_at_checked(n).ok_or("record too short")?;
        self.0 = rest;
        Ok(head)
    }

    pub(super) fn u8(&mut self) -> Result<u8, &'static str> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn u16(&mut self) -> Result<u16, &'static str> {
        let b = self.take(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    pub(super) fn u32(&mut self) -> Result<u32, &'static str> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub(super) fn u64(&mut self) -> Result<u64, &'static str> {
        let mut v = [0; 8];
        v.copy_from_slice(self.take(8)?);
        Ok(u64::from_be_bytes(v))
    }

    pub(super) fn flag(&mut self) -> Result<bool, &'static str> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid flag"),
        }
    }

    /// A count of elements at least `min` bytes each.
    pub(super) fn count(&mut self, min: usize) -> Result<usize, &'static str> {
        let n = self.u32()? as usize;
        if n > self.0.len() / min {
            return Err("count beyond the record");
        }
        Ok(n)
    }

    pub(super) fn bytes(&mut self) -> Result<&'a [u8], &'static str> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    pub(super) fn str(&mut self) -> Result<&'a str, &'static str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| "invalid UTF-8")
    }

    pub(super) fn qos(&mut self) -> Result<QoS, &'static str> {
        QoS::try_from(self.u8()?).map_err(|_| "invalid QoS")
    }

    pub(super) const fn end(&self) -> Result<(), &'static str> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err("bytes after the record")
        }
    }

    pub(super) fn msg(&mut self, clock: Clock) -> Result<Msg, &'static str> {
        let topic = self.str()?;
        validate_name(topic).map_err(|_| "invalid topic")?;
        let qos = self.qos()?;
        let retain = self.flag()?;
        let expires_at = clock.instant(self.u64()?);
        let props = self.bytes()?;
        Properties::try_from(props).map_err(|_| "invalid properties")?;
        let payload = self.bytes()?;
        let mut m = Msg::new(topic, payload, props, qos, retain);
        m.expires_at = expires_at;
        Ok(m)
    }

    /// A QoS 1/2 message a session holds.
    pub(super) fn item(&mut self, clock: Clock) -> Result<Queued, &'static str> {
        let qos = self.qos()?;
        if qos == QoS::AtMostOnce {
            return Err("QoS 0 message held");
        }
        let retain = self.flag()?;
        let mut sub_id = SubIds::default();
        for _ in 0..4 {
            sub_id.add(self.u32()?);
        }
        Ok(Queued {
            msg: self.msg(clock)?,
            qos,
            retain,
            sub_id,
        })
    }

    pub(super) fn session(&mut self, clock: Clock) -> Result<SessionState, &'static str> {
        let client_id = self.str()?;
        if client_id.is_empty() {
            return Err("empty client ID");
        }
        let username = if self.flag()? {
            Some(self.str()?.into())
        } else {
            None
        };
        let version = match self.u8()? {
            3 => Version::V31,
            4 => Version::V311,
            5 => Version::V5,
            _ => return Err("invalid protocol level"),
        };
        let expiry = self.u32()?;
        let expires_at = clock.instant(self.u64()?);
        let will = if self.flag()? {
            let delay = self.u32()?;
            Some(Will {
                msg: self.msg(clock)?,
                delay,
            })
        } else {
            None
        };
        let delayed_will = if self.flag()? {
            let due = clock
                .instant(self.u64()?)
                .ok_or("delayed will without a time")?;
            Some((due, self.msg(clock)?))
        } else {
            None
        };
        let next_pkid = self.u16()?;
        let subs = self.subs()?;
        let awaiting_rel = self.awaiting_rel()?;
        let inflight = self.inflight(clock)?;
        let n = self.count(MIN_ITEM)?;
        let mut queue = Vec::with_capacity(n);
        for _ in 0..n {
            queue.push(self.item(clock)?);
        }
        Ok(SessionState {
            client_id: client_id.into(),
            username,
            version,
            expiry,
            expires_at,
            will,
            delayed_will,
            next_pkid,
            subs,
            awaiting_rel,
            inflight,
            queue,
            seqs: Vec::new(),
            last_seq: 0,
        })
    }

    pub(super) fn subs(&mut self) -> Result<Vec<Sub>, &'static str> {
        let n = self.count(MIN_SUB)?;
        let mut subs = Vec::with_capacity(n);
        for _ in 0..n {
            let filter = self.str()?;
            validate_filter(filter).map_err(|_| "invalid filter")?;
            let opts = SubscriptionOptions::from_byte(self.u8()?, Version::V5)
                .map_err(|_| "invalid subscription options")?;
            subs.push((filter.into(), opts, self.u32()?));
        }
        if subs.windows(2).any(|w| w[0].0 >= w[1].0) {
            return Err("subscriptions not sorted or repeated");
        }
        Ok(subs)
    }

    /// Inbound QoS 2 packet IDs, sorted and nonzero.
    pub(super) fn awaiting_rel(&mut self) -> Result<Vec<u16>, &'static str> {
        let n = self.count(2)?;
        let mut ids = Vec::with_capacity(n);
        for _ in 0..n {
            ids.push(self.u16()?);
        }
        if ids.first() == Some(&0) || ids.windows(2).any(|w| w[0] >= w[1]) {
            return Err("invalid inbound packet IDs");
        }
        Ok(ids)
    }

    /// The inflight window, each packet ID nonzero and used once.
    pub(super) fn inflight(&mut self, clock: Clock) -> Result<Vec<Sent>, &'static str> {
        let n = self.count(2 + 1 + MIN_ITEM)?;
        let mut inflight = Vec::with_capacity(n);
        let mut seen = vec![0u64; 1024];
        for _ in 0..n {
            let pkid = self.u16()?;
            let (word, bit) = (usize::from(pkid >> 6), 1u64 << (pkid & 63));
            if pkid == 0 || seen[word] & bit != 0 {
                return Err("invalid inflight packet ID");
            }
            seen[word] |= bit;
            let released = self.flag()?;
            let item = self.item(clock)?;
            if released && item.qos != QoS::ExactlyOnce {
                return Err("released QoS 1 message");
            }
            inflight.push((pkid, item, released));
        }
        Ok(inflight)
    }
}

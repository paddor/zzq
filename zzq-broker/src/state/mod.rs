//! Versioned encoding of broker state: sessions and retained messages.
//!
//! An image holds what a broker needs to restore them: for a dump at
//! shutdown, for sessions frozen in memory, and for replica recovery.
//! Format, big endian:
//!
//! ```text
//! image    = "ZZQSTATE" version:u16 record* end
//! record   = len:u64 xxh3_64(body):u64 body
//! body     = 1:u8 session | 2:u8 msg
//! end      = a record with body 0:u8
//! session  = client_id:str username:opt(str) version:u8 expiry:u32
//!            expires:time will:opt(delay:u32 msg) delayed_will:opt(time msg)
//!            next_pkid:u16 subs:u32 sub* awaiting_rel:u32 pkid:u16*
//!            inflight:u32 (pkid:u16 released:u8 item)* queue:u32 item*
//! sub      = filter:str options:u8 sub_id:u32
//! item     = qos:u8 retain:u8 sub_ids:u32[4] msg
//! msg      = topic:str qos:u8 retain:u8 expires:time props:bytes payload:bytes
//! time     = Unix milliseconds:u64, 0 for none
//! str      = bytes, UTF-8
//! bytes    = len:u32 data
//! opt(x)   = 0:u8 | 1:u8 x
//! ```
//!
//! Options are the v5 subscription options byte. Subscriptions and inbound
//! packet IDs are sorted, so equal state encodes to equal bytes. Decoding
//! checks counts against the input before it allocates, and refuses state
//! a session could not have.

use std::io::{self, Write};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use xxhash_rust::xxh3::xxh3_64;
use zzq_proto::{SubscriptionOptions, Version};

use crate::msg::Msg;
use crate::session::{Queued, Session, Will};

mod codec;
pub(crate) mod journal;

use codec::{Decoder, put_msg, put_session};
pub(crate) use codec::{decode_msg, msg_head};

const MAGIC: &[u8; 8] = b"ZZQSTATE";
/// Format version. A reader refuses any other.
pub(crate) const VERSION: u16 = 1;
const END: u8 = 0;
const SESSION: u8 = 1;
const RETAINED: u8 = 2;

/// Maps deadlines between `Instant` and Unix time from one reading of both
/// clocks, so a deadline survives a round trip to the millisecond.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Clock {
    now: Instant,
    wall_ms: u64,
}

impl Clock {
    pub(crate) fn now() -> Clock {
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Clock {
            now: Instant::now(),
            wall_ms: millis(wall),
        }
    }

    fn unix_ms(self, t: Option<Instant>) -> u64 {
        let Some(t) = t else {
            return 0;
        };
        let ms = if t >= self.now {
            self.wall_ms.saturating_add(millis(t - self.now))
        } else {
            self.wall_ms.saturating_sub(millis(self.now - t))
        };
        ms.max(1)
    }

    /// The deadline at Unix time `ms`. A past one becomes as long ago,
    /// or now if that is before this process could tell.
    fn instant(self, ms: u64) -> Option<Instant> {
        if ms == 0 {
            return None;
        }
        if ms >= self.wall_ms {
            self.now
                .checked_add(Duration::from_millis(ms - self.wall_ms))
        } else {
            let ago = Duration::from_millis(self.wall_ms - ms);
            Some(self.now.checked_sub(ago).unwrap_or(self.now))
        }
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A subscription: filter, options and Subscription Identifier.
pub(crate) type Sub = (Box<str>, SubscriptionOptions, u32);
/// A message in flight: packet ID, message, and whether PUBREC released it.
pub(crate) type Sent = (u16, Queued, bool);

/// A session as an image holds it, before a lane restores it.
#[derive(Debug)]
pub(crate) struct SessionState {
    pub(crate) client_id: Box<str>,
    pub(crate) username: Option<Box<str>>,
    pub(crate) version: Version,
    pub(crate) expiry: u32,
    pub(crate) expires_at: Option<Instant>,
    pub(crate) will: Option<Will>,
    /// When a delayed will is due, and the will.
    pub(crate) delayed_will: Option<(Instant, Msg)>,
    pub(crate) next_pkid: u16,
    pub(crate) subs: Vec<Sub>,
    pub(crate) awaiting_rel: Vec<u16>,
    pub(crate) inflight: Vec<Sent>,
    pub(crate) queue: Vec<Queued>,
    /// The journal numbers of the deliveries in `inflight`, then in
    /// `queue`; empty if a journal does not hold them.
    pub(crate) seqs: Vec<u64>,
    /// The last journal number the session gave a delivery.
    pub(crate) last_seq: u64,
}

/// One decoded record.
#[derive(Debug)]
pub(crate) enum Record {
    Session(Box<SessionState>),
    Retained(Msg),
}

/// Writes an image record by record.
#[derive(Debug)]
pub(crate) struct ImageWriter<W> {
    w: W,
    clock: Clock,
    body: Vec<u8>,
}

impl<W: Write> ImageWriter<W> {
    /// Writes the header.
    pub(crate) fn new(mut w: W, clock: Clock) -> io::Result<ImageWriter<W>> {
        w.write_all(MAGIC)?;
        w.write_all(&VERSION.to_be_bytes())?;
        Ok(ImageWriter::part(w, clock))
    }

    /// Writes records of an image another writer started and ends.
    pub(crate) const fn part(w: W, clock: Clock) -> ImageWriter<W> {
        ImageWriter {
            w,
            clock,
            body: Vec::new(),
        }
    }

    /// The bytes written so far, for a writer that moves them on.
    pub(crate) const fn get_mut(&mut self) -> &mut W {
        &mut self.w
    }

    pub(crate) fn session(&mut self, s: &Session) -> io::Result<()> {
        self.body.clear();
        self.body.push(SESSION);
        put_session(&mut self.body, s, self.clock);
        self.record()
    }

    pub(crate) fn retained(&mut self, m: &Msg) -> io::Result<()> {
        self.body.clear();
        self.body.push(RETAINED);
        put_msg(&mut self.body, m, self.clock);
        self.record()
    }

    /// Writes the end record, which tells a complete image from a torn
    /// one.
    pub(crate) fn end(&mut self) -> io::Result<()> {
        self.body.clear();
        self.body.push(END);
        self.record()
    }

    /// Writes the end record and returns the writer.
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn finish(mut self) -> io::Result<W> {
        self.end()?;
        Ok(self.w)
    }

    fn record(&mut self) -> io::Result<()> {
        self.w.write_all(&(self.body.len() as u64).to_be_bytes())?;
        self.w.write_all(&xxh3_64(&self.body).to_be_bytes())?;
        self.w.write_all(&self.body)
    }
}

/// Decodes an image from pieces of any size, such as a decompressor
/// yields. Fails on a wrong header, another version, a record that does
/// not check out, or a missing end, as after a torn write.
#[derive(Debug)]
pub(crate) struct ImageReader {
    clock: Clock,
    records: Vec<Record>,
    /// The start of a record whose bytes have not all come yet.
    buf: Vec<u8>,
    started: bool,
    ended: bool,
}

impl ImageReader {
    pub(crate) const fn new(clock: Clock) -> ImageReader {
        ImageReader {
            clock,
            records: Vec::new(),
            buf: Vec::new(),
            started: false,
            ended: false,
        }
    }

    /// Decodes the records `piece` completes.
    pub(crate) fn read(&mut self, piece: &[u8]) -> Result<(), &'static str> {
        if self.ended && !piece.is_empty() {
            return Err("bytes after the end");
        }
        self.buf.extend_from_slice(piece);
        let mut rest = &self.buf[..];
        if !self.started {
            let Some((head, r)) = rest.split_first_chunk::<10>() else {
                return Ok(());
            };
            let (magic, version) = head.split_at(MAGIC.len());
            if magic != MAGIC {
                return Err("not a state image");
            }
            if version != VERSION.to_be_bytes() {
                return Err("unsupported state version");
            }
            rest = r;
            self.started = true;
        }
        while let Some((len, r)) = rest.split_first_chunk::<8>()
            && let Some((sum, r)) = r.split_first_chunk::<8>()
        {
            let len = usize::try_from(u64::from_be_bytes(*len)).map_err(|_| "torn image")?;
            let Some((body, r)) = r.split_at_checked(len) else {
                break;
            };
            if xxh3_64(body) != u64::from_be_bytes(*sum) {
                return Err("record checksum mismatch");
            }
            rest = r;
            let mut d = Decoder(body);
            let record = match d.u8()? {
                END => None,
                SESSION => Some(Record::Session(Box::new(d.session(self.clock)?))),
                RETAINED => Some(Record::Retained(d.msg(self.clock)?)),
                _ => return Err("unknown record"),
            };
            d.end()?;
            let Some(record) = record else {
                if !rest.is_empty() {
                    return Err("bytes after the end");
                }
                self.ended = true;
                break;
            };
            self.records.push(record);
        }
        let used = self.buf.len() - rest.len();
        self.buf.drain(..used);
        Ok(())
    }

    /// The records, once the end has come.
    pub(crate) fn finish(self) -> Result<Vec<Record>, &'static str> {
        match (self.started, self.ended) {
            (_, true) => Ok(self.records),
            (true, false) => Err("torn image"),
            (false, false) => Err("not a state image"),
        }
    }
}

/// Decodes a whole image.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn read_image(data: &[u8], clock: Clock) -> Result<Vec<Record>, &'static str> {
    let mut image = ImageReader::new(clock);
    image.read(data)?;
    image.finish()
}

/// An image of one record with `body`, checksum and end included.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn framed(body: &[u8]) -> Vec<u8> {
    let mut out = MAGIC.to_vec();
    out.extend_from_slice(&VERSION.to_be_bytes());
    for b in [body, &[END]] {
        out.extend_from_slice(&(b.len() as u64).to_be_bytes());
        out.extend_from_slice(&xxh3_64(b).to_be_bytes());
        out.extend_from_slice(b);
    }
    out
}

#[cfg(test)]
mod tests {
    use zzq_proto::QoS;
    use zzq_proto::property::{self, Property};

    use super::codec::{put_bytes, put_count, put_u16, put_u32, put_u64};
    use super::*;
    use crate::budget::LaneTotal;
    use crate::index::SubIds;
    use crate::session::DelayedWill;

    fn msg(topic: &str, payload: &[u8], qos: QoS, expires_at: Option<Instant>) -> Msg {
        let mut props = Vec::new();
        property::encode_all(
            &[Property::Str(property::CONTENT_TYPE, "text/plain")],
            &mut props,
        );
        let mut m = Msg::new(topic, payload, &props, qos, false);
        m.expires_at = expires_at;
        m
    }

    fn item(n: u8, qos: QoS) -> Queued {
        Queued {
            msg: msg("a/b", &[n], qos, None),
            qos,
            retain: n.is_multiple_of(2),
            sub_id: SubIds::one(u32::from(n) + 1),
        }
    }

    fn state(clock: Clock) -> SessionState {
        let mut expiring = item(5, QoS::ExactlyOnce);
        expiring.msg.expires_at = clock.now.checked_sub(Duration::from_secs(1));
        SessionState {
            client_id: "dev-1".into(),
            username: Some("user".into()),
            version: Version::V5,
            expiry: 3600,
            expires_at: Some(clock.now + Duration::from_secs(90)),
            will: Some(Will {
                msg: msg("will", b"gone", QoS::AtLeastOnce, None),
                delay: 30,
            }),
            delayed_will: None,
            next_pkid: 7,
            subs: vec![
                (
                    "a/+".into(),
                    SubscriptionOptions {
                        qos: QoS::ExactlyOnce,
                        no_local: true,
                        retain_as_published: true,
                        retain_handling: zzq_proto::RetainHandling::Never,
                    },
                    9,
                ),
                ("$share/g/c/#".into(), QoS::AtLeastOnce.into(), 0),
            ],
            awaiting_rel: vec![65535, 3],
            // Wrapped: the oldest ID is the highest.
            inflight: vec![
                (65535, item(1, QoS::AtLeastOnce), false),
                (6, item(2, QoS::ExactlyOnce), true),
                (7, item(3, QoS::ExactlyOnce), false),
            ],
            queue: vec![item(4, QoS::AtLeastOnce), expiring],
            seqs: Vec::new(),
            last_seq: 0,
        }
    }

    /// An image of one restored session, with a delayed will put back, and
    /// one retained message.
    fn image(clock: Clock, st: SessionState) -> Vec<u8> {
        let (mut s, _) = Session::restore(st, LaneTotal::default(), 2);
        s.delayed_will = Some(DelayedWill {
            timer: 1,
            due: clock.now + Duration::from_secs(5),
            msg: msg("old/will", b"x", QoS::AtMostOnce, None),
        });
        let retained = msg(
            "r/1",
            b"on",
            QoS::AtLeastOnce,
            Some(clock.now + Duration::from_secs(60)),
        );
        let mut w = ImageWriter::new(Vec::new(), clock).unwrap();
        w.session(&s).unwrap();
        w.retained(&retained).unwrap();
        w.finish().unwrap()
    }

    fn session(records: Vec<Record>) -> SessionState {
        match records.into_iter().next() {
            Some(Record::Session(st)) => *st,
            other => panic!("expected a session, got {other:?}"),
        }
    }

    #[test]
    fn image_round_trips_to_the_same_bytes() {
        let clock = Clock::now();
        let first = image(clock, state(clock));
        let records = read_image(&first, clock).unwrap();
        assert_eq!(records.len(), 2);
        let Some(Record::Retained(r)) = records.get(1) else {
            panic!("expected a retained message");
        };
        assert_eq!((r.topic(), r.payload()), ("r/1", &b"on"[..]));
        let st = session(records);
        assert_eq!(st.subs[0].0.as_ref(), "$share/g/c/#");
        assert_eq!(st.awaiting_rel, [3, 65535]);
        let ids: Vec<(u16, bool)> = st.inflight.iter().map(|f| (f.0, f.2)).collect();
        assert_eq!(ids, [(65535, false), (6, true), (7, false)]);
        assert!(st.delayed_will.is_some());
        assert!(st.queue[1].msg.expires_at.is_some_and(|t| t < clock.now));
        assert_eq!(image(clock, st), first);
    }

    #[test]
    fn torn_or_flipped_images_are_refused() {
        let clock = Clock::now();
        let bytes = image(clock, state(clock));
        for n in 0..bytes.len() {
            assert!(read_image(&bytes[..n], clock).is_err(), "cut at {n}");
        }
        for i in 0..bytes.len() {
            let mut b = bytes.clone();
            b[i] ^= 0x10;
            assert!(read_image(&b, clock).is_err(), "flipped byte {i}");
        }
        let mut more = bytes;
        more.push(0);
        assert_eq!(read_image(&more, clock).err(), Some("bytes after the end"));
    }

    #[test]
    fn an_image_reads_in_pieces_of_any_size() {
        let clock = Clock::now();
        let bytes = image(clock, state(clock));
        let read = |pieces: &mut dyn Iterator<Item = &[u8]>| {
            let mut r = ImageReader::new(clock);
            for p in pieces {
                r.read(p)?;
            }
            r.finish().map(|records| records.len())
        };
        for cut in 0..=bytes.len() {
            let (a, b) = bytes.split_at(cut);
            assert_eq!(read(&mut [a, b].into_iter()), Ok(2), "cut at {cut}");
            assert!(read(&mut [a].into_iter()).is_err() || b.is_empty());
        }
        assert_eq!(read(&mut bytes.chunks(1)), Ok(2));
        assert_eq!(
            read(&mut [&bytes[..], &[0]].into_iter()),
            Err("bytes after the end")
        );
    }

    /// A session body: client ID "c", protocol level `level`, no
    /// subscriptions, and `inflight` as packet ID and QoS.
    fn session_body(level: u8, inflight: &[(u16, u8)], subs: u32) -> Vec<u8> {
        let mut b = vec![SESSION];
        put_bytes(&mut b, b"c");
        b.extend_from_slice(&[0, level]);
        put_u32(&mut b, 0);
        put_u64(&mut b, 0);
        b.extend_from_slice(&[0, 0]);
        put_u16(&mut b, 0);
        put_u32(&mut b, subs);
        put_u32(&mut b, 0);
        put_count(&mut b, inflight.len());
        for &(pkid, qos) in inflight {
            put_u16(&mut b, pkid);
            b.extend_from_slice(&[0, qos, 0]);
            b.extend_from_slice(&[0; 16]);
            put_bytes(&mut b, b"t");
            b.extend_from_slice(&[qos, 0]);
            put_u64(&mut b, 0);
            put_bytes(&mut b, b"");
            put_bytes(&mut b, b"x");
        }
        put_u32(&mut b, 0);
        b
    }

    #[test]
    fn checksummed_nonsense_is_refused() {
        let clock = Clock::now();
        let read = |body: &[u8]| read_image(&framed(body), clock).err();
        assert_eq!(read(&session_body(5, &[(1, 1), (2, 2)], 0)), None);
        assert_eq!(read(&[9]), Some("unknown record"));
        assert_eq!(
            read(&session_body(6, &[], 0)),
            Some("invalid protocol level")
        );
        assert_eq!(
            read(&session_body(5, &[(0, 1)], 0)),
            Some("invalid inflight packet ID")
        );
        assert_eq!(
            read(&session_body(5, &[(4, 1), (4, 2)], 0)),
            Some("invalid inflight packet ID")
        );
        assert_eq!(
            read(&session_body(5, &[(4, 0)], 0)),
            Some("QoS 0 message held")
        );
        assert_eq!(
            read(&session_body(5, &[], u32::MAX)),
            Some("count beyond the record")
        );
        let mut long = session_body(5, &[], 0);
        long.push(0);
        assert_eq!(read(&long), Some("bytes after the record"));
        let mut v2 = framed(&[END]);
        v2[MAGIC.len() + 1] = 2;
        assert_eq!(
            read_image(&v2, clock).err(),
            Some("unsupported state version")
        );
    }

    #[test]
    fn deadlines_survive_to_the_millisecond() {
        let clock = Clock::now();
        for ms in [
            clock.wall_ms - 1000,
            clock.wall_ms,
            clock.wall_ms + 1,
            clock.wall_ms + 1_000_000_000,
        ] {
            assert_eq!(clock.unix_ms(clock.instant(ms)), ms);
        }
        assert_eq!((clock.unix_ms(None), clock.instant(0)), (0, None));
    }
}

//! Owned messages and the publish view used for routing.

use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use zzq_proto::{QoS, Version};

/// A message that outlives the read buffer: queued, inflight, retained or
/// sent to another lane. Topic, properties and payload share one buffer,
/// usually a slice of the read buffer, so a clone is one refcount update.
#[derive(Debug, Clone)]
pub(crate) struct Msg {
    buf: Bytes,
    topic: Span,
    /// Raw v5 properties without Topic Alias and Subscription Identifier.
    props: Span,
    payload: Span,
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
    /// From the v5 Message Expiry Interval.
    pub(crate) expires_at: Option<Instant>,
}

/// Offset and length in `Msg::buf`.
#[derive(Debug, Clone, Copy)]
struct Span(u32, u32);

impl Span {
    fn get(self, buf: &[u8]) -> &[u8] {
        let (at, len) = (self.0 as usize, self.1 as usize);
        buf.get(at..at + len).unwrap_or_default()
    }
}

impl Msg {
    /// A message in one new allocation.
    pub(crate) fn new(topic: &str, payload: &[u8], props: &[u8], qos: QoS, retain: bool) -> Msg {
        let mut all = BytesMut::with_capacity(topic.len() + props.len() + payload.len());
        all.extend_from_slice(topic.as_bytes());
        all.extend_from_slice(props);
        all.extend_from_slice(payload);
        let (t, r) = (topic.len() as u32, props.len() as u32);
        Msg {
            buf: all.freeze(),
            topic: Span(0, t),
            props: Span(t, r),
            payload: Span(t + r, payload.len() as u32),
            qos,
            retain,
            expires_at: None,
        }
    }

    #[inline]
    #[expect(
        unsafe_code,
        reason = "topics are read per delivery; UTF-8 is checked on decode"
    )]
    pub(crate) fn topic(&self) -> &str {
        let b = self.topic.get(&self.buf);
        // SAFETY: the span holds a copy or a slice of a `&str` topic.
        unsafe { std::str::from_utf8_unchecked(b) }
    }

    #[inline]
    pub(crate) fn payload(&self) -> &[u8] {
        self.payload.get(&self.buf)
    }

    #[inline]
    pub(crate) fn props(&self) -> &[u8] {
        self.props.get(&self.buf)
    }

    /// The payload as its own handle on the buffer.
    pub(crate) fn payload_bytes(&self) -> Bytes {
        self.buf.slice_ref(self.payload())
    }

    /// The properties as their own handle on the buffer.
    pub(crate) fn props_bytes(&self) -> Bytes {
        self.buf.slice_ref(self.props())
    }

    /// A copy in one new allocation, so long-lived storage does not pin the
    /// read buffer the fields were sliced from.
    pub(crate) fn compact(&self) -> Msg {
        let mut m = Msg::new(
            self.topic(),
            self.payload(),
            self.props(),
            self.qos,
            self.retain,
        );
        m.expires_at = self.expires_at;
        m
    }

    /// Whether the message shares `chunk`.
    pub(crate) fn pins(&self, chunk: &Bytes) -> bool {
        !self.buf.is_empty() && within(chunk, &self.buf)
    }
}

/// Copies of messages a session holds long, so they do not pin the read
/// buffers they were sliced from. Copies share blocks, which grow while the
/// session keeps copying, so a busy session allocates per block, not per
/// message.
#[derive(Debug, Default)]
pub(crate) struct Arena {
    block: BytesMut,
    /// Size of the next block.
    next: usize,
}

/// First and largest arena block.
const ARENA_MIN: usize = 1 << 10;
const ARENA_MAX: usize = 64 << 10;

impl Arena {
    /// The `len` bytes `fill` appends, in the current block.
    pub(crate) fn put(&mut self, len: usize, fill: impl FnOnce(&mut BytesMut)) -> Bytes {
        if self.block.capacity() - self.block.len() < len {
            self.next = (self.next * 2).clamp(ARENA_MIN, ARENA_MAX);
            self.block = BytesMut::with_capacity(self.next.max(len));
        }
        fill(&mut self.block);
        self.block.split().freeze()
    }

    /// A copy of `msg` in the current block.
    pub(crate) fn copy(&mut self, msg: &Msg) -> Msg {
        Msg {
            buf: self.put(msg.buf.len(), |b| b.extend_from_slice(&msg.buf)),
            topic: msg.topic,
            props: msg.props,
            payload: msg.payload,
            qos: msg.qos,
            retain: msg.retain,
            expires_at: msg.expires_at,
        }
    }
}

/// Encodes into an arena block.
pub(crate) struct Block<'a>(pub(crate) &'a mut BytesMut);

impl zzq_proto::encode::Sink for Block<'_> {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.0.extend_from_slice(bytes);
    }
}

/// One incoming publish while it is routed.
#[derive(Debug)]
pub(crate) struct Publication<'a> {
    pub(crate) topic: &'a str,
    pub(crate) payload: &'a [u8],
    pub(crate) props: &'a [u8],
    pub(crate) qos: QoS,
    pub(crate) retain: bool,
    /// The publisher's whole packet, if subscribers on the same protocol
    /// family may get it byte for byte: QoS 0 and unchanged.
    pub(crate) raw: Option<(&'a [u8], Version)>,
    /// The publisher's whole packet, if another lane may route it as it
    /// is: no topic alias to resolve and no property to drop.
    pub(crate) packet: Option<(&'a [u8], Version)>,
    /// Buffer the borrowed fields point into, to make owned slices.
    pub(crate) chunk: Option<&'a Bytes>,
    /// Slices of `chunk` pin a read buffer, so a message held long gets a
    /// copy instead.
    pub(crate) pins: bool,
    /// Deadline of a stored message. `None` for a live publish, whose
    /// Message Expiry Interval in `props` starts now.
    pub(crate) expires_at: Option<Instant>,
}

impl Publication<'_> {
    /// A view of an owned message.
    pub(crate) fn of(msg: &Msg) -> Publication<'_> {
        Publication {
            topic: msg.topic(),
            payload: msg.payload(),
            props: msg.props(),
            qos: msg.qos,
            retain: msg.retain,
            raw: None,
            packet: None,
            chunk: None,
            pins: false,
            expires_at: msg.expires_at,
        }
    }

    /// Slices the publication out of its read buffer, or copies it.
    pub(crate) fn to_msg(&self) -> Msg {
        let parts = [self.topic.as_bytes(), self.props, self.payload];
        let expires_at = self.expires_at.or_else(|| expiry(self.props));
        if let Some(c) = self.chunk
            && parts.iter().all(|b| b.is_empty() || within(c, b))
        {
            // Offset in `c` of a part that lies inside it.
            let offset = |b: &[u8]| b.as_ptr() as usize - c.as_ptr() as usize;
            let live = parts.iter().filter(|b| !b.is_empty());
            let start = live.clone().map(|b| offset(b)).min();
            let end = live.map(|b| offset(b) + b.len()).max();
            if let (Some(start), Some(end)) = (start, end) {
                let at = |b: &[u8]| {
                    if b.is_empty() {
                        Span(0, 0)
                    } else {
                        Span((offset(b) - start) as u32, b.len() as u32)
                    }
                };
                return Msg {
                    buf: c.slice(start..end),
                    topic: at(parts[0]),
                    props: at(parts[1]),
                    payload: at(parts[2]),
                    qos: self.qos,
                    retain: self.retain,
                    expires_at,
                };
            }
        }
        let mut m = Msg::new(self.topic, self.payload, self.props, self.qos, self.retain);
        m.expires_at = expires_at;
        m
    }
}

/// Whether two versions share the PUBLISH encoding.
#[inline]
pub(crate) const fn same_family(a: Version, b: Version) -> bool {
    a.is_v5() == b.is_v5()
}

/// Whether `topic` has more than `max` levels. Shorter strings cannot, so
/// the common case is one comparison.
#[inline]
pub(crate) fn too_deep(topic: &str, max: usize) -> bool {
    topic.len() >= max && topic.bytes().filter(|&b| b == b'/').count() >= max
}

/// Whether `part` lies inside `whole`, so `Bytes::slice_ref` accepts it.
#[inline]
pub(crate) fn within(whole: &Bytes, part: &[u8]) -> bool {
    let start = whole.as_ptr() as usize;
    let p = part.as_ptr() as usize;
    p >= start && p + part.len() <= start + whole.len()
}

/// Deadline from a Message Expiry Interval in raw v5 properties.
pub(crate) fn expiry(props: &[u8]) -> Option<Instant> {
    if props.is_empty() {
        return None;
    }
    let p = zzq_proto::Properties::try_from(props).ok()?;
    let seconds = p.get_int(zzq_proto::property::MESSAGE_EXPIRY_INTERVAL)?;
    Some(Instant::now() + Duration::from_secs(u64::from(seconds)))
}

/// Whether a message with this deadline has expired.
#[inline]
pub(crate) fn expired(expires_at: Option<Instant>, now: Instant) -> bool {
    expires_at.is_some_and(|t| t <= now)
}

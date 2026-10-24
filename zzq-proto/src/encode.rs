//! Packet encoders.
//!
//! Every encoder knows the exact packet size before writing and writes into
//! a [`Sink`]: a `Vec<u8>` with `alloc`, or a [`SliceSink`] over a fixed
//! buffer.

use crate::property::{self, Property};
use crate::{PacketType, QoS, SubscriptionOptions, Version, varint};

/// Byte output for encoders.
pub trait Sink {
    /// Appends bytes.
    fn put(&mut self, bytes: &[u8]);

    /// Appends one byte.
    #[inline]
    fn put_u8(&mut self, b: u8) {
        self.put(&[b]);
    }

    /// Appends a big-endian `u16`.
    #[inline]
    fn put_u16(&mut self, v: u16) {
        self.put(&v.to_be_bytes());
    }

    /// Appends a variable byte integer.
    #[inline]
    fn put_varint(&mut self, v: usize) {
        let mut buf = [0; 4];
        let n = varint::encode(v, &mut buf);
        self.put(&buf[..n]);
    }

    /// Appends length-prefixed binary data.
    #[inline]
    fn put_binary(&mut self, b: &[u8]) {
        self.put_u16(b.len() as u16);
        self.put(b);
    }

    /// Appends a length-prefixed string.
    #[inline]
    fn put_str(&mut self, s: &str) {
        self.put_binary(s.as_bytes());
    }

    /// Appends a fixed header.
    #[inline]
    fn put_header(&mut self, byte1: u8, remaining: usize) {
        self.put_u8(byte1);
        self.put_varint(remaining);
    }
}

#[cfg(feature = "alloc")]
impl Sink for alloc::vec::Vec<u8> {
    #[inline]
    fn put(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    #[inline]
    fn put_u8(&mut self, b: u8) {
        self.push(b);
    }
}

/// A [`Sink`] over a fixed buffer. Writes past the end set `overflow`.
#[derive(Debug)]
pub struct SliceSink<'a> {
    buf: &'a mut [u8],
    len: usize,
    overflow: bool,
}

impl<'a> SliceSink<'a> {
    /// Wraps an empty buffer.
    pub const fn new(buf: &'a mut [u8]) -> Self {
        SliceSink {
            buf,
            len: 0,
            overflow: false,
        }
    }

    /// The written bytes, or `None` if a write did not fit.
    pub fn written(&self) -> Option<&[u8]> {
        if self.overflow {
            None
        } else {
            Some(&self.buf[..self.len])
        }
    }
}

impl Sink for SliceSink<'_> {
    fn put(&mut self, bytes: &[u8]) {
        let end = self.len + bytes.len();
        if self.overflow || end > self.buf.len() {
            self.overflow = true;
            return;
        }
        self.buf[self.len..end].copy_from_slice(bytes);
        self.len = end;
    }
}

/// Wire size of a property block with its length prefix.
#[inline]
const fn props_block(len: usize) -> usize {
    varint::len(len) + len
}

/// Wire size of a whole packet with `remaining` body bytes.
#[inline]
pub const fn packet_len(remaining: usize) -> usize {
    1 + varint::len(remaining) + remaining
}

/// Fields of an outgoing PUBLISH.
#[derive(Debug, Clone, Copy)]
pub struct PublishHeader<'a> {
    pub version: Version,
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a str,
    pub pkid: u16,
    /// Raw v5 property bytes, without the length prefix.
    pub properties: &'a [u8],
}

impl PublishHeader<'_> {
    /// First header byte.
    #[inline]
    pub const fn byte1(&self) -> u8 {
        let mut b = 0x30 | ((self.qos as u8) << 1);
        if self.dup {
            b |= 0x08;
        }
        if self.retain {
            b |= 0x01;
        }
        b
    }

    /// Remaining length for a payload of `payload_len` bytes.
    #[inline]
    pub fn remaining(&self, payload_len: usize) -> usize {
        let mut n = 2 + self.topic.len() + payload_len;
        if self.qos != QoS::AtMostOnce {
            n += 2;
        }
        if self.version.is_v5() {
            n += props_block(self.properties.len());
        }
        n
    }

    /// Writes everything but the payload.
    #[inline]
    pub fn encode<S: Sink + ?Sized>(&self, payload_len: usize, out: &mut S) {
        out.put_header(self.byte1(), self.remaining(payload_len));
        out.put_str(self.topic);
        if self.qos != QoS::AtMostOnce {
            out.put_u16(self.pkid);
        }
        if self.version.is_v5() {
            out.put_varint(self.properties.len());
            out.put(self.properties);
        }
    }
}

/// Writes a whole PUBLISH.
#[inline]
pub fn publish<S: Sink + ?Sized>(h: &PublishHeader<'_>, payload: &[u8], out: &mut S) {
    h.encode(payload.len(), out);
    out.put(payload);
}

/// Fields of PUBACK, PUBREC, PUBREL or PUBCOMP.
#[derive(Debug, Clone, Copy)]
pub struct AckFields<'a> {
    pub kind: PacketType,
    pub pkid: u16,
    pub reason: u8,
    /// Raw v5 property bytes, without the length prefix.
    pub properties: &'a [u8],
}

impl AckFields<'_> {
    /// A success ack without properties.
    #[inline]
    pub const fn new(kind: PacketType, pkid: u16) -> Self {
        AckFields {
            kind,
            pkid,
            reason: 0,
            properties: &[],
        }
    }
}

/// Writes an ack. v5 uses the short form for success without properties.
#[inline]
pub fn ack<S: Sink + ?Sized>(a: &AckFields<'_>, version: Version, out: &mut S) {
    let byte1 = a.kind.header_byte();
    if !version.is_v5() || (a.reason == 0 && a.properties.is_empty()) {
        out.put(&[byte1, 2]);
        out.put_u16(a.pkid);
        return;
    }
    if a.properties.is_empty() {
        out.put(&[byte1, 3]);
        out.put_u16(a.pkid);
        out.put_u8(a.reason);
        return;
    }
    out.put_header(byte1, 3 + props_block(a.properties.len()));
    out.put_u16(a.pkid);
    out.put_u8(a.reason);
    out.put_varint(a.properties.len());
    out.put(a.properties);
}

/// Writes CONNACK. For v3.1.1 `code` is the return code.
pub fn connack<S: Sink + ?Sized>(
    version: Version,
    session_present: bool,
    code: u8,
    properties: &[u8],
    out: &mut S,
) {
    let mut remaining = 2;
    if version.is_v5() {
        remaining += props_block(properties.len());
    }
    out.put_header(0x20, remaining);
    out.put_u8(u8::from(session_present));
    out.put_u8(code);
    if version.is_v5() {
        out.put_varint(properties.len());
        out.put(properties);
    }
}

/// Fields of SUBACK or UNSUBACK.
#[derive(Debug, Clone, Copy)]
pub struct SubAckFields<'a> {
    /// `SubAck` or `UnsubAck`.
    pub kind: PacketType,
    pub pkid: u16,
    /// Raw v5 property bytes, without the length prefix.
    pub properties: &'a [u8],
    pub codes: &'a [u8],
}

/// Writes SUBACK or UNSUBACK. A v3.1.1 UNSUBACK carries no codes.
pub fn suback<S: Sink + ?Sized>(a: &SubAckFields<'_>, version: Version, out: &mut S) {
    let v5 = version.is_v5();
    let codes = if !v5 && a.kind == PacketType::UnsubAck {
        &[][..]
    } else {
        a.codes
    };
    let mut remaining = 2 + codes.len();
    if v5 {
        remaining += props_block(a.properties.len());
    }
    out.put_header(a.kind.header_byte(), remaining);
    out.put_u16(a.pkid);
    if v5 {
        out.put_varint(a.properties.len());
        out.put(a.properties);
    }
    out.put(codes);
}

/// Writes PINGREQ.
pub fn pingreq<S: Sink + ?Sized>(out: &mut S) {
    out.put(&[0xc0, 0]);
}

/// Writes PINGRESP.
pub fn pingresp<S: Sink + ?Sized>(out: &mut S) {
    out.put(&[0xd0, 0]);
}

/// Writes DISCONNECT (`Disconnect`) or AUTH (`Auth`). v3.1.1 DISCONNECT has
/// no body.
pub fn reason_only<S: Sink + ?Sized>(
    kind: PacketType,
    version: Version,
    reason: u8,
    properties: &[u8],
    out: &mut S,
) {
    let byte1 = kind.header_byte();
    if !version.is_v5() || (reason == 0 && properties.is_empty()) {
        out.put(&[byte1, 0]);
    } else if properties.is_empty() {
        out.put(&[byte1, 1, reason]);
    } else {
        out.put_header(byte1, 1 + props_block(properties.len()));
        out.put_u8(reason);
        out.put_varint(properties.len());
        out.put(properties);
    }
}

/// Will message for [`ConnectFields`].
#[derive(Debug, Clone, Copy)]
pub struct WillFields<'a> {
    pub qos: QoS,
    pub retain: bool,
    pub properties: &'a [Property<'a>],
    pub topic: &'a str,
    pub payload: &'a [u8],
}

/// Fields of an outgoing CONNECT.
#[derive(Debug, Clone, Copy)]
pub struct ConnectFields<'a> {
    pub version: Version,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub properties: &'a [Property<'a>],
    pub client_id: &'a str,
    pub will: Option<WillFields<'a>>,
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
}

/// Writes CONNECT.
pub fn connect<S: Sink + ?Sized>(c: &ConnectFields<'_>, out: &mut S) {
    let v5 = c.version.is_v5();
    let name: &[u8] = if c.version == Version::V31 {
        b"MQIsdp"
    } else {
        b"MQTT"
    };
    let mut remaining = 2 + name.len() + 4 + 2 + c.client_id.len();
    if v5 {
        remaining += props_block(property::encoded_len(c.properties));
    }
    let mut flags = 0u8;
    if c.clean_start {
        flags |= 0x02;
    }
    if let Some(w) = &c.will {
        flags |= 0x04 | ((w.qos as u8) << 3);
        if w.retain {
            flags |= 0x20;
        }
        remaining += 4 + w.topic.len() + w.payload.len();
        if v5 {
            remaining += props_block(property::encoded_len(w.properties));
        }
    }
    if let Some(u) = c.username {
        flags |= 0x80;
        remaining += 2 + u.len();
    }
    if let Some(p) = c.password {
        flags |= 0x40;
        remaining += 2 + p.len();
    }
    out.put_header(0x10, remaining);
    out.put_binary(name);
    out.put_u8(c.version.level());
    out.put_u8(flags);
    out.put_u16(c.keep_alive);
    if v5 {
        put_props(c.properties, out);
    }
    out.put_str(c.client_id);
    if let Some(w) = &c.will {
        if v5 {
            put_props(w.properties, out);
        }
        out.put_str(w.topic);
        out.put_binary(w.payload);
    }
    if let Some(u) = c.username {
        out.put_str(u);
    }
    if let Some(p) = c.password {
        out.put_binary(p);
    }
}

/// Writes SUBSCRIBE.
pub fn subscribe<S: Sink + ?Sized>(
    version: Version,
    pkid: u16,
    properties: &[Property<'_>],
    filters: &[(&str, SubscriptionOptions)],
    out: &mut S,
) {
    let v5 = version.is_v5();
    let mut remaining = 2 + filters.iter().map(|(f, _)| 3 + f.len()).sum::<usize>();
    if v5 {
        remaining += props_block(property::encoded_len(properties));
    }
    out.put_header(PacketType::Subscribe.header_byte(), remaining);
    out.put_u16(pkid);
    if v5 {
        put_props(properties, out);
    }
    for (f, o) in filters {
        out.put_str(f);
        out.put_u8(o.to_byte(version));
    }
}

/// Writes UNSUBSCRIBE.
pub fn unsubscribe<S: Sink + ?Sized>(
    version: Version,
    pkid: u16,
    properties: &[Property<'_>],
    filters: &[&str],
    out: &mut S,
) {
    let v5 = version.is_v5();
    let mut remaining = 2 + filters.iter().map(|f| 2 + f.len()).sum::<usize>();
    if v5 {
        remaining += props_block(property::encoded_len(properties));
    }
    out.put_header(PacketType::Unsubscribe.header_byte(), remaining);
    out.put_u16(pkid);
    if v5 {
        put_props(properties, out);
    }
    for f in filters {
        out.put_str(f);
    }
}

/// Writes a v5 property block with its length prefix.
pub fn put_props<S: Sink + ?Sized>(props: &[Property<'_>], out: &mut S) {
    out.put_varint(property::encoded_len(props));
    property::encode_all(props, out);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::{Packet, decode};
    use crate::property::{RECEIVE_MAXIMUM, USER_PROPERTY};
    use std::vec::Vec;

    fn one(buf: &[u8], version: Version) -> Packet<'_> {
        let (p, n) = decode(buf, version, usize::MAX).unwrap().unwrap();
        assert_eq!(n, buf.len());
        p
    }

    #[test]
    fn publish_round_trip() {
        for version in [Version::V311, Version::V5] {
            for qos in [QoS::AtMostOnce, QoS::AtLeastOnce, QoS::ExactlyOnce] {
                let props = [Property::User("a", "b")];
                let mut raw = Vec::new();
                property::encode_all(&props, &mut raw);
                let h = PublishHeader {
                    version,
                    dup: qos != QoS::AtMostOnce,
                    qos,
                    retain: true,
                    topic: "a/b",
                    pkid: if qos == QoS::AtMostOnce { 0 } else { 7 },
                    properties: &raw,
                };
                let mut out = Vec::new();
                publish(&h, b"hello", &mut out);
                assert_eq!(out.len(), packet_len(h.remaining(5)));
                let Packet::Publish(p) = one(&out, version) else {
                    panic!("not a publish");
                };
                assert_eq!((p.topic, p.qos, p.retain, p.dup), ("a/b", qos, true, h.dup));
                assert_eq!(p.pkid, h.pkid);
                assert_eq!(p.payload, b"hello");
                let want: &[u8] = if version.is_v5() { &raw } else { &[] };
                assert_eq!(p.properties.raw(), want);
                assert_eq!(p.properties.get(USER_PROPERTY).is_some(), version.is_v5());
            }
        }
    }

    #[test]
    fn connect_round_trip() {
        let props = [Property::U16(RECEIVE_MAXIMUM, 5)];
        let c = ConnectFields {
            version: Version::V5,
            clean_start: true,
            keep_alive: 30,
            properties: &props,
            client_id: "c1",
            will: Some(WillFields {
                qos: QoS::AtLeastOnce,
                retain: true,
                properties: &[],
                topic: "w",
                payload: b"bye",
            }),
            username: Some("u"),
            password: Some(b"p"),
        };
        let mut out = Vec::new();
        connect(&c, &mut out);
        let Packet::Connect(p) = one(&out, Version::V311) else {
            panic!("not a connect");
        };
        assert_eq!(p.version, Version::V5);
        assert_eq!(p.client_id, "c1");
        assert_eq!(p.keep_alive, 30);
        assert_eq!(p.properties.get_int(RECEIVE_MAXIMUM), Some(5));
        let w = p.will.unwrap();
        assert_eq!(
            (w.topic, w.payload, w.qos, w.retain),
            ("w", &b"bye"[..], QoS::AtLeastOnce, true)
        );
        assert_eq!((p.username, p.password), (Some("u"), Some(&b"p"[..])));
    }

    #[test]
    fn subscribe_round_trip() {
        let opts = SubscriptionOptions {
            qos: QoS::ExactlyOnce,
            no_local: true,
            retain_as_published: true,
            retain_handling: 2,
        };
        for version in [Version::V311, Version::V5] {
            let mut out = Vec::new();
            subscribe(
                version,
                3,
                &[],
                &[("a/+", opts), ("b/#", SubscriptionOptions::default())],
                &mut out,
            );
            let Packet::Subscribe(s) = one(&out, version) else {
                panic!("not a subscribe");
            };
            let filters: Vec<_> = s.filters().collect();
            let want = if version.is_v5() {
                opts
            } else {
                SubscriptionOptions::qos(QoS::ExactlyOnce)
            };
            assert_eq!(
                filters,
                [("a/+", want), ("b/#", SubscriptionOptions::default())]
            );
        }
    }

    #[test]
    fn acks_and_small_packets() {
        let mut out = Vec::new();
        let a = AckFields {
            reason: 0x92,
            ..AckFields::new(PacketType::PubRel, 9)
        };
        ack(&a, Version::V5, &mut out);
        let Packet::PubRel(a) = one(&out, Version::V5) else {
            panic!("not a pubrel");
        };
        assert_eq!((a.pkid, a.reason), (9, 0x92));

        out.clear();
        let a = SubAckFields {
            kind: PacketType::SubAck,
            pkid: 4,
            properties: &[],
            codes: &[0, 1, 0x80],
        };
        suback(&a, Version::V5, &mut out);
        let Packet::SubAck(s) = one(&out, Version::V5) else {
            panic!("not a suback");
        };
        assert_eq!((s.pkid, s.codes), (4, &[0, 1, 0x80][..]));

        out.clear();
        connack(Version::V5, true, 0x86, &[], &mut out);
        let Packet::ConnAck(c) = one(&out, Version::V5) else {
            panic!("not a connack");
        };
        assert!(c.session_present);
        assert_eq!(c.code, 0x86);

        out.clear();
        reason_only(PacketType::Disconnect, Version::V5, 0x8e, &[], &mut out);
        let Packet::Disconnect(d) = one(&out, Version::V5) else {
            panic!("not a disconnect");
        };
        assert_eq!(d.reason, 0x8e);
    }

    #[test]
    fn incomplete_and_too_large() {
        let mut out = Vec::new();
        let h = PublishHeader {
            version: Version::V311,
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "t",
            pkid: 0,
            properties: &[],
        };
        publish(&h, &[0; 200], &mut out);
        for n in 0..out.len() {
            assert_eq!(decode(&out[..n], Version::V311, usize::MAX), Ok(None));
        }
        assert_eq!(
            decode(&out, Version::V311, 100),
            Err(crate::DecodeError::TooLarge)
        );
    }

    #[test]
    fn slice_sink_overflow() {
        let mut buf = [0u8; 3];
        let mut s = SliceSink::new(&mut buf);
        pingreq(&mut s);
        assert_eq!(s.written(), Some(&[0xc0, 0][..]));
        pingreq(&mut s);
        assert_eq!(s.written(), None);
    }
}

//! MQTT v5 properties.
//!
//! [`Properties`] wraps the raw property bytes of a packet. They are checked
//! once at decode time and then read lazily.

use crate::cursor::Cursor;
use crate::encode::Sink;
use crate::{DecodeError, varint};

/// Payload Format Indicator (`0x01`).
pub const PAYLOAD_FORMAT_INDICATOR: u8 = 0x01;
/// Message Expiry Interval (`0x02`).
pub const MESSAGE_EXPIRY_INTERVAL: u8 = 0x02;
/// Content Type (`0x03`).
pub const CONTENT_TYPE: u8 = 0x03;
/// Response Topic (`0x08`).
pub const RESPONSE_TOPIC: u8 = 0x08;
/// Correlation Data (`0x09`).
pub const CORRELATION_DATA: u8 = 0x09;
/// Subscription Identifier (`0x0b`).
pub const SUBSCRIPTION_IDENTIFIER: u8 = 0x0b;
/// Session Expiry Interval (`0x11`).
pub const SESSION_EXPIRY_INTERVAL: u8 = 0x11;
/// Assigned Client Identifier (`0x12`).
pub const ASSIGNED_CLIENT_IDENTIFIER: u8 = 0x12;
/// Server Keep Alive (`0x13`).
pub const SERVER_KEEP_ALIVE: u8 = 0x13;
/// Authentication Method (`0x15`).
pub const AUTHENTICATION_METHOD: u8 = 0x15;
/// Authentication Data (`0x16`).
pub const AUTHENTICATION_DATA: u8 = 0x16;
/// Request Problem Information (`0x17`).
pub const REQUEST_PROBLEM_INFORMATION: u8 = 0x17;
/// Will Delay Interval (`0x18`).
pub const WILL_DELAY_INTERVAL: u8 = 0x18;
/// Request Response Information (`0x19`).
pub const REQUEST_RESPONSE_INFORMATION: u8 = 0x19;
/// Response Information (`0x1a`).
pub const RESPONSE_INFORMATION: u8 = 0x1a;
/// Server Reference (`0x1c`).
pub const SERVER_REFERENCE: u8 = 0x1c;
/// Reason String (`0x1f`).
pub const REASON_STRING: u8 = 0x1f;
/// Receive Maximum (`0x21`).
pub const RECEIVE_MAXIMUM: u8 = 0x21;
/// Topic Alias Maximum (`0x22`).
pub const TOPIC_ALIAS_MAXIMUM: u8 = 0x22;
/// Topic Alias (`0x23`).
pub const TOPIC_ALIAS: u8 = 0x23;
/// Maximum QoS (`0x24`).
pub const MAXIMUM_QOS: u8 = 0x24;
/// Retain Available (`0x25`).
pub const RETAIN_AVAILABLE: u8 = 0x25;
/// User Property (`0x26`).
pub const USER_PROPERTY: u8 = 0x26;
/// Maximum Packet Size (`0x27`).
pub const MAXIMUM_PACKET_SIZE: u8 = 0x27;
/// Wildcard Subscription Available (`0x28`).
pub const WILDCARD_SUBSCRIPTION_AVAILABLE: u8 = 0x28;
/// Subscription Identifier Available (`0x29`).
pub const SUBSCRIPTION_IDENTIFIER_AVAILABLE: u8 = 0x29;
/// Shared Subscription Available (`0x2a`).
pub const SHARED_SUBSCRIPTION_AVAILABLE: u8 = 0x2a;

/// One decoded property.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Property<'a> {
    /// A one-byte property.
    Byte(u8, u8),
    /// A two-byte integer property.
    U16(u8, u16),
    /// A four-byte integer property.
    U32(u8, u32),
    /// A variable byte integer property (Subscription Identifier).
    VarInt(u8, u32),
    /// A UTF-8 string property.
    Str(u8, &'a str),
    /// A binary data property.
    Binary(u8, &'a [u8]),
    /// A User Property pair.
    User(&'a str, &'a str),
}

impl Property<'_> {
    /// The property identifier.
    pub const fn id(&self) -> u8 {
        match *self {
            Property::Byte(id, _)
            | Property::U16(id, _)
            | Property::U32(id, _)
            | Property::VarInt(id, _)
            | Property::Str(id, _)
            | Property::Binary(id, _) => id,
            Property::User(..) => USER_PROPERTY,
        }
    }

    /// Encoded size in bytes, identifier included.
    pub const fn len(&self) -> usize {
        1 + match *self {
            Property::Byte(..) => 1,
            Property::U16(..) => 2,
            Property::U32(..) => 4,
            Property::VarInt(_, v) => varint::len(v as usize),
            Property::Str(_, s) => 2 + s.len(),
            Property::Binary(_, b) => 2 + b.len(),
            Property::User(k, v) => 4 + k.len() + v.len(),
        }
    }

    /// Always false: a property has at least an identifier.
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// Writes the property.
    pub fn encode<S: Sink + ?Sized>(&self, out: &mut S) {
        out.put_u8(self.id());
        match *self {
            Property::Byte(_, v) => out.put_u8(v),
            Property::U16(_, v) => out.put(&v.to_be_bytes()),
            Property::U32(_, v) => out.put(&v.to_be_bytes()),
            Property::VarInt(_, v) => out.put_varint(v as usize),
            Property::Str(_, s) => out.put_str(s),
            Property::Binary(_, b) => out.put_binary(b),
            Property::User(k, v) => {
                out.put_str(k);
                out.put_str(v);
            }
        }
    }
}

/// Raw, validated v5 properties of one packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Properties<'a> {
    raw: &'a [u8],
}

impl<'a> TryFrom<&'a [u8]> for Properties<'a> {
    type Error = DecodeError;

    /// Checks that raw property bytes (without the length prefix) encode
    /// valid properties. Which of them a packet type allows is checked by
    /// the packet decoder.
    fn try_from(raw: &'a [u8]) -> Result<Self, DecodeError> {
        let mut c = Cursor::new(raw);
        while !c.is_empty() {
            read_one(&mut c)?;
        }
        Ok(Properties { raw })
    }
}

/// The properties one packet may carry, and those it may repeat.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Rules {
    allowed: u64,
    repeat: u64,
}

const fn bits(ids: &[u8]) -> u64 {
    let mut b = 0;
    let mut i = 0;
    while i < ids.len() {
        b |= 1 << ids[i];
        i += 1;
    }
    b
}

impl Rules {
    const fn new(allowed: &[u8], repeat: &[u8]) -> Rules {
        Rules {
            allowed: bits(allowed) | bits(&[USER_PROPERTY]),
            repeat: bits(repeat) | bits(&[USER_PROPERTY]),
        }
    }

    pub(crate) const CONNECT: Rules = Rules::new(
        &[
            SESSION_EXPIRY_INTERVAL,
            AUTHENTICATION_METHOD,
            AUTHENTICATION_DATA,
            REQUEST_PROBLEM_INFORMATION,
            REQUEST_RESPONSE_INFORMATION,
            RECEIVE_MAXIMUM,
            TOPIC_ALIAS_MAXIMUM,
            MAXIMUM_PACKET_SIZE,
        ],
        &[],
    );
    pub(crate) const WILL: Rules = Rules::new(
        &[
            PAYLOAD_FORMAT_INDICATOR,
            MESSAGE_EXPIRY_INTERVAL,
            CONTENT_TYPE,
            RESPONSE_TOPIC,
            CORRELATION_DATA,
            WILL_DELAY_INTERVAL,
        ],
        &[],
    );
    pub(crate) const CONNACK: Rules = Rules::new(
        &[
            SESSION_EXPIRY_INTERVAL,
            ASSIGNED_CLIENT_IDENTIFIER,
            SERVER_KEEP_ALIVE,
            AUTHENTICATION_METHOD,
            AUTHENTICATION_DATA,
            RESPONSE_INFORMATION,
            SERVER_REFERENCE,
            REASON_STRING,
            RECEIVE_MAXIMUM,
            TOPIC_ALIAS_MAXIMUM,
            MAXIMUM_QOS,
            RETAIN_AVAILABLE,
            MAXIMUM_PACKET_SIZE,
            WILDCARD_SUBSCRIPTION_AVAILABLE,
            SUBSCRIPTION_IDENTIFIER_AVAILABLE,
            SHARED_SUBSCRIPTION_AVAILABLE,
        ],
        &[],
    );
    /// A PUBLISH from the server carries one Subscription Identifier per
    /// matching subscription.
    pub(crate) const PUBLISH: Rules = Rules::new(
        &[
            PAYLOAD_FORMAT_INDICATOR,
            MESSAGE_EXPIRY_INTERVAL,
            CONTENT_TYPE,
            RESPONSE_TOPIC,
            CORRELATION_DATA,
            SUBSCRIPTION_IDENTIFIER,
            TOPIC_ALIAS,
        ],
        &[SUBSCRIPTION_IDENTIFIER],
    );
    /// PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK and UNSUBACK.
    pub(crate) const ACK: Rules = Rules::new(&[REASON_STRING], &[]);
    pub(crate) const SUBSCRIBE: Rules = Rules::new(&[SUBSCRIPTION_IDENTIFIER], &[]);
    pub(crate) const UNSUBSCRIBE: Rules = Rules::new(&[], &[]);
    pub(crate) const DISCONNECT: Rules = Rules::new(
        &[SESSION_EXPIRY_INTERVAL, REASON_STRING, SERVER_REFERENCE],
        &[],
    );
    pub(crate) const AUTH: Rules = Rules::new(
        &[AUTHENTICATION_METHOD, AUTHENTICATION_DATA, REASON_STRING],
        &[],
    );
}

impl<'a> Properties<'a> {
    /// Validates raw property bytes for a packet with `rules`: an identifier
    /// the packet does not allow is malformed; a repeat, an out-of-range
    /// value, a Response Topic with wildcards or Authentication Data
    /// without a method breaks a protocol rule.
    pub(crate) fn parse(raw: &'a [u8], rules: Rules) -> Result<Self, DecodeError> {
        let mut seen = 0u64;
        let mut c = Cursor::new(raw);
        while !c.is_empty() {
            let p = read_one(&mut c)?;
            let bit = 1u64 << p.id();
            if rules.allowed & bit == 0 {
                return Err(DecodeError::Malformed);
            }
            if seen & bit & !rules.repeat != 0 {
                return Err(DecodeError::Protocol);
            }
            seen |= bit;
            let valid = match p {
                // Every one-byte property is 0 or 1.
                Property::Byte(_, v) => v <= 1,
                Property::U16(RECEIVE_MAXIMUM, 0) | Property::U32(MAXIMUM_PACKET_SIZE, 0) => false,
                Property::Str(RESPONSE_TOPIC, s) => !s.contains(['+', '#']),
                _ => true,
            };
            if !valid {
                return Err(DecodeError::Protocol);
            }
        }
        if seen & bits(&[AUTHENTICATION_DATA, AUTHENTICATION_METHOD])
            == bits(&[AUTHENTICATION_DATA])
        {
            return Err(DecodeError::Protocol);
        }
        Ok(Properties { raw })
    }

    /// No properties.
    pub const EMPTY: Properties<'static> = Properties { raw: &[] };

    /// The raw property bytes, without the length prefix.
    #[inline]
    pub const fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// Whether there are no properties.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.raw.is_empty()
    }

    /// Iterates over the properties.
    pub const fn iter(&self) -> PropertyIter<'a> {
        PropertyIter {
            cursor: Cursor::new(self.raw),
        }
    }

    /// First property with identifier `id`.
    pub fn get(&self, id: u8) -> Option<Property<'a>> {
        if self.raw.is_empty() {
            return None;
        }
        self.iter().find(|p| p.id() == id)
    }

    /// An integer property, widened to `u32`.
    pub fn get_int(&self, id: u8) -> Option<u32> {
        match self.get(id)? {
            Property::Byte(_, v) => Some(u32::from(v)),
            Property::U16(_, v) => Some(u32::from(v)),
            Property::U32(_, v) | Property::VarInt(_, v) => Some(v),
            _ => None,
        }
    }

    /// A string property.
    pub fn get_str(&self, id: u8) -> Option<&'a str> {
        match self.get(id)? {
            Property::Str(_, s) => Some(s),
            _ => None,
        }
    }

    /// A binary property.
    pub fn get_binary(&self, id: u8) -> Option<&'a [u8]> {
        match self.get(id)? {
            Property::Binary(_, b) => Some(b),
            _ => None,
        }
    }
}

impl<'a> IntoIterator for &Properties<'a> {
    type Item = Property<'a>;
    type IntoIter = PropertyIter<'a>;

    fn into_iter(self) -> PropertyIter<'a> {
        self.iter()
    }
}

/// Iterator over validated properties.
#[derive(Debug)]
pub struct PropertyIter<'a> {
    cursor: Cursor<'a>,
}

impl<'a> Iterator for PropertyIter<'a> {
    type Item = Property<'a>;

    fn next(&mut self) -> Option<Property<'a>> {
        if self.cursor.is_empty() {
            return None;
        }
        read_one(&mut self.cursor).ok()
    }
}

fn read_one<'a>(c: &mut Cursor<'a>) -> Result<Property<'a>, DecodeError> {
    let id = c.u8()?;
    Ok(match id {
        PAYLOAD_FORMAT_INDICATOR
        | REQUEST_PROBLEM_INFORMATION
        | REQUEST_RESPONSE_INFORMATION
        | MAXIMUM_QOS
        | RETAIN_AVAILABLE
        | WILDCARD_SUBSCRIPTION_AVAILABLE
        | SUBSCRIPTION_IDENTIFIER_AVAILABLE
        | SHARED_SUBSCRIPTION_AVAILABLE => Property::Byte(id, c.u8()?),
        SERVER_KEEP_ALIVE | RECEIVE_MAXIMUM | TOPIC_ALIAS_MAXIMUM | TOPIC_ALIAS => {
            Property::U16(id, c.u16()?)
        }
        MESSAGE_EXPIRY_INTERVAL
        | SESSION_EXPIRY_INTERVAL
        | WILL_DELAY_INTERVAL
        | MAXIMUM_PACKET_SIZE => Property::U32(id, c.u32()?),
        SUBSCRIPTION_IDENTIFIER => {
            let v = c.varint()?;
            if v == 0 {
                return Err(DecodeError::Protocol);
            }
            Property::VarInt(id, v as u32)
        }
        CONTENT_TYPE
        | RESPONSE_TOPIC
        | ASSIGNED_CLIENT_IDENTIFIER
        | AUTHENTICATION_METHOD
        | RESPONSE_INFORMATION
        | SERVER_REFERENCE
        | REASON_STRING => Property::Str(id, c.string()?),
        CORRELATION_DATA | AUTHENTICATION_DATA => Property::Binary(id, c.binary()?),
        USER_PROPERTY => Property::User(c.string()?, c.string()?),
        _ => return Err(DecodeError::Malformed),
    })
}

/// Encoded size of `props`, without the length prefix.
pub fn encoded_len(props: &[Property<'_>]) -> usize {
    props.iter().map(Property::len).sum()
}

/// Writes `props` without the length prefix.
pub fn encode_all<S: Sink + ?Sized>(props: &[Property<'_>], out: &mut S) {
    for p in props {
        p.encode(out);
    }
}

// The tests encode into `Vec`, a `Sink` only with `alloc`.
#[cfg(test)]
#[cfg(feature = "alloc")]
mod tests {
    use super::*;
    use std::vec::Vec;

    #[test]
    fn typed_getters_and_iteration_agree() {
        let props = [
            Property::Binary(CORRELATION_DATA, b"id"),
            Property::U16(RECEIVE_MAXIMUM, 3),
        ];
        let mut out = Vec::new();
        encode_all(&props, &mut out);
        let parsed = Properties::try_from(out.as_slice()).unwrap();
        assert_eq!(parsed.get_binary(CORRELATION_DATA), Some(&b"id"[..]));
        assert_eq!(parsed.get_binary(RECEIVE_MAXIMUM), None);
        assert_eq!((&parsed).into_iter().count(), 2);
        assert!(props.iter().all(|p| !p.is_empty() && p.len() > 1));
    }

    #[test]
    fn encoded_properties_parse_back_unchanged() {
        let props = [
            Property::U32(SESSION_EXPIRY_INTERVAL, 60),
            Property::U16(RECEIVE_MAXIMUM, 10),
            Property::VarInt(SUBSCRIPTION_IDENTIFIER, 300),
            Property::Str(CONTENT_TYPE, "text"),
            Property::Binary(CORRELATION_DATA, b"\x00\x01"),
            Property::User("k", "v"),
        ];
        let mut out = Vec::new();
        encode_all(&props, &mut out);
        assert_eq!(out.len(), encoded_len(&props));
        let parsed = Properties::try_from(out.as_slice()).unwrap();
        let back: Vec<_> = parsed.iter().collect();
        assert_eq!(back, props);
        assert_eq!(parsed.get_int(RECEIVE_MAXIMUM), Some(10));
        assert_eq!(parsed.get_str(CONTENT_TYPE), Some("text"));
    }

    #[test]
    fn rejects_unknown_and_truncated() {
        assert_eq!(
            Properties::try_from(&[0x7f, 0][..]),
            Err(DecodeError::Malformed)
        );
        assert_eq!(
            Properties::try_from(&[RECEIVE_MAXIMUM, 0][..]),
            Err(DecodeError::Malformed)
        );
        assert_eq!(
            Properties::try_from(&[SUBSCRIPTION_IDENTIFIER, 0][..]),
            Err(DecodeError::Protocol)
        );
    }
}

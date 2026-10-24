use core::fmt;

use crate::reason;

/// Why a packet could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    /// Bytes do not form a valid packet.
    Malformed,
    /// Valid bytes that break a protocol rule.
    Protocol,
    /// The packet is larger than the allowed maximum.
    TooLarge,
    /// A string is not valid UTF-8 or contains U+0000.
    InvalidUtf8,
    /// A topic name or filter is invalid.
    InvalidTopic,
    /// The CONNECT protocol level is not supported.
    UnsupportedVersion(u8),
}

impl DecodeError {
    /// The MQTT v5 reason code to report this error with.
    pub fn reason_code(self) -> u8 {
        match self {
            DecodeError::Malformed | DecodeError::InvalidUtf8 => reason::MALFORMED_PACKET,
            DecodeError::Protocol => reason::PROTOCOL_ERROR,
            DecodeError::TooLarge => reason::PACKET_TOO_LARGE,
            DecodeError::InvalidTopic => reason::TOPIC_NAME_INVALID,
            DecodeError::UnsupportedVersion(_) => reason::UNSUPPORTED_PROTOCOL_VERSION,
        }
    }
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Malformed => f.write_str("malformed packet"),
            DecodeError::Protocol => f.write_str("protocol error"),
            DecodeError::TooLarge => f.write_str("packet too large"),
            DecodeError::InvalidUtf8 => f.write_str("invalid UTF-8 string"),
            DecodeError::InvalidTopic => f.write_str("invalid topic"),
            DecodeError::UnsupportedVersion(v) => write!(f, "unsupported protocol level {v}"),
        }
    }
}

impl core::error::Error for DecodeError {}

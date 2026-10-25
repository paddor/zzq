use core::fmt;

use crate::reason;

/// Why a packet could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
    pub const fn reason_code(self) -> u8 {
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

#[cfg(test)]
mod tests {
    use std::string::ToString;

    use super::*;

    #[test]
    fn every_error_has_a_message_and_a_reason_code() {
        let cases = [
            (
                DecodeError::Malformed,
                "malformed packet",
                reason::MALFORMED_PACKET,
            ),
            (
                DecodeError::Protocol,
                "protocol error",
                reason::PROTOCOL_ERROR,
            ),
            (
                DecodeError::TooLarge,
                "packet too large",
                reason::PACKET_TOO_LARGE,
            ),
            (
                DecodeError::InvalidUtf8,
                "invalid UTF-8 string",
                reason::MALFORMED_PACKET,
            ),
            (
                DecodeError::InvalidTopic,
                "invalid topic",
                reason::TOPIC_NAME_INVALID,
            ),
            (
                DecodeError::UnsupportedVersion(7),
                "unsupported protocol level 7",
                reason::UNSUPPORTED_PROTOCOL_VERSION,
            ),
        ];
        for (e, message, code) in cases {
            assert_eq!((e.to_string().as_str(), e.reason_code()), (message, code));
        }
    }
}

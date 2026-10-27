use crate::reason;

/// Why a packet could not be decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// Bytes do not form a valid packet.
    #[error("malformed packet")]
    Malformed,
    /// Valid bytes that break a protocol rule.
    #[error("protocol error")]
    Protocol,
    /// The packet is larger than the allowed maximum.
    #[error("packet too large")]
    TooLarge,
    /// A string is not valid UTF-8 or contains U+0000.
    #[error("invalid UTF-8 string")]
    InvalidUtf8,
    /// A topic name is invalid.
    #[error("invalid topic")]
    InvalidTopic,
    /// A topic filter is invalid.
    #[error("invalid topic filter")]
    InvalidFilter,
    /// The CONNECT protocol level is not supported.
    #[error("unsupported protocol level {0}")]
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
            DecodeError::InvalidFilter => reason::TOPIC_FILTER_INVALID,
            DecodeError::UnsupportedVersion(_) => reason::UNSUPPORTED_PROTOCOL_VERSION,
        }
    }
}

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
                DecodeError::InvalidFilter,
                "invalid topic filter",
                reason::TOPIC_FILTER_INVALID,
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

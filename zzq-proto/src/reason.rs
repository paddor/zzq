//! MQTT v5 reason codes, plus v3.1.1 CONNACK return codes.
//!
//! The constants are the raw bytes the codec reads and writes.
//! [`ReasonCode`] carries one through APIs that hand codes to applications,
//! with the same names as associated constants.

use core::fmt;

/// An MQTT v5 reason code. Codes from 0x80 up are errors.
///
/// # Examples
///
/// ```
/// use zzq_proto::ReasonCode;
///
/// let code = ReasonCode::from(0x87);
/// assert_eq!(code, ReasonCode::NOT_AUTHORIZED);
/// assert!(code.is_error());
/// assert_eq!(code.to_string(), "not authorized (0x87)");
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReasonCode(u8);

macro_rules! reason_codes {
    ($($name:ident = $value:literal => $text:literal,)*) => {
        $(
            #[doc = concat!("`", stringify!($value), "`: ", $text, ".")]
            pub const $name: u8 = $value;
        )*

        impl ReasonCode {
            $(
                #[doc = concat!("`", stringify!($value), "`: ", $text, ".")]
                pub const $name: ReasonCode = ReasonCode($value);
            )*
        }

        /// Spec name of each code. The first entry wins for shared values.
        const NAMES: &[(u8, &str)] = &[$(($value, $text)),*];
    };
}

reason_codes! {
    SUCCESS = 0x00 => "success",
    GRANTED_QOS_1 = 0x01 => "granted QoS 1",
    GRANTED_QOS_2 = 0x02 => "granted QoS 2",
    DISCONNECT_WITH_WILL = 0x04 => "disconnect with will message",
    NO_MATCHING_SUBSCRIBERS = 0x10 => "no matching subscribers",
    NO_SUBSCRIPTION_EXISTED = 0x11 => "no subscription existed",
    CONTINUE_AUTHENTICATION = 0x18 => "continue authentication",
    RE_AUTHENTICATE = 0x19 => "re-authenticate",
    UNSPECIFIED_ERROR = 0x80 => "unspecified error",
    MALFORMED_PACKET = 0x81 => "malformed packet",
    PROTOCOL_ERROR = 0x82 => "protocol error",
    IMPLEMENTATION_SPECIFIC_ERROR = 0x83 => "implementation specific error",
    UNSUPPORTED_PROTOCOL_VERSION = 0x84 => "unsupported protocol version",
    CLIENT_IDENTIFIER_NOT_VALID = 0x85 => "client identifier not valid",
    BAD_USER_NAME_OR_PASSWORD = 0x86 => "bad user name or password",
    NOT_AUTHORIZED = 0x87 => "not authorized",
    SERVER_UNAVAILABLE = 0x88 => "server unavailable",
    SERVER_BUSY = 0x89 => "server busy",
    BANNED = 0x8a => "banned",
    SERVER_SHUTTING_DOWN = 0x8b => "server shutting down",
    BAD_AUTHENTICATION_METHOD = 0x8c => "bad authentication method",
    KEEP_ALIVE_TIMEOUT = 0x8d => "keep alive timeout",
    SESSION_TAKEN_OVER = 0x8e => "session taken over",
    TOPIC_FILTER_INVALID = 0x8f => "topic filter invalid",
    TOPIC_NAME_INVALID = 0x90 => "topic name invalid",
    PACKET_IDENTIFIER_IN_USE = 0x91 => "packet identifier in use",
    PACKET_IDENTIFIER_NOT_FOUND = 0x92 => "packet identifier not found",
    RECEIVE_MAXIMUM_EXCEEDED = 0x93 => "receive maximum exceeded",
    TOPIC_ALIAS_INVALID = 0x94 => "topic alias invalid",
    PACKET_TOO_LARGE = 0x95 => "packet too large",
    MESSAGE_RATE_TOO_HIGH = 0x96 => "message rate too high",
    QUOTA_EXCEEDED = 0x97 => "quota exceeded",
    ADMINISTRATIVE_ACTION = 0x98 => "administrative action",
    PAYLOAD_FORMAT_INVALID = 0x99 => "payload format invalid",
    RETAIN_NOT_SUPPORTED = 0x9a => "retain not supported",
    QOS_NOT_SUPPORTED = 0x9b => "QoS not supported",
    USE_ANOTHER_SERVER = 0x9c => "use another server",
    SERVER_MOVED = 0x9d => "server moved",
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 0x9e => "shared subscriptions not supported",
    CONNECTION_RATE_EXCEEDED = 0x9f => "connection rate exceeded",
    MAXIMUM_CONNECT_TIME = 0xa0 => "maximum connect time",
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 0xa1 => "subscription identifiers not supported",
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 0xa2 => "wildcard subscriptions not supported",
}

impl ReasonCode {
    /// Whether the code reports a failure.
    pub const fn is_error(self) -> bool {
        self.0 >= 0x80
    }

    /// The spec name, if the code is known. Shared values, such as `0x00`
    /// for success, normal disconnection and granted QoS 0, get one name.
    pub fn name(self) -> Option<&'static str> {
        NAMES.iter().find(|(c, _)| *c == self.0).map(|(_, n)| *n)
    }
}

impl From<u8> for ReasonCode {
    fn from(code: u8) -> Self {
        ReasonCode(code)
    }
}

impl From<ReasonCode> for u8 {
    fn from(code: ReasonCode) -> Self {
        code.0
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => write!(f, "{name} ({:#04x})", self.0),
            None => write!(f, "reason code {:#04x}", self.0),
        }
    }
}

impl fmt::Debug for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ReasonCode({:#04x})", self.0)
    }
}

/// v3.1.1 CONNACK return codes.
pub mod v311 {
    /// Connection accepted.
    pub const ACCEPTED: u8 = 0;
    /// The server does not support the requested protocol level.
    pub const UNACCEPTABLE_PROTOCOL_VERSION: u8 = 1;
    /// The client identifier is not allowed.
    pub const IDENTIFIER_REJECTED: u8 = 2;
    /// The MQTT service is unavailable.
    pub const SERVER_UNAVAILABLE: u8 = 3;
    /// The user name or password is malformed or wrong.
    pub const BAD_USER_NAME_OR_PASSWORD: u8 = 4;
    /// The client may not connect.
    pub const NOT_AUTHORIZED: u8 = 5;

    /// Maps a v5 CONNACK reason code to the closest v3.1.1 return code.
    pub const fn from_v5(code: u8) -> u8 {
        match code {
            super::SUCCESS => ACCEPTED,
            super::UNSUPPORTED_PROTOCOL_VERSION => UNACCEPTABLE_PROTOCOL_VERSION,
            super::CLIENT_IDENTIFIER_NOT_VALID => IDENTIFIER_REJECTED,
            super::BAD_USER_NAME_OR_PASSWORD => BAD_USER_NAME_OR_PASSWORD,
            super::NOT_AUTHORIZED => NOT_AUTHORIZED,
            _ => SERVER_UNAVAILABLE,
        }
    }

    /// Maps a v3.1.1 return code to the v5 reason code with the same
    /// meaning. Unknown codes become unspecified error.
    pub const fn to_v5(code: u8) -> u8 {
        match code {
            ACCEPTED => super::SUCCESS,
            UNACCEPTABLE_PROTOCOL_VERSION => super::UNSUPPORTED_PROTOCOL_VERSION,
            IDENTIFIER_REJECTED => super::CLIENT_IDENTIFIER_NOT_VALID,
            SERVER_UNAVAILABLE => super::SERVER_UNAVAILABLE,
            BAD_USER_NAME_OR_PASSWORD => super::BAD_USER_NAME_OR_PASSWORD,
            NOT_AUTHORIZED => super::NOT_AUTHORIZED,
            _ => super::UNSPECIFIED_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::format;

    use super::*;

    #[test]
    fn codes_convert_and_print_their_spec_name() {
        assert_eq!(ReasonCode::from(NOT_AUTHORIZED), ReasonCode::NOT_AUTHORIZED);
        assert_eq!(u8::from(ReasonCode::QUOTA_EXCEEDED), 0x97);
        assert!(!ReasonCode::SUCCESS.is_error());
        assert!(ReasonCode::UNSPECIFIED_ERROR.is_error());
        let shown = format!("{} {}", ReasonCode::SUCCESS, ReasonCode::from(0x03));
        assert_eq!(shown, "success (0x00) reason code 0x03");
        assert_eq!(format!("{:?}", ReasonCode::BANNED), "ReasonCode(0x8a)");
    }

    #[test]
    fn v311_return_codes_map_both_ways() {
        for v3 in 0..=5 {
            assert_eq!(v311::from_v5(v311::to_v5(v3)), v3);
        }
        assert_eq!(v311::to_v5(9), UNSPECIFIED_ERROR);
        assert_eq!(v311::from_v5(QUOTA_EXCEEDED), v311::SERVER_UNAVAILABLE);
    }
}

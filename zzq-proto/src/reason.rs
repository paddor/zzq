//! MQTT v5 reason codes, plus v3.1.1 CONNACK return codes.

pub const SUCCESS: u8 = 0x00;
pub const GRANTED_QOS_1: u8 = 0x01;
pub const GRANTED_QOS_2: u8 = 0x02;
pub const DISCONNECT_WITH_WILL: u8 = 0x04;
pub const NO_MATCHING_SUBSCRIBERS: u8 = 0x10;
pub const NO_SUBSCRIPTION_EXISTED: u8 = 0x11;
pub const UNSPECIFIED_ERROR: u8 = 0x80;
pub const MALFORMED_PACKET: u8 = 0x81;
pub const PROTOCOL_ERROR: u8 = 0x82;
pub const IMPLEMENTATION_SPECIFIC_ERROR: u8 = 0x83;
pub const UNSUPPORTED_PROTOCOL_VERSION: u8 = 0x84;
pub const CLIENT_IDENTIFIER_NOT_VALID: u8 = 0x85;
pub const BAD_USER_NAME_OR_PASSWORD: u8 = 0x86;
pub const NOT_AUTHORIZED: u8 = 0x87;
pub const SERVER_UNAVAILABLE: u8 = 0x88;
pub const SERVER_BUSY: u8 = 0x89;
pub const SERVER_SHUTTING_DOWN: u8 = 0x8b;
pub const KEEP_ALIVE_TIMEOUT: u8 = 0x8d;
pub const SESSION_TAKEN_OVER: u8 = 0x8e;
pub const TOPIC_FILTER_INVALID: u8 = 0x8f;
pub const TOPIC_NAME_INVALID: u8 = 0x90;
pub const PACKET_IDENTIFIER_IN_USE: u8 = 0x91;
pub const PACKET_IDENTIFIER_NOT_FOUND: u8 = 0x92;
pub const RECEIVE_MAXIMUM_EXCEEDED: u8 = 0x93;
pub const TOPIC_ALIAS_INVALID: u8 = 0x94;
pub const PACKET_TOO_LARGE: u8 = 0x95;
pub const QUOTA_EXCEEDED: u8 = 0x97;
pub const PAYLOAD_FORMAT_INVALID: u8 = 0x99;
pub const RETAIN_NOT_SUPPORTED: u8 = 0x9a;
pub const QOS_NOT_SUPPORTED: u8 = 0x9b;
pub const SHARED_SUBSCRIPTIONS_NOT_SUPPORTED: u8 = 0x9e;
pub const WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED: u8 = 0xa2;

/// v3.1.1 CONNACK return codes.
pub mod v311 {
    pub const ACCEPTED: u8 = 0;
    pub const UNACCEPTABLE_PROTOCOL_VERSION: u8 = 1;
    pub const IDENTIFIER_REJECTED: u8 = 2;
    pub const SERVER_UNAVAILABLE: u8 = 3;
    pub const BAD_USER_NAME_OR_PASSWORD: u8 = 4;
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
}

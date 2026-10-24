use crate::DecodeError;

/// Quality of service level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(u8)]
pub enum QoS {
    /// At most once.
    #[default]
    AtMostOnce = 0,
    /// At least once.
    AtLeastOnce = 1,
    /// Exactly once.
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = DecodeError;

    /// Converts a wire value.
    #[inline]
    fn try_from(v: u8) -> Result<QoS, DecodeError> {
        match v {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => Err(DecodeError::Malformed),
        }
    }
}

/// Protocol version negotiated by CONNECT.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Version {
    /// MQTT 3.1 (protocol name `MQIsdp`), handled like 3.1.1.
    V31,
    /// MQTT 3.1.1, protocol level 4.
    V311,
    /// MQTT 5.0, protocol level 5.
    V5,
}

impl Version {
    /// Whether this version carries properties and reason codes.
    #[inline]
    pub const fn is_v5(self) -> bool {
        matches!(self, Version::V5)
    }

    /// The CONNECT protocol level.
    pub const fn level(self) -> u8 {
        match self {
            Version::V31 => 3,
            Version::V311 => 4,
            Version::V5 => 5,
        }
    }
}

/// Control packet type, the high nibble of the first header byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    /// CONNECT
    Connect = 1,
    /// CONNACK
    ConnAck = 2,
    /// PUBLISH
    Publish = 3,
    /// PUBACK
    PubAck = 4,
    /// PUBREC
    PubRec = 5,
    /// PUBREL
    PubRel = 6,
    /// PUBCOMP
    PubComp = 7,
    /// SUBSCRIBE
    Subscribe = 8,
    /// SUBACK
    SubAck = 9,
    /// UNSUBSCRIBE
    Unsubscribe = 10,
    /// UNSUBACK
    UnsubAck = 11,
    /// PINGREQ
    PingReq = 12,
    /// PINGRESP
    PingResp = 13,
    /// DISCONNECT
    Disconnect = 14,
    /// AUTH (v5 only)
    Auth = 15,
}

impl PacketType {
    /// First header byte for this type with its mandatory flags.
    pub const fn header_byte(self) -> u8 {
        let flags = match self {
            PacketType::PubRel | PacketType::Subscribe | PacketType::Unsubscribe => 0x02,
            _ => 0,
        };
        ((self as u8) << 4) | flags
    }
}

/// Per-filter options of a SUBSCRIBE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SubscriptionOptions {
    /// Maximum QoS granted for this filter.
    pub qos: QoS,
    /// v5: do not forward messages published by this client.
    pub no_local: bool,
    /// v5: keep the publisher's retain flag on forwarded messages.
    pub retain_as_published: bool,
    /// v5: 0 send retained on subscribe, 1 only if new, 2 never.
    pub retain_handling: u8,
}

impl From<QoS> for SubscriptionOptions {
    /// Options with only a QoS.
    fn from(qos: QoS) -> Self {
        SubscriptionOptions {
            qos,
            ..Self::default()
        }
    }
}

impl SubscriptionOptions {
    /// Parses the options byte.
    ///
    /// # Errors
    ///
    /// [`DecodeError::Malformed`] for reserved bits, an invalid QoS or retain
    /// handling, or v5 bits in a v3.1.1 byte.
    pub fn from_byte(b: u8, version: Version) -> Result<Self, DecodeError> {
        if version.is_v5() {
            if b & 0xc0 != 0 || (b >> 4) & 0x03 == 3 {
                return Err(DecodeError::Malformed);
            }
            Ok(SubscriptionOptions {
                qos: QoS::try_from(b & 0x03)?,
                no_local: b & 0x04 != 0,
                retain_as_published: b & 0x08 != 0,
                retain_handling: (b >> 4) & 0x03,
            })
        } else {
            if b & 0xfc != 0 {
                return Err(DecodeError::Malformed);
            }
            Ok(SubscriptionOptions::from(QoS::try_from(b)?))
        }
    }

    /// Encodes the options byte.
    pub const fn to_byte(self, version: Version) -> u8 {
        let mut b = self.qos as u8;
        if version.is_v5() {
            if self.no_local {
                b |= 0x04;
            }
            if self.retain_as_published {
                b |= 0x08;
            }
            b |= (self.retain_handling & 0x03) << 4;
        }
        b
    }
}

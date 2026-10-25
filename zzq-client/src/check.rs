//! Input checks before anything is encoded. MQTT length prefixes hold
//! 65535 bytes per field and 268435455 per packet, and the encoders assume
//! valid input.

use zzq_proto::encode::{MAX_FIELD_LEN, PublishHeader};
use zzq_proto::topic::{validate_filter, validate_name};
use zzq_proto::{QoS, Version, varint};

use crate::{ClientOptions, Error, Result};

/// A PUBLISH to `topic` with a payload of `payload_len` bytes.
pub(crate) fn publish(topic: &str, payload_len: usize) -> Result<()> {
    validate_name(topic).map_err(|_| Error::InvalidTopic(topic.to_owned()))?;
    // The largest header: v5, a packet ID and an empty property block.
    let h = PublishHeader {
        version: Version::V5,
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic,
        pkid: 1,
        properties: &[],
    };
    if h.remaining(payload_len) > varint::MAX {
        return Err(Error::TooLarge);
    }
    Ok(())
}

/// The filters of a SUBSCRIBE or UNSUBSCRIBE.
pub(crate) fn filters<'a>(filters: impl IntoIterator<Item = &'a str>) -> Result<()> {
    // Packet ID and a property block with a Subscription Identifier.
    let mut len = 2 + 6;
    for f in filters {
        validate_filter(f).map_err(|_| Error::InvalidTopic(f.to_owned()))?;
        len += 3 + f.len();
    }
    if len > varint::MAX {
        return Err(Error::TooLarge);
    }
    Ok(())
}

/// The fields of CONNECT.
pub(crate) fn options(o: &ClientOptions) -> Result<()> {
    field("client ID", o.client_id.len())?;
    if let Some(u) = &o.username {
        field("username", u.len())?;
    }
    if let Some(p) = &o.password {
        field("password", p.len())?;
    }
    if let Some(w) = &o.will {
        validate_name(&w.topic).map_err(|_| Error::InvalidTopic(w.topic.clone()))?;
        field("will payload", w.payload.len())?;
    }
    Ok(())
}

const fn field(what: &'static str, len: usize) -> Result<()> {
    if len > MAX_FIELD_LEN {
        return Err(Error::TooLong(what));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LastWill;

    #[test]
    fn publish_needs_a_valid_topic_and_an_encodable_size() {
        assert!(publish("a/b", 10).is_ok());
        for bad in ["", "a/+", "a/#"] {
            assert!(
                matches!(publish(bad, 0), Err(Error::InvalidTopic(_))),
                "{bad}"
            );
        }
        let long = "t".repeat(MAX_FIELD_LEN + 1);
        assert!(matches!(publish(&long, 0), Err(Error::InvalidTopic(_))));
        assert!(matches!(publish("a", varint::MAX), Err(Error::TooLarge)));
        assert!(publish("a", varint::MAX - 8).is_ok());
    }

    #[test]
    fn filters_must_be_valid() {
        assert!(filters(["a/#", "+/b", "$share/g/x"]).is_ok());
        assert!(matches!(filters(["a/#/b"]), Err(Error::InvalidTopic(f)) if f == "a/#/b"));
        assert!(matches!(filters([""]), Err(Error::InvalidTopic(_))));
    }

    #[test]
    fn connect_fields_fit_their_length_prefixes() {
        let long = "x".repeat(MAX_FIELD_LEN + 1);
        assert!(options(&ClientOptions::new("ok")).is_ok());
        let cases = [
            (ClientOptions::new(long.clone()), "client ID"),
            (
                ClientOptions {
                    username: Some(long.clone()),
                    ..ClientOptions::default()
                },
                "username",
            ),
            (
                ClientOptions {
                    password: Some(long.clone().into()),
                    ..ClientOptions::default()
                },
                "password",
            ),
            (
                ClientOptions {
                    will: Some(LastWill {
                        topic: "w".to_owned(),
                        payload: long.into(),
                        qos: QoS::AtMostOnce,
                        retain: false,
                        delay: 0,
                    }),
                    ..ClientOptions::default()
                },
                "will payload",
            ),
        ];
        for (opts, what) in cases {
            assert!(
                matches!(options(&opts), Err(Error::TooLong(w)) if w == what),
                "{what}"
            );
        }
        let bad_will = ClientOptions {
            will: Some(LastWill {
                topic: "w/#".to_owned(),
                payload: "x".into(),
                qos: QoS::AtMostOnce,
                retain: false,
                delay: 0,
            }),
            ..ClientOptions::default()
        };
        assert!(matches!(options(&bad_will), Err(Error::InvalidTopic(_))));
    }
}

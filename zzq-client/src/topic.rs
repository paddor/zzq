//! Topic names of received messages.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use bytes::Bytes;
use bytestring::ByteString;

/// A topic name. A received one shares the buffer it was read into, so it
/// costs no allocation, and a clone is one refcount update.
///
/// It reads as a `str` and compares with string types.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Topic(ByteString);

impl Topic {
    /// `topic`, which lies in `buf`, as a slice of `buf`.
    pub(crate) fn slice_of(buf: &Bytes, topic: &str) -> Topic {
        // A `&str` is UTF-8, so this cannot fail; the copy only keeps it
        // from panicking.
        ByteString::try_from(buf.slice_ref(topic.as_bytes())).map_or_else(|_| topic.into(), Topic)
    }

    /// The name.
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The name's bytes, still sharing the buffer.
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.0.into_bytes()
    }
}

impl Deref for Topic {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Topic {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for Topic {
    fn borrow(&self) -> &str {
        &self.0
    }
}

/// Hashes as the `str`, so maps keyed by topic look up by `&str`.
impl Hash for Topic {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl fmt::Debug for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self)
    }
}

impl From<&str> for Topic {
    fn from(s: &str) -> Topic {
        Topic(ByteString::from(s))
    }
}

/// Takes over the string's buffer.
impl From<String> for Topic {
    fn from(s: String) -> Topic {
        Topic(ByteString::from(s))
    }
}

impl From<Topic> for String {
    fn from(t: Topic) -> String {
        t.as_str().to_owned()
    }
}

impl PartialEq<str> for Topic {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Topic {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for Topic {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<Topic> for str {
    fn eq(&self, other: &Topic) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<Topic> for &str {
    fn eq(&self, other: &Topic) -> bool {
        *self == other.as_str()
    }
}

impl PartialEq<Topic> for String {
    fn eq(&self, other: &Topic) -> bool {
        self == other.as_str()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn a_received_topic_shares_the_buffer_and_reads_as_a_str() {
        let buf = Bytes::from_static(b"\x30\x09\x00\x07bench/tx");
        let name = std::str::from_utf8(&buf[4..11]).unwrap_or_default();
        let topic = Topic::slice_of(&buf, name);
        assert_eq!(topic, "bench/t");
        assert_eq!("bench/t", topic);
        assert_eq!(topic, String::from("bench/t"));
        assert!(topic.starts_with("bench/"));
        assert_eq!(format!("{topic} {topic:?}"), "bench/t \"bench/t\"");
        assert_eq!(topic.clone().into_bytes().as_ptr(), buf[4..].as_ptr());

        let set: HashSet<Topic> = [topic].into();
        assert!(set.contains("bench/t"), "hashes like the str");
    }
}

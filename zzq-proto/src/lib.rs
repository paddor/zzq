//! Sans-I/O MQTT 3.1.1 and 5.0 codec.
//!
//! The core is `no_std` and allocation-free: packets decode into borrowed
//! views over the input buffer and encode into any [`Sink`]. The `alloc`
//! feature adds owned helpers such as the [`trie::TopicTrie`].
//!
//! # Examples
//!
//! ```
//! use zzq_proto::encode::{self, PublishHeader, SliceSink};
//! use zzq_proto::{Packet, QoS, Version, decode};
//!
//! let header = PublishHeader {
//!     version: Version::V5,
//!     dup: false,
//!     qos: QoS::AtLeastOnce,
//!     retain: false,
//!     topic: "a/b",
//!     pkid: 7,
//!     properties: &[],
//! };
//! // No allocation: encode into a fixed buffer.
//! let mut storage = [0u8; 64];
//! let mut sink = SliceSink::new(&mut storage);
//! encode::publish(&header, b"hi", &mut sink);
//! let buf = sink.written().expect("fits");
//!
//! let (packet, used) = decode(buf, Version::V5, usize::MAX)?.expect("a whole packet");
//! assert_eq!(used, buf.len());
//! let Packet::Publish(p) = packet else {
//!     panic!("not a PUBLISH");
//! };
//! assert_eq!((p.topic, p.pkid, p.payload), ("a/b", 7, &b"hi"[..]));
//! # Ok::<(), zzq_proto::DecodeError>(())
//! ```

#![no_std]

#[cfg(feature = "alloc")]
extern crate alloc;
#[cfg(test)]
extern crate std;

mod cursor;
pub mod encode;
mod error;
pub mod packet;
pub mod property;
pub mod reason;
pub mod sn;
pub mod topic;
#[cfg(feature = "alloc")]
pub mod trie;
mod types;
pub mod varint;

pub use encode::{Sink, SliceSink};
pub use error::DecodeError;
pub use packet::{Packet, decode, fixed_header};
pub use property::{Properties, Property};
pub use reason::ReasonCode;
pub use types::{PacketType, QoS, RetainHandling, SubscriptionOptions, Version};

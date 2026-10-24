//! Sans-I/O MQTT 3.1.1 and 5.0 codec.
//!
//! The core is `no_std` and allocation-free: packets decode into borrowed
//! views over the input buffer and encode into any [`Sink`]. The `alloc`
//! feature adds owned helpers such as the [`trie::TopicTrie`].

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
pub use types::{PacketType, QoS, SubscriptionOptions, Version};

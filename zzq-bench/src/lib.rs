//! Raw-socket MQTT load generator and broker memory probe.
//!
//! Publishers write pre-encoded batches and subscribers count and ack per
//! read, so the broker is the bottleneck, not the client.

#![forbid(unsafe_code)]

pub mod child;
pub mod load;
pub mod ram;
pub mod wire;

//! Tokio engine shared by the zzq broker and client.
//!
//! A [`Context`] owns one or more IO lanes. A lane is a thread with a
//! current-thread Tokio runtime and a local task set. Connections are
//! assigned to one lane and stay there. Transports produce plain sockets
//! that can move between lanes; the lane wraps them for async IO.

pub mod context;
pub mod endpoint;
mod error;
pub mod transport;

pub use context::{Config, Context, LaneHandle};
pub use endpoint::Endpoint;
pub use error::{Error, Result};
pub use zzq_proto as proto;

//! Tokio engine shared by the zzq broker and client.
//!
//! A [`Context`] owns one or more IO lanes. A lane is a thread with a
//! current-thread Tokio runtime and a local task set. Connections are
//! assigned to one lane and stay there. Transports produce plain sockets
//! that can move between lanes; the lane wraps them for async IO.
//!
//! Applications use [`Context`], [`Config`], [`Endpoint`] and the transport
//! options. Hidden items are the engine API that zzq-client and zzq-broker
//! share; they are not covered by semver.

#![forbid(unsafe_code)]

pub mod context;
pub mod endpoint;
mod error;
pub mod transport;

pub use context::{Config, Context, LaneHandle};
pub use endpoint::Endpoint;
pub use error::{BoxError, Error, Result};
pub use zzq_proto as proto;

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
//!
//! # Examples
//!
//! ```
//! use zzq_tokio::{Config, Context, Endpoint};
//!
//! let ctx = Context::with_config(&Config {
//!     io_threads: 2,
//!     ..Config::default()
//! })?;
//! assert_eq!(ctx.lanes(), 2);
//!
//! let endpoint: Endpoint = "tcp://localhost".parse()?;
//! assert_eq!(endpoint.to_string(), "tcp://localhost:1883");
//! # Ok::<(), zzq_tokio::Error>(())
//! ```

#![forbid(unsafe_code)]

pub mod context;
pub mod endpoint;
mod error;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
pub mod transport;

pub use context::{Config, Context, LaneHandle};
pub use endpoint::Endpoint;
pub use error::{BoxError, Error, Result};
pub use zzq_proto as proto;

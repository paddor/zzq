//! Rust errors as `ZZQ::Error` and its subclasses, which `lib/zzq/errors.rb`
//! defines before the extension loads.

use std::sync::OnceLock;

use rb_sys::VALUE;

use crate::rb::{self, RbResult, RubyErr};

/// An error from the client or the broker crate.
#[derive(Debug)]
pub enum Error {
    Client(zzq_client::Error),
    Broker(zzq_broker::Error),
}

impl From<zzq_client::Error> for Error {
    fn from(e: zzq_client::Error) -> Self {
        Error::Client(e)
    }
}

impl From<zzq_broker::Error> for Error {
    fn from(e: zzq_broker::Error) -> Self {
        Error::Broker(e)
    }
}

struct Classes {
    error: VALUE,
    closed: VALUE,
    transport: VALUE,
    protocol: VALUE,
    invalid_topic: VALUE,
    too_large: VALUE,
    refused: VALUE,
    rejected: VALUE,
}

unsafe impl Send for Classes {}
unsafe impl Sync for Classes {}

static CLASSES: OnceLock<Classes> = OnceLock::new();

pub fn init(zzq: VALUE) -> RbResult<()> {
    let classes = Classes {
        error: rb::const_get(zzq, c"Error")?,
        closed: rb::const_get(zzq, c"ClosedError")?,
        transport: rb::const_get(zzq, c"TransportError")?,
        protocol: rb::const_get(zzq, c"ProtocolError")?,
        invalid_topic: rb::const_get(zzq, c"InvalidTopic")?,
        too_large: rb::const_get(zzq, c"TooLarge")?,
        refused: rb::const_get(zzq, c"ConnectionRefused")?,
        rejected: rb::const_get(zzq, c"Rejected")?,
    };
    let _ = CLASSES.set(classes);
    Ok(())
}

fn classes() -> &'static Classes {
    CLASSES.get().expect("error classes loaded")
}

/// `ZZQ::Error` with `message`.
pub fn zzq(message: impl Into<String>) -> RubyErr {
    RubyErr::new(classes().error, message)
}

/// The message of `e` and of each error that caused it.
fn chain(e: &dyn std::error::Error) -> String {
    let mut message = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        message.push_str(": ");
        message.push_str(&s.to_string());
        source = s.source();
    }
    message
}

/// An exception that carries the reason code.
fn with_code(class: VALUE, message: &str, code: zzq_client::ReasonCode) -> RubyErr {
    let built = rb::new_utf8_string(message).and_then(|m| {
        let code = rb::u64_value(u8::from(code).into());
        rb::call(class, c"new", &[m, code])
    });
    match built {
        Ok(exc) => RubyErr::Exception(exc),
        Err(e) => e,
    }
}

impl From<Error> for RubyErr {
    fn from(e: Error) -> Self {
        use zzq_client::Error as C;
        let c = classes();
        let message = match &e {
            Error::Client(e) => chain(e),
            Error::Broker(e) => chain(e),
        };
        let class = match e {
            Error::Client(C::Refused(code)) => return with_code(c.refused, &message, code),
            Error::Client(C::Rejected(code)) => return with_code(c.rejected, &message, code),
            Error::Client(C::Transport(_)) | Error::Broker(zzq_broker::Error::Transport(_)) => {
                c.transport
            }
            Error::Client(C::Protocol(_)) => c.protocol,
            Error::Client(C::InvalidTopic(_))
            | Error::Broker(zzq_broker::Error::InvalidTopic(_)) => c.invalid_topic,
            Error::Client(C::TooLarge | C::TooLong(_))
            | Error::Broker(zzq_broker::Error::TooLarge) => c.too_large,
            Error::Client(C::Closed) | Error::Broker(zzq_broker::Error::Closed) => c.closed,
            _ => c.error,
        };
        RubyErr::new(class, message)
    }
}

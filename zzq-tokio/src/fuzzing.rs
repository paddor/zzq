//! Entry points for the fuzz targets in `fuzz/afl`. Not part of the API.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Runs `f` to completion. Reads from a byte slice never wait, so one poll
/// is enough.
fn ready<F: Future>(f: F) -> Option<F::Output> {
    match pin!(f).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

/// Reads `data` as a WebSocket upgrade request and the frames after it.
pub fn websocket_server(data: &[u8]) {
    let _ = ready(crate::transport::ws::fuzz_server(data));
}

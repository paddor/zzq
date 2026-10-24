//! `inproc://` in-memory streams within one context.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use tokio::io::DuplexStream;
use tokio::sync::mpsc;

use crate::endpoint::Endpoint;
use crate::transport::Stream;
use crate::{Context, Error, Result};

/// Buffer size of each direction of an inproc stream.
const PIPE_BYTES: usize = 1 << 20;

/// Bound inproc names of one context.
#[derive(Debug, Default)]
pub(crate) struct Registry {
    names: Mutex<HashMap<String, mpsc::Sender<DuplexStream>>>,
}

/// A bound inproc name. Unbinds on drop.
#[derive(Debug)]
pub struct InprocListener {
    rx: mpsc::Receiver<DuplexStream>,
    name: String,
    registry: Weak<Registry>,
}

impl InprocListener {
    pub(crate) fn bind(ctx: &Context, name: &str) -> Result<InprocListener> {
        let registry = ctx.inproc();
        let mut names = registry
            .names
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if names.get(name).is_some_and(|tx| !tx.is_closed()) {
            return Err(Error::AddressInUse(format!("inproc://{name}")));
        }
        let (tx, rx) = mpsc::channel(64);
        names.insert(name.to_owned(), tx);
        Ok(InprocListener {
            rx,
            name: name.to_owned(),
            registry: Arc::downgrade(registry),
        })
    }

    pub(crate) async fn accept(&mut self) -> Result<Stream> {
        self.rx
            .recv()
            .await
            .map(Stream::Inproc)
            .ok_or(Error::Closed)
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        Endpoint::Inproc(self.name.clone())
    }
}

impl Drop for InprocListener {
    fn drop(&mut self) {
        self.rx.close();
        if let Some(registry) = self.registry.upgrade() {
            let mut names = registry
                .names
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if names.get(&self.name).is_some_and(mpsc::Sender::is_closed) {
                names.remove(&self.name);
            }
        }
    }
}

pub(crate) async fn connect(ctx: &Context, name: &str) -> Result<Stream> {
    let refused = || Error::Refused(format!("inproc://{name}"));
    let tx = {
        let names = ctx
            .inproc()
            .names
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        names.get(name).cloned().ok_or_else(refused)?
    };
    let (local, remote) = tokio::io::duplex(PIPE_BYTES);
    tx.send(remote).await.map_err(|_| refused())?;
    Ok(Stream::Inproc(local))
}

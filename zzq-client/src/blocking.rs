//! Blocking client on the same session task as [`crate::Client`].
//!
//! For threads without an async runtime. Calls made inside one return
//! [`Error::InAsync`] instead of blocking it.
//!
//! # Examples
//!
//! ```no_run
//! use zzq_client::blocking::Client;
//! use zzq_client::{ClientOptions, Context, QoS};
//!
//! let ctx = Context::new()?;
//! let opts = ClientOptions::new("sensor");
//! let client = Client::connect(&ctx, "tcp://localhost:1883", opts)?;
//! client.publish("sensors/temp", "21.5", QoS::AtLeastOnce)?;
//! client.disconnect();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::pin::pin;
use std::sync::Arc;
use std::task::{self, Poll, Wake, Waker};

use zzq_proto::{QoS, SubscriptionOptions};
use zzq_tokio::Context;

use crate::{
    Ack, ClientOptions, Cmd, Connected, Error, Link, Message, ReasonCode, Result, inbox, start,
    subscribe_cmd, unsubscribe_cmd,
};

/// A connected client with blocking calls.
#[derive(Debug)]
pub struct Client {
    link: Link,
    inbox: inbox::Receiver,
    connected: Connected,
}

fn not_in_async() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        Err(Error::InAsync)
    } else {
        Ok(())
    }
}

fn wait<T>(rx: Ack<T>) -> Result<T> {
    rx.blocking_recv().map_err(|_| Error::Closed)?
}

/// Runs `f` to completion on this thread, parking it while `f` waits.
fn block_on<F: Future>(f: F) -> F::Output {
    struct Unpark(std::thread::Thread);

    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    thread_local! {
        static WAKER: Waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    }

    let mut f = pin!(f);
    WAKER.with(|waker| {
        let mut cx = task::Context::from_waker(waker);
        loop {
            if let Poll::Ready(out) = f.as_mut().poll(&mut cx) {
                return out;
            }
            std::thread::park();
        }
    })
}

impl Client {
    /// Connects to `uri` and waits for CONNACK.
    ///
    /// # Errors
    ///
    /// As [`crate::Client::connect`], and [`Error::InAsync`] inside an async
    /// runtime.
    pub fn connect(ctx: &Context, uri: &str, opts: ClientOptions) -> Result<Client> {
        not_in_async()?;
        let st = start(ctx, uri, opts)?;
        let connected = wait(st.connack)?;
        Ok(Client {
            link: st.link,
            inbox: st.inbox,
            connected,
        })
    }

    /// CONNACK details.
    pub const fn connected(&self) -> &Connected {
        &self.connected
    }

    /// Publishes. QoS 0 returns once queued, waiting while about 1 MiB is
    /// queued already; QoS 1 and 2 wait for the ack.
    ///
    /// # Errors
    ///
    /// As [`crate::Client::publish`], and [`Error::InAsync`] inside an async
    /// runtime.
    pub fn publish(&self, topic: &str, payload: impl AsRef<[u8]>, qos: QoS) -> Result<()> {
        self.send_publish(topic, payload.as_ref(), qos, false)
    }

    /// Publishes a retained message: the broker keeps it for later
    /// subscribers. An empty payload clears the topic.
    ///
    /// # Errors
    ///
    /// As [`Client::publish`].
    pub fn publish_retained(&self, topic: &str, payload: impl AsRef<[u8]>, qos: QoS) -> Result<()> {
        self.send_publish(topic, payload.as_ref(), qos, true)
    }

    fn send_publish(&self, topic: &str, payload: &[u8], qos: QoS, retain: bool) -> Result<()> {
        not_in_async()?;
        block_on(self.link.publish(topic, payload, qos, retain))?.map_or(Ok(()), block_on)
    }

    /// Subscribes to one filter with a maximum QoS. Returns the granted
    /// QoS, or the reason the broker refused the filter.
    ///
    /// # Errors
    ///
    /// As [`crate::Client::subscribe`], and [`Error::InAsync`] inside an
    /// async runtime.
    pub fn subscribe(&self, filter: &str, qos: QoS) -> Result<ReasonCode> {
        let codes = self.subscribe_with(&[(filter, SubscriptionOptions::from(qos))], 0)?;
        Ok(codes
            .first()
            .copied()
            .unwrap_or(ReasonCode::UNSPECIFIED_ERROR))
    }

    /// Subscribes to several filters, optionally with a v5 Subscription
    /// Identifier. Returns one code per filter.
    ///
    /// # Errors
    ///
    /// As [`Client::subscribe`].
    pub fn subscribe_with(
        &self,
        filters: &[(&str, SubscriptionOptions)],
        sub_id: u32,
    ) -> Result<Vec<ReasonCode>> {
        not_in_async()?;
        let (cmd, rx) = subscribe_cmd(filters, sub_id)?;
        self.link.send(cmd)?;
        wait(rx)
    }

    /// Unsubscribes. Returns one code per filter (empty for v3.1.1).
    ///
    /// # Errors
    ///
    /// As [`Client::subscribe`].
    pub fn unsubscribe(&self, filters: &[&str]) -> Result<Vec<ReasonCode>> {
        not_in_async()?;
        let (cmd, rx) = unsubscribe_cmd(filters)?;
        self.link.send(cmd)?;
        wait(rx)
    }

    /// Next received message. `None` once the client is gone for good, or
    /// when called inside an async runtime.
    pub fn recv(&mut self) -> Option<Message> {
        not_in_async().ok()?;
        block_on(self.inbox.recv())
    }

    /// Sends DISCONNECT and closes.
    pub fn disconnect(self) {
        if not_in_async().is_err() {
            return;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .link
            .send(Cmd::Disconnect {
                reason: ReasonCode::SUCCESS,
                done: tx,
            })
            .is_ok()
        {
            let _ = rx.blocking_recv();
        }
    }
}

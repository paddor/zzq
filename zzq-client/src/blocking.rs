//! Blocking client on the same session task as [`crate::Client`].
//!
//! For threads without an async runtime. Calls made inside one return
//! [`Error::InAsync`] instead of blocking it.

use bytes::Bytes;
use tokio::sync::mpsc;
use zzq_proto::{QoS, SubscriptionOptions};
use zzq_tokio::Context;

use crate::{
    Ack, ClientOptions, Cmd, Connected, Error, Message, ReasonCode, Result, publish_cmd, start,
    subscribe_cmd, unsubscribe_cmd,
};

/// A connected client with blocking calls.
#[derive(Debug)]
pub struct Client {
    cmd: mpsc::UnboundedSender<Cmd>,
    inbox: mpsc::Receiver<Message>,
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

impl Client {
    /// Connects to `uri` and waits for CONNACK.
    pub fn connect(ctx: &Context, uri: &str, opts: ClientOptions) -> Result<Client> {
        not_in_async()?;
        let st = start(ctx, uri, opts)?;
        let connected = wait(st.connack)?;
        Ok(Client {
            cmd: st.cmd,
            inbox: st.inbox,
            connected,
        })
    }

    /// CONNACK details.
    pub const fn connected(&self) -> &Connected {
        &self.connected
    }

    /// Publishes. QoS 0 returns once queued; QoS 1 and 2 wait for the ack.
    pub fn publish(
        &self,
        topic: &str,
        payload: impl Into<Bytes>,
        qos: QoS,
        retain: bool,
    ) -> Result<()> {
        not_in_async()?;
        let (cmd, rx) = publish_cmd(topic, payload.into(), qos, retain)?;
        self.send(cmd)?;
        rx.map_or(Ok(()), wait)
    }

    /// Subscribes to one filter with a maximum QoS. Returns the granted code.
    pub fn subscribe(&self, filter: &str, qos: QoS) -> Result<ReasonCode> {
        let codes = self.subscribe_with(&[(filter, SubscriptionOptions::from(qos))], 0)?;
        Ok(codes
            .first()
            .copied()
            .unwrap_or(ReasonCode::UNSPECIFIED_ERROR))
    }

    /// Subscribes to several filters, optionally with a v5 Subscription
    /// Identifier. Returns one code per filter.
    pub fn subscribe_with(
        &self,
        filters: &[(&str, SubscriptionOptions)],
        sub_id: u32,
    ) -> Result<Vec<ReasonCode>> {
        not_in_async()?;
        let (cmd, rx) = subscribe_cmd(filters, sub_id)?;
        self.send(cmd)?;
        wait(rx)
    }

    /// Unsubscribes. Returns one code per filter (empty for v3.1.1).
    pub fn unsubscribe(&self, filters: &[&str]) -> Result<Vec<ReasonCode>> {
        not_in_async()?;
        let (cmd, rx) = unsubscribe_cmd(filters)?;
        self.send(cmd)?;
        wait(rx)
    }

    /// Next received message. `None` once the client is gone for good, or
    /// when called inside an async runtime.
    pub fn recv(&mut self) -> Option<Message> {
        not_in_async().ok()?;
        let m = self.inbox.blocking_recv()?;
        crate::ack(&self.cmd, &m);
        Some(m)
    }

    /// Sends DISCONNECT and closes.
    pub fn disconnect(self) {
        if not_in_async().is_err() {
            return;
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .send(Cmd::Disconnect {
                reason: ReasonCode::SUCCESS,
                done: tx,
            })
            .is_ok()
        {
            let _ = rx.blocking_recv();
        }
    }

    fn send(&self, cmd: Cmd) -> Result<()> {
        self.cmd.send(cmd).map_err(|_| Error::Closed)
    }
}

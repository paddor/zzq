//! The client session task: connects, runs one connection at a time and
//! reconnects with backoff.
//!
//! [`Session`] outlives connections. It holds the requests still waiting
//! for an ack, as encoded packets, and the subscriptions, so a new
//! connection can resend the first and restore the second.

use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rustc_hash::{FxHashMap, FxHashSet};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::oneshot;
use zzq_proto::encode;
use zzq_proto::{QoS, ReasonCode, SubscriptionOptions};
use zzq_tokio::transport::{Driver, TransportOptions};
use zzq_tokio::{Context, Endpoint};

use crate::acks::Acks;
use crate::submit::{self, Batch, Op};
use crate::{ClientOptions, Cmd, Connected, Error, Result, inbox};

mod handshake;
mod inbound;
mod io;
mod outbound;

use handshake::handshake;
use io::{Out, make_room, writer};

/// Largest packet accepted from the broker.
const MAX_PACKET: usize = 256 << 20;

/// Read buffer size. Received payloads are slices of it.
const READ_BUF: usize = 64 << 10;

/// Bytes read on while the inbox is full, for the acks of the client's own
/// requests among them.
const READ_AHEAD: usize = 4 << 20;

/// Spare room below which a read gets a fresh buffer first.
const READ_MIN: usize = 8 << 10;

/// How long a session that ends gives its writer to send what is left,
/// such as DISCONNECT, to a broker that does not read.
const CLOSE_LINGER: Duration = Duration::from_secs(2);

/// How long the call woken for a batch of publish acks has to wake the
/// others before the session does.
const CHAIN_WAIT: Duration = Duration::from_millis(1);

/// Connects, then keeps the session going until the client disconnects,
/// the handle is dropped, or a connection fails without `reconnect`.
pub(crate) async fn run_session(
    ctx: Context,
    endpoint: Endpoint,
    transport: TransportOptions,
    mut s: Session,
) {
    let (min, max) = s
        .opts
        .reconnect
        .map_or((Duration::ZERO, Duration::ZERO), |r| (r.min, r.max));
    let mut delay = min;
    let limit = s.opts.connect_timeout;
    loop {
        let connect = zzq_tokio::transport::connect(&ctx, &endpoint, &transport);
        match tokio::time::timeout(limit, connect).await {
            Ok(Ok(stream)) => match stream.drive(ConnDriver(s)).await {
                Ok(back) => s = back,
                Err(_) => return,
            },
            failed => {
                if let Some(tx) = s.connack.take() {
                    let e = match failed {
                        Ok(Err(e)) => e.into(),
                        _ => Error::Closed,
                    };
                    let _ = tx.send(Err(e));
                    return;
                }
            }
        }
        if s.ended || s.opts.reconnect.is_none() || s.submit.dropped() {
            break;
        }
        // Only a connection that stayed up starts the backoff over. One that
        // ends soon after CONNACK, such as one another client with the same
        // ID takes over, keeps backing off.
        if s.established.take().is_some_and(|at| at.elapsed() >= max) {
            delay = min;
        }
        if !s.wait_offline(delay).await {
            break;
        }
        delay = (delay * 2).min(max);
    }
}

#[derive(Debug)]
enum Pending {
    /// A QoS 1/2 publish and its ack slot.
    Publish(usize),
    Sub(oneshot::Sender<Result<Vec<ReasonCode>>>, Change),
    /// A resubscribe after reconnecting, nobody waits for it.
    Restore,
}

/// What a SUBACK or UNSUBACK changes in the subscriptions to restore.
#[derive(Debug)]
enum Change {
    Add(Vec<(String, SubscriptionOptions)>, u32),
    Remove(Vec<String>),
}

/// State that outlives a connection.
#[derive(Debug)]
pub(crate) struct Session {
    pub(crate) opts: ClientOptions,
    pub(crate) submit: submit::Receiver,
    acks: Arc<Acks>,
    pub(crate) inbox: inbox::Sender,
    pub(crate) connack: Option<oneshot::Sender<Result<Connected>>>,
    requests: Requests,
    inbound: Inbound,
    /// Granted subscriptions: filter, options and Subscription Identifier.
    subs: Vec<(String, SubscriptionOptions, u32)>,
    scratch: Scratch,
    /// When a connection got its CONNACK, since the last backoff.
    established: Option<Instant>,
    /// Do not reconnect.
    ended: bool,
}

/// Requests sent to the broker and waiting for an ack, or for room.
#[derive(Debug)]
struct Requests {
    pending: FxHashMap<u16, Pending>,
    /// Encoded requests not yet acked, by packet ID, with their send order.
    unacked: FxHashMap<u16, (u64, Vec<u8>)>,
    sent: u64,
    next_pkid: u16,
    /// QoS 1/2 publishes beyond the broker's Receive Maximum.
    waiting: VecDeque<Cmd>,
    /// Bytes of the publishes in `waiting`. At `submit::QUEUED`, the session takes
    /// no more requests, so callers wait as on a full queue.
    waiting_bytes: usize,
    /// QoS 1/2 publishes sent and not yet acked, with those in `resend`.
    inflight: usize,
    /// Unacked publishes to send again after a reconnect, once the broker's
    /// Receive Maximum has room: their packet IDs, oldest first.
    resend: VecDeque<u16>,
    /// The broker's Receive Maximum.
    quota: usize,
}

/// QoS 2 messages from the broker.
#[derive(Debug, Default)]
struct Inbound {
    /// PUBREC sent, waiting for PUBREL.
    awaiting_rel: FxHashSet<u16>,
    /// In the inbox, PUBREC not sent yet.
    unconsumed: FxHashSet<u16>,
}

/// Buffers kept for their capacity, and what one read leaves to do.
#[derive(Debug, Default)]
struct Scratch {
    /// Requests taken from the queue.
    batch: Batch,
    /// Acks taken from the inbox.
    taken: Vec<(u16, QoS)>,
    /// Publish acks of the current read, for the waiting calls.
    finished: Vec<(usize, Result<()>)>,
    /// Woken calls have finished calls to wake.
    chained: bool,
    /// What a full inbox left to handle, in order: the rest of the read
    /// where handling stopped, then later reads without their acks.
    parked: VecDeque<Bytes>,
    /// Bytes in `parked`.
    parked_bytes: usize,
}

impl Session {
    pub(crate) fn new(
        opts: ClientOptions,
        submit: submit::Receiver,
        acks: Arc<Acks>,
        inbox: inbox::Sender,
        connack: oneshot::Sender<Result<Connected>>,
    ) -> Session {
        Session {
            opts,
            submit,
            acks,
            inbox,
            connack: Some(connack),
            requests: Requests {
                pending: FxHashMap::default(),
                unacked: FxHashMap::default(),
                sent: 0,
                next_pkid: 0,
                waiting: VecDeque::new(),
                waiting_bytes: 0,
                inflight: 0,
                resend: VecDeque::new(),
                quota: usize::from(u16::MAX),
            },
            inbound: Inbound::default(),
            subs: Vec::new(),
            scratch: Scratch::default(),
            established: None,
            ended: false,
        }
    }
}

struct ConnDriver(Session);

impl Driver for ConnDriver {
    type Output = Session;

    fn run<R, W>(self, reader: R, writer: W) -> impl Future<Output = Session>
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
        run(self.0, reader, writer)
    }
}

/// One connection of a session.
struct Conn<'a> {
    s: &'a mut Session,
    out: Rc<Out>,
}

async fn run<R, W>(mut s: Session, mut r: R, mut w: W) -> Session
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    let mut buf = BytesMut::with_capacity(READ_BUF);
    let resume = s.connack.is_none();
    let handshake = handshake(&s.opts, resume, &mut r, &mut w, &mut buf);
    let connected = tokio::time::timeout(s.opts.connect_timeout, handshake)
        .await
        .unwrap_or(Err(Error::Closed));
    let session_present = match connected {
        Ok((c, quota)) => {
            s.established = Some(Instant::now());
            s.requests.quota = usize::from(quota.max(1));
            let present = c.session_present;
            if let Some(tx) = s.connack.take() {
                let _ = tx.send(Ok(c));
            }
            present
        }
        Err(e) => {
            // A refused first connect ends the client; a lost one retries.
            if let Some(tx) = s.connack.take() {
                let _ = tx.send(Err(e));
                s.ended = true;
            } else if let Error::Refused(code) = e {
                s.ended = !retryable(code);
            }
            return s;
        }
    };
    let out = Rc::new(Out::default());
    let mut writer = tokio::task::spawn_local(writer(out.clone(), w));
    let mut c = Conn { s: &mut s, out };
    if resume {
        c.restore(session_present);
    }
    let _ = c.serve(&mut r, &mut buf).await;
    c.out.close();
    // Packets a full inbox left unread belong to this connection: with the
    // session, the broker sends its messages again; without, their packet
    // IDs mean other messages on the next.
    s.scratch.parked.clear();
    s.scratch.parked_bytes = 0;
    // A DISCONNECT gets a while to go out. A lost connection's output is
    // not worth waiting for: its unacked requests go again on the next.
    if s.ended {
        let _ = tokio::time::timeout(CLOSE_LINGER, &mut writer).await;
    }
    writer.abort();
    s
}

impl Conn<'_> {
    /// Reads, runs commands and pings until the connection ends.
    async fn serve<R: AsyncRead + Unpin>(&mut self, r: &mut R, buf: &mut BytesMut) -> Result<()> {
        self.input(buf)?;
        // Requests made while offline.
        if !self.requests() {
            return Ok(());
        }
        self.ack_taken();
        let ka = u64::from(self.s.opts.keep_alive);
        let mut ping = tokio::time::interval(Duration::from_secs(ka.max(1)));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping.tick().await;
        let mut last_rx = Instant::now();
        let chain = tokio::time::sleep(CHAIN_WAIT);
        tokio::pin!(chain);
        let mut chain_armed = false;
        // Requests are queued that the session had no room for.
        let mut deferred = false;
        loop {
            if let Some(end) = self.between(&mut deferred) {
                return end;
            }
            // A full inbox stops reading past READ_AHEAD, not pings, requests
            // or acks.
            let blocked = self.s.inbox.blocked();
            make_room(buf);
            if self.s.scratch.chained && !chain_armed {
                chain.as_mut().reset((Instant::now() + CHAIN_WAIT).into());
                chain_armed = true;
            }
            tokio::select! {
                () = self.s.submit.ready() => {
                    if !self.on_requests(&mut deferred) {
                        return Ok(());
                    }
                }
                () = self.out.drained.notified(), if !self.out.has_room() => {}
                n = r.read_buf(buf), if self.s.scratch.parked_bytes < READ_AHEAD => {
                    if !matches!(n, Ok(n) if n > 0) {
                        return Err(Error::Closed);
                    }
                    last_rx = Instant::now();
                    self.input(buf)?;
                }
                () = self.s.inbox.room(), if blocked => {
                    self.resume()?;
                    // Nothing was read meanwhile, not even PINGRESP.
                    last_rx = Instant::now();
                }
                () = self.s.inbox.acked() => self.ack_taken(),
                () = &mut chain, if chain_armed => {
                    chain_armed = false;
                    self.s.scratch.chained = false;
                    self.s.acks.wake_chained();
                }
                _ = ping.tick(), if ka > 0 => {
                    if !blocked && last_rx.elapsed() > Duration::from_millis(ka * 1500) {
                        return Err(Error::Closed);
                    }
                    self.out.push(encode::pingreq);
                }
            }
        }
    }

    /// Checks between events: a broken writer ends the connection, and
    /// `deferred` requests run once there is room. `Some` to stop.
    fn between(&mut self, deferred: &mut bool) -> Option<Result<()>> {
        if self.out.broken.get() {
            return Some(Err(Error::Closed));
        }
        if *deferred && self.takes_requests() {
            *deferred = false;
            if !self.requests() {
                return Some(Ok(()));
            }
        }
        None
    }

    /// Runs queued requests, or without room for them only a DISCONNECT,
    /// and marks the rest `deferred`. False once the session ends.
    fn on_requests(&mut self, deferred: &mut bool) -> bool {
        if self.takes_requests() {
            return self.requests();
        }
        if let Some((reason, done)) = self.s.submit.take_disconnect() {
            self.say_bye(reason, done);
            self.s.ended = true;
            return false;
        }
        if self.s.submit.dropped() {
            self.s.ended = true;
            return false;
        }
        *deferred = true;
        true
    }
}

impl Session {
    /// Waits `delay` before the next connect. Watches the requests meanwhile,
    /// so `disconnect` works while offline. Returns false to stop.
    async fn wait_offline(&mut self, delay: Duration) -> bool {
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                () = &mut sleep => return true,
                () = self.submit.ready() => {
                    if let Some((_, done)) = self.submit.take_disconnect() {
                        let _ = done.send(());
                        self.ended = true;
                        return false;
                    }
                    if self.submit.dropped() {
                        self.ended = true;
                        return false;
                    }
                }
            }
        }
    }

    /// Fails every request still waiting for an ack, and refuses new ones.
    fn fail_pending(&mut self) {
        let queued = self.submit.close();
        let left = std::mem::take(&mut self.scratch.batch);
        let mut cmds: Vec<Cmd> = self.requests.waiting.drain(..).collect();
        self.requests.waiting_bytes = 0;
        for op in left.ops.into_iter().chain(queued.ops) {
            match op {
                Op::Publish {
                    qos: QoS::AtMostOnce,
                    ..
                } => {}
                Op::Publish { slot, .. } => self.scratch.finished.push((slot, Err(Error::Closed))),
                Op::Cmd(cmd) => cmds.push(cmd),
            }
        }
        for cmd in cmds {
            match cmd {
                Cmd::Publish { slot, .. } => self.scratch.finished.push((slot, Err(Error::Closed))),
                Cmd::Subscribe { done, .. } | Cmd::Unsubscribe { done, .. } => {
                    let _ = done.send(Err(Error::Closed));
                }
                Cmd::Disconnect { done, .. } => {
                    let _ = done.send(());
                }
            }
        }
        for (_, p) in self.requests.pending.drain() {
            match p {
                Pending::Publish(slot) => self.scratch.finished.push((slot, Err(Error::Closed))),
                Pending::Sub(tx, _) => {
                    let _ = tx.send(Err(Error::Closed));
                }
                Pending::Restore => {}
            }
        }
        self.acks.finish(&mut self.scratch.finished);
        self.acks.wake_chained();
    }
}

impl Drop for Session {
    /// Whether it ended, failed or was dropped with its lane, the waiting
    /// calls learn the session is gone.
    fn drop(&mut self) {
        self.fail_pending();
    }
}

/// Refusals worth another attempt: the broker is busy or full, not
/// rejecting the client.
const fn retryable(code: ReasonCode) -> bool {
    matches!(
        code,
        ReasonCode::SERVER_UNAVAILABLE
            | ReasonCode::SERVER_BUSY
            | ReasonCode::QUOTA_EXCEEDED
            | ReasonCode::CONNECTION_RATE_EXCEEDED
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_busy_or_full_brokers_are_retried() {
        for code in [
            ReasonCode::SERVER_UNAVAILABLE,
            ReasonCode::SERVER_BUSY,
            ReasonCode::QUOTA_EXCEEDED,
            ReasonCode::CONNECTION_RATE_EXCEEDED,
        ] {
            assert!(retryable(code), "{code}");
        }
        for code in [ReasonCode::NOT_AUTHORIZED, ReasonCode::BANNED] {
            assert!(!retryable(code), "{code}");
        }
    }
}

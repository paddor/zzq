//! The broker handle, shared state and lane setup.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Notify, mpsc, oneshot};
use zzq_proto::QoS;
use zzq_proto::topic::validate_name;
use zzq_tokio::transport::Listener;
use zzq_tokio::{Context, Endpoint};

use crate::conn::ConnDriver;
use crate::lane::{Control, Lane, LaneRef, LaneStats, Remote};
use crate::msg::Msg;
use crate::options::BrokerOptions;
use crate::retain::RetainedStore;
use crate::{Error, Result};

/// Batches in flight to one lane before senders wait.
const DATA_BATCHES: usize = 64;
/// Interval of keep alive and session expiry checks.
const SWEEP: Duration = Duration::from_secs(1);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static LANES: RefCell<HashMap<u64, LaneRef>> = RefCell::new(HashMap::new());
}

/// Sends batches for other lanes collected on `lane`. Waits while a target
/// lane is full, which slows the caller down instead of dropping.
pub(crate) async fn ship(lane: &LaneRef) {
    let (shared, remote) = {
        let mut l = lane.borrow_mut();
        (l.shared.clone(), l.take_remote())
    };
    for (target, batch) in remote {
        let _ = shared.lanes[target].data.send(batch).await;
    }
}

/// The lane state of broker `id` on the current lane thread.
pub(crate) fn lane_ref(id: u64) -> Option<LaneRef> {
    LANES.with(|l| l.borrow().get(&id).cloned())
}

#[derive(Debug)]
pub(crate) struct LaneTx {
    pub(crate) control: mpsc::UnboundedSender<Control>,
    pub(crate) data: mpsc::Sender<Vec<Remote>>,
}

/// State shared by all lanes of one broker.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) id: u64,
    pub(crate) ctx: Context,
    pub(crate) opts: BrokerOptions,
    pub(crate) retained: Mutex<RetainedStore>,
    pub(crate) lanes: Box<[LaneTx]>,
    /// Connected clients on all lanes, for `max_connections`.
    pub(crate) connections: AtomicUsize,
    /// Accept loops and other tasks stopped when the broker is dropped.
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

/// Broker counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub connections: u64,
    pub sessions: u64,
    /// Subscriptions in the index of one lane (all lanes hold all of them).
    pub subscriptions: u64,
    pub retained: u64,
    pub publishes: u64,
    pub deliveries: u64,
    pub dropped_buffer_full: u64,
    pub dropped_offline: u64,
    pub dropped_queue_full: u64,
    pub dropped_too_large: u64,
    /// Queued messages whose Message Expiry Interval ran out.
    pub dropped_expired: u64,
    /// Retained messages not stored because `max_retained` was reached.
    pub retained_rejected: u64,
}

/// An MQTT broker. Dropping it stops listening and closes all connections.
#[derive(Debug)]
pub struct Broker {
    shared: Arc<Shared>,
}

impl Broker {
    /// Starts a broker on the lanes of `ctx`.
    pub async fn new(ctx: &Context, opts: BrokerOptions) -> Result<Broker> {
        let n = ctx.lanes();
        let mut lanes = Vec::with_capacity(n);
        let mut receivers = Vec::with_capacity(n);
        for _ in 0..n {
            let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
            let (data_tx, data_rx) = mpsc::channel(DATA_BATCHES);
            lanes.push(LaneTx {
                control: ctl_tx,
                data: data_tx,
            });
            receivers.push((ctl_rx, data_rx));
        }
        let retained = RetainedStore::new(opts.persistence.clone(), opts.max_retained)?;
        let shared = Arc::new(Shared {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            ctx: ctx.clone(),
            opts,
            retained: Mutex::new(retained),
            lanes: lanes.into_boxed_slice(),
            connections: AtomicUsize::new(0),
            tasks: Mutex::default(),
        });
        for (i, (ctl_rx, data_rx)) in receivers.into_iter().enumerate() {
            let s = shared.clone();
            ctx.lane(i)
                .call(move || install(i as u16, s, ctl_rx, data_rx))
                .await?;
        }
        if let Some(every) = shared.opts.sys_interval {
            let s = shared.clone();
            ctx.lane(0)
                .call(move || {
                    let task = tokio::task::spawn_local(crate::sys::run(s.clone(), every));
                    s.tasks
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push(task.abort_handle());
                })
                .await?;
        }
        Ok(Broker { shared })
    }

    /// Binds an endpoint such as `tcp://0.0.0.0:1883` and returns the bound
    /// endpoint, with the actual port for port 0.
    pub async fn bind(&self, uri: &str) -> Result<Endpoint> {
        let endpoint = Endpoint::parse(uri)?;
        let (tx, rx) = oneshot::channel();
        let shared = self.shared.clone();
        let lane = self.shared.ctx.next_lane().clone();
        lane.spawn(move || async move {
            if matches!(endpoint, Endpoint::Udp { .. }) {
                bind_udp(shared, endpoint, tx).await;
                return;
            }
            let listener =
                match Listener::bind(&shared.ctx, &endpoint, &shared.opts.transport).await {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                };
            let bound = listener.endpoint();
            let task = tokio::task::spawn_local(accept_loop(shared.clone(), listener));
            shared
                .tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(task.abort_handle());
            let _ = tx.send(Ok(bound));
        })?;
        Ok(rx.await.map_err(|_| Error::Closed)??)
    }

    /// Publishes a message as the broker itself.
    pub fn publish(
        &self,
        topic: &str,
        payload: impl Into<Bytes>,
        qos: QoS,
        retain: bool,
    ) -> Result<()> {
        validate_name(topic).map_err(|_| Error::InvalidTopic(topic.to_owned()))?;
        if crate::msg::too_deep(topic, self.shared.opts.max_topic_levels) {
            return Err(Error::InvalidTopic(topic.to_owned()));
        }
        let payload: Bytes = payload.into();
        let h = zzq_proto::encode::PublishHeader {
            version: zzq_proto::Version::V5,
            dup: false,
            qos,
            retain,
            topic,
            pkid: 1,
            properties: &[],
        };
        if h.remaining(payload.len()) > zzq_proto::varint::MAX {
            return Err(Error::TooLarge);
        }
        let msg = Msg::new(topic, &payload, &[], qos, retain);
        publish(&self.shared, msg)
    }

    /// Current counters, summed over lanes.
    pub async fn stats(&self) -> Result<Stats> {
        collect(&self.shared).await
    }

    /// Stops the broker and waits until every lane has closed its
    /// listeners and connections. Dropping the broker stops it too, but
    /// without waiting.
    pub async fn shutdown(self) {
        self.abort_tasks();
        let mut done = Vec::with_capacity(self.shared.lanes.len());
        for lane in &self.shared.lanes {
            let (tx, rx) = oneshot::channel();
            if lane.control.send(Control::Shutdown(Some(tx))).is_ok() {
                done.push(rx);
            }
        }
        for rx in done {
            let _ = rx.await;
        }
    }

    fn abort_tasks(&self) {
        for a in self
            .shared
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
        {
            a.abort();
        }
    }
}

/// Counters of all lanes.
pub(crate) async fn collect(shared: &Shared) -> Result<Stats> {
    let mut total = Stats::default();
    for lane in &shared.lanes {
        let (tx, rx) = oneshot::channel();
        lane.control
            .send(Control::Stats(tx))
            .map_err(|_| Error::Closed)?;
        let s: LaneStats = rx.await.map_err(|_| Error::Closed)?;
        total.connections += s.connections;
        total.sessions += s.sessions;
        total.subscriptions = total.subscriptions.max(s.subscriptions);
        total.publishes += s.publishes;
        total.deliveries += s.deliveries;
        total.dropped_buffer_full += s.dropped_buffer_full;
        total.dropped_offline += s.dropped_offline;
        total.dropped_queue_full += s.dropped_queue_full;
        total.dropped_too_large += s.dropped_too_large;
        total.dropped_expired += s.dropped_expired;
    }
    {
        let r = shared
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        total.retained = r.len() as u64;
        total.retained_rejected = r.rejected;
    }
    Ok(total)
}

/// Routes a message from the broker itself on the next lane.
pub(crate) fn publish(shared: &Shared, msg: Msg) -> Result<()> {
    let lane = shared.ctx.next_lane().index();
    shared.lanes[lane]
        .control
        .send(Control::Publish(msg))
        .map_err(|_| Error::Closed)
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.abort_tasks();
        for lane in &self.shared.lanes {
            let _ = lane.control.send(Control::Shutdown(None));
        }
    }
}

fn install(
    id: u16,
    shared: Arc<Shared>,
    mut ctl_rx: mpsc::UnboundedReceiver<Control>,
    mut data_rx: mpsc::Receiver<Vec<Remote>>,
) {
    let broker = shared.id;
    let lane: LaneRef = Rc::new(RefCell::new(Lane::new(id, shared)));
    LANES.with(|l| l.borrow_mut().insert(broker, lane.clone()));

    // Ends the data loop, which would otherwise keep the lane alive through
    // the senders in `Shared`.
    let stopped = Rc::new(Notify::new());
    let l = lane.clone();
    let stop = stopped.clone();
    tokio::task::spawn_local(async move {
        while let Some(c) = ctl_rx.recv().await {
            let last = matches!(c, Control::Shutdown(_));
            l.borrow_mut().control(c);
            ship(&l).await;
            if last {
                LANES.with(|lanes| lanes.borrow_mut().remove(&broker));
                stop.notify_one();
                break;
            }
        }
    });

    let l = lane.clone();
    tokio::task::spawn_local(async move {
        let mut batches = Vec::new();
        loop {
            let n = tokio::select! {
                n = data_rx.recv_many(&mut batches, 64) => n,
                () = stopped.notified() => break,
            };
            if n == 0 {
                break;
            }
            {
                let mut lane = l.borrow_mut();
                if lane.shut_down {
                    break;
                }
                for batch in batches.drain(..) {
                    for r in batch {
                        lane.deliver_remote(&r);
                    }
                }
                lane.flush();
            }
            tokio::task::yield_now().await;
        }
    });

    tokio::task::spawn_local(async move {
        let mut tick = tokio::time::interval(SWEEP);
        loop {
            tick.tick().await;
            {
                let mut l = lane.borrow_mut();
                if l.shut_down {
                    break;
                }
                l.sweep(Instant::now());
                l.flush();
            }
            ship(&lane).await;
        }
    });
}

async fn accept_loop(shared: Arc<Shared>, mut listener: Listener) {
    loop {
        let stream = match listener.accept().await {
            Ok(s) => s,
            Err(zzq_tokio::Error::Closed) => return,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        tokio::task::spawn_local(admit(shared.clone(), stream));
    }
}

/// Binds `udp://` and runs the MQTT-SN gateway on the current lane.
async fn bind_udp(
    shared: Arc<Shared>,
    endpoint: Endpoint,
    tx: oneshot::Sender<zzq_tokio::Result<Endpoint>>,
) {
    let socket = match zzq_tokio::transport::udp::bind(&endpoint).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(e));
            return;
        }
    };
    let bound = match (&endpoint, socket.local_addr()) {
        (Endpoint::Udp { host, .. }, Ok(a)) => Endpoint::Udp {
            host: host.clone(),
            port: a.port(),
        },
        _ => endpoint.clone(),
    };
    let task = tokio::task::spawn_local(crate::sn::gateway(shared.clone(), socket));
    shared
        .tasks
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(task.abort_handle());
    let _ = tx.send(Ok(bound));
}

/// The lane that owns sessions of `client_id`. Every connection of a client
/// ID runs there, so a session never moves between lanes.
pub fn lane_for(client_id: &str, lanes: usize) -> usize {
    use std::hash::{BuildHasher, BuildHasherDefault};
    let hash = BuildHasherDefault::<rustc_hash::FxHasher>::default().hash_one(client_id.as_bytes());
    (hash % lanes.max(1) as u64) as usize
}

/// Reads CONNECT on the accepting lane, then moves the connection to the
/// lane that owns its client ID.
pub(crate) async fn admit(shared: Arc<Shared>, stream: zzq_tokio::transport::Stream) {
    let max = shared.opts.max_packet_size;
    let mut buf = bytes::BytesMut::new();
    let done = |b: &[u8]| !matches!(zzq_proto::decode(b, zzq_proto::Version::V5, max), Ok(None));
    let read =
        tokio::time::timeout(shared.opts.connect_timeout, stream.prefetch(&mut buf, done)).await;
    let Ok(Ok(stream)) = read else {
        return;
    };
    let peer_cert = stream.peer_cert();
    let lanes = shared.ctx.lanes();
    let lane = match zzq_proto::decode(&buf, zzq_proto::Version::V5, max) {
        Ok(Some((zzq_proto::Packet::Connect(c), _))) if !c.client_id.is_empty() => {
            shared.ctx.lane(lane_for(c.client_id, lanes))
        }
        _ => shared.ctx.next_lane(),
    };
    let id = shared.id;
    let _ = lane.spawn(move || async move {
        if let Some(lane) = lane_ref(id) {
            let driver = ConnDriver {
                lane,
                initial: buf,
                peer_cert,
            };
            let _ = stream.drive(driver).await;
        }
    });
}

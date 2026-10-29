//! The broker handle, shared state and lane setup.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use zzq_proto::QoS;
use zzq_proto::topic::validate_name;
use zzq_tokio::{Context, Endpoint};

use crate::disk::DataDir;
use crate::durable::{Cursor, JournalId};
use crate::lane::{Control, Lane, LaneLog, LaneRef, LaneStats, Phase, Remote};
use crate::mailbox::{self, Mailbox};
use crate::msg::Msg;
use crate::options::BrokerOptions;
use crate::retain::Retained;
use crate::{Error, Result};

/// Batches in flight to one lane before senders wait.
const DATA_BATCHES: usize = 64;
/// Controls a lane handles before it lets its connections run.
const CONTROL_RUN: usize = 64;
/// Interval of keep alive and session expiry checks, each over a part of
/// the sessions, so a lane with many does not stall for one long sweep.
const SWEEP: Duration = Duration::from_millis(1000 / crate::lane::SWEEP_PARTS as u64);

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

mod listen;
mod open;

pub(crate) use listen::{admit, listen};
pub(crate) use open::Opened;
use open::{blocking, check, open_data, start};

thread_local! {
    /// Lane state by broker and lane: one thread runs several lanes of a
    /// context made with `Context::local`.
    static LANES: RefCell<HashMap<(u64, u16), LaneRef>> = RefCell::new(HashMap::new());
}

/// Sends batches for other lanes collected on `lane`. Waits while a target
/// lane is full, which slows the caller down instead of dropping.
pub(crate) async fn ship(lane: &LaneRef) -> bool {
    if try_ship(lane) {
        return false;
    }
    // Boxed, so the rare wait does not size every connection's future.
    Box::pin(ship_waiting(lane)).await;
    true
}

/// Ships what goes without waiting. False if a batch is left that must
/// wait: its lane is full, or another task waits to ship.
fn try_ship(lane: &LaneRef) -> bool {
    let mut l = lane.borrow_mut();
    if !l.has_remote() {
        return true;
    }
    let shipping = l.out.shipping.clone();
    let Ok(_turn) = shipping.try_lock() else {
        return false;
    };
    let shared = l.shared.clone();
    while let Some((target, batch)) = l.next_remote() {
        if let Err(batch) = shared.lanes[target].data.try_send(batch) {
            l.unship(target, batch);
            return false;
        }
    }
    true
}

async fn ship_waiting(lane: &LaneRef) {
    let (shared, shipping) = {
        let l = lane.borrow();
        if !l.has_remote() {
            return;
        }
        (l.shared.clone(), l.out.shipping.clone())
    };
    let _turn = shipping.lock().await;
    loop {
        let next = lane.borrow_mut().next_remote();
        let Some((target, batch)) = next else {
            return;
        };
        let _ = shared.lanes[target].data.send(batch).await;
    }
}

/// The state of lane `lane` of broker `id`, on that lane's thread.
pub(crate) fn lane_ref(id: u64, lane: usize) -> Option<LaneRef> {
    let lane = u16::try_from(lane).ok()?;
    LANES.with(|l| l.borrow().get(&(id, lane)).cloned())
}

#[derive(Debug)]
pub(crate) struct LaneTx {
    /// Requests; pushed without waiting.
    pub(crate) control: Mailbox<Control>,
    /// Deliveries routed on other lanes.
    pub(crate) data: Mailbox<Vec<Remote>>,
    /// Bytes the lane's sessions counted against `max_state_bytes` at its
    /// last sweep.
    pub(crate) state: AtomicUsize,
}

/// State shared by all lanes of one broker.
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) id: u64,
    pub(crate) ctx: Context,
    pub(crate) opts: BrokerOptions,
    pub(crate) retained: Retained,
    /// The session journal of each lane.
    pub(crate) lane_logs: Box<[LaneLog]>,
    /// For lane `to`, how far its journal holds the deliveries lane `from`
    /// sent it, at `to * lanes + from`.
    pairs: Box<[Cursor]>,
    /// The disk threads of `opts.data_dir`, after the journals they serve.
    data: Option<DataDir>,
    /// With `opts.cluster`: the groups the journals hand to the replica.
    cluster: Option<Arc<crate::cluster::Groups>>,
    pub(crate) lanes: Box<[LaneTx]>,
    /// Connected clients on all lanes, for `max_connections`.
    pub(crate) connections: AtomicUsize,
    /// Accept loops and other tasks stopped when the broker is dropped;
    /// `None` once stopped.
    tasks: Mutex<Option<Vec<AbortHandle>>>,
}

impl Shared {
    /// The cursor of journal `j`.
    pub(crate) fn cursor(&self, j: JournalId) -> &Cursor {
        match j {
            JournalId::Retained(i) => self.retained.cursor(i),
            JournalId::Lane(i) => &self.lane_logs[i].cursor,
            JournalId::Pair { from, to } => &self.pairs[to * self.lane_logs.len() + from],
        }
    }

    /// Whether the broker journals its state: to a data directory, or to
    /// the cluster.
    pub(crate) const fn journals(&self) -> bool {
        self.data.is_some() || self.cluster.is_some()
    }

    /// Whether the broker is a node of a cluster.
    pub(crate) const fn clustered(&self) -> bool {
        self.cluster.is_some()
    }

    /// The groups the cluster journals hand to the replica. Only called
    /// with a cluster.
    pub(crate) fn cluster_groups(&self) -> Arc<crate::cluster::Groups> {
        self.cluster.clone().unwrap_or_default()
    }

    /// Bytes the state may take: `max_state_bytes`, and no more than the
    /// room on the device of the data directory.
    pub(crate) fn state_limit(&self) -> usize {
        let room = self.data.as_ref().map_or(u64::MAX, DataDir::room);
        self.opts
            .max_state_bytes
            .min(usize::try_from(room).unwrap_or(usize::MAX))
    }

    /// Makes the data directory's device seem to have `free` bytes, or
    /// what it has with `None`.
    #[cfg(test)]
    pub(crate) fn set_free_bytes(&self, free: Option<u64>) {
        if let Some(data) = &self.data {
            data.set_free(free);
        }
    }

    /// Closes the cursors of the lane journals and their pairs: what they
    /// did not write by now they never will.
    fn close_cursors(&self) {
        for log in &self.lane_logs {
            log.cursor.close();
        }
        for pair in &self.pairs {
            pair.close();
        }
    }

    /// Makes the next write of journal `j` fail.
    #[cfg(test)]
    pub(crate) fn fail_next_write(&self, j: JournalId) {
        match j {
            JournalId::Retained(i) => self.retained.fail_next_write(i),
            JournalId::Lane(i) | JournalId::Pair { to: i, .. } => {
                self.lane_logs[i].fail_next_write();
            }
        }
    }

    /// Makes the next segment journal `j` asks for fail, as on a full
    /// device.
    #[cfg(test)]
    pub(crate) fn fail_next_segment(&self, j: JournalId) {
        match j {
            JournalId::Retained(i) => self.retained.fail_next_segment(i),
            JournalId::Lane(i) | JournalId::Pair { to: i, .. } => {
                self.lane_logs[i].fail_next_segment();
            }
        }
    }

    /// Stops `task` with the broker, or now if the broker stopped already,
    /// as a bind cancelled before its listener was up may find it.
    pub(crate) fn own_task(&self, task: AbortHandle) {
        match &mut *self.tasks.lock().unwrap_or_else(PoisonError::into_inner) {
            Some(tasks) => {
                // Listeners come and go with a cluster's leadership.
                tasks.retain(|t| !t.is_finished());
                tasks.push(task);
            }
            None => task.abort(),
        }
    }
}

/// Broker counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Connected clients.
    pub connections: u64,
    /// Sessions, connected or not.
    pub sessions: u64,
    /// Subscriptions in the index of one lane (all lanes hold all of them).
    pub subscriptions: u64,
    /// Retained messages stored.
    pub retained: u64,
    /// Messages routed.
    pub publishes: u64,
    /// Messages written to subscribers.
    pub deliveries: u64,
    /// Messages dropped, by reason.
    pub dropped: Dropped,
    /// Retained messages not stored because `max_retained` or
    /// `max_state_bytes` was reached.
    pub retained_rejected: u64,
    /// Approximate bytes of state counted against `max_state_bytes`.
    pub state_bytes: u64,
    /// Writes to the data directory that failed. Their v5 publishers got
    /// Unspecified error, and v3.1.1 ones were disconnected. A journal that
    /// could not make a new segment counts each try.
    pub write_errors: u64,
    /// Journals in the data directory that could not make a new segment,
    /// as on a full device. Their writes go on in the spare segment each
    /// keeps back, and wait once that is full too, until space comes back.
    pub full_journals: u64,
    /// Bytes the state may take before it stops growing:
    /// `max_state_bytes`, or less while the device of the data directory
    /// has less room.
    pub state_limit: u64,
}

/// Messages the broker dropped, by reason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Dropped {
    /// QoS 0 messages dropped because a connection's write buffer was full.
    pub buffer_full: u64,
    /// QoS 0 messages for offline or ended sessions.
    pub offline: u64,
    /// QoS 1/2 messages dropped from a full session queue.
    pub queue_full: u64,
    /// Messages larger than the subscriber's Maximum Packet Size.
    pub too_large: u64,
    /// Queued messages whose Message Expiry Interval ran out.
    pub expired: u64,
    /// QoS 1/2 messages a session still held, queued or unacked, when it
    /// ended: by expiry, a clean start, or DISCONNECT with expiry 0. Also
    /// those a share group sent to a member on another lane whose session
    /// ended on the way.
    pub session_ended: u64,
    /// QoS 1/2 messages dropped instead of queued because
    /// `max_state_bytes` was reached.
    pub state_full: u64,
}

impl Dropped {
    /// All dropped messages.
    pub const fn total(&self) -> u64 {
        self.buffer_full
            + self.offline
            + self.queue_full
            + self.too_large
            + self.expired
            + self.session_ended
            + self.state_full
    }
}

impl std::ops::AddAssign for Dropped {
    fn add_assign(&mut self, o: Dropped) {
        self.buffer_full += o.buffer_full;
        self.offline += o.offline;
        self.queue_full += o.queue_full;
        self.too_large += o.too_large;
        self.expired += o.expired;
        self.session_ended += o.session_ended;
        self.state_full += o.state_full;
    }
}

/// An MQTT broker. Dropping it stops listening and closes all connections.
#[derive(Debug)]
pub struct Broker {
    shared: Arc<Shared>,
}

impl Broker {
    /// Starts a broker on the lanes of `ctx`.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidOption`] for a zero `sys_interval`, `sn.retry`,
    /// `sn.max_keep_alive` or `receive_maximum`, [`Error::DataDir`] if the
    /// data directory cannot be opened or read, or the system is not
    /// Linux, [`Error::State`] if the state file cannot be read, and
    /// [`Error::Transport`] if the lanes have stopped.
    pub async fn new(ctx: &Context, opts: BrokerOptions) -> Result<Broker> {
        check(&opts)?;
        let n = ctx.lanes();
        let mut lanes = Vec::with_capacity(n);
        let mut receivers = Vec::with_capacity(n);
        for _ in 0..n {
            let (ctl_tx, ctl_rx) = mailbox::mailbox(usize::MAX);
            let (data_tx, data_rx) = mailbox::mailbox(DATA_BATCHES);
            lanes.push(LaneTx {
                control: ctl_tx,
                data: data_tx,
                state: AtomicUsize::new(0),
            });
            receivers.push((ctl_rx, data_rx));
        }
        let Opened {
            retained,
            data,
            cluster,
            lane_logs,
            sessions,
        } = open_data(&opts, n).await?;
        let shared = Arc::new(Shared {
            // Relaxed: only unique, no data rides on it.
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            ctx: ctx.clone(),
            opts,
            retained,
            lane_logs,
            pairs: (0..n * n).map(|_| Cursor::default()).collect(),
            data,
            cluster,
            lanes: lanes.into_boxed_slice(),
            connections: AtomicUsize::new(0),
            tasks: Mutex::new(Some(Vec::new())),
        });
        // Stops the lanes again when a later step fails.
        let broker = Broker {
            shared: shared.clone(),
        };
        for (i, (ctl_rx, data_rx)) in receivers.into_iter().enumerate() {
            let s = shared.clone();
            ctx.lane(i)
                .call(move || install(i as u16, s, ctl_rx, data_rx))
                .await?;
        }
        start(&shared, sessions).await?;
        Ok(broker)
    }

    /// The broker's node of a cluster, as its replica last saw it. Engine
    /// API for the simulator.
    #[doc(hidden)]
    pub fn cluster_status(&self) -> Option<zzq_cluster::node::NodeStatus> {
        let groups = self.shared.cluster.as_ref()?;
        *groups.status.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The ID that tells this broker's lane state apart on a lane thread.
    #[cfg(test)]
    pub(crate) fn id(&self) -> u64 {
        self.shared.id
    }

    #[cfg(test)]
    pub(crate) fn shared(&self) -> &Shared {
        &self.shared
    }

    /// Binds an endpoint such as `tcp://0.0.0.0:1883` and returns the bound
    /// endpoint, with the actual port for port 0.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] if the URI is invalid or binding fails, for
    /// example because the address is in use.
    ///
    /// A node of a cluster listens only while it leads: it binds `uri`
    /// then, and closes its listeners and connections when it no longer
    /// does. It returns `uri` as given.
    pub async fn bind(&self, uri: &str) -> Result<Endpoint> {
        let endpoint = uri.parse::<Endpoint>()?;
        if self.shared.cluster.is_some() {
            return crate::cluster::bind(&self.shared, endpoint).await;
        }
        Ok(listen(&self.shared, endpoint).await?.0)
    }

    /// Publishes a message as the broker itself. Messages to one topic
    /// reach subscribers in the order they were published.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidTopic`] for a topic clients could not publish to,
    /// [`Error::TooLarge`] for a message MQTT cannot carry, and
    /// [`Error::Closed`] once the broker has shut down.
    pub fn publish(&self, topic: &str, payload: impl AsRef<[u8]>, qos: QoS) -> Result<()> {
        self.send_publish(topic, payload.as_ref(), qos, false)
    }

    /// Publishes a retained message as the broker itself. An empty payload
    /// clears the topic.
    ///
    /// # Errors
    ///
    /// As [`Broker::publish`].
    pub fn publish_retained(&self, topic: &str, payload: impl AsRef<[u8]>, qos: QoS) -> Result<()> {
        self.send_publish(topic, payload.as_ref(), qos, true)
    }

    fn send_publish(&self, topic: &str, payload: &[u8], qos: QoS, retain: bool) -> Result<()> {
        validate_name(topic).map_err(|_| Error::InvalidTopic(topic.to_owned()))?;
        if crate::msg::too_deep(topic, self.shared.opts.max_topic_levels) {
            return Err(Error::InvalidTopic(topic.to_owned()));
        }
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
        let msg = Msg::new(topic, payload, &[], qos, retain);
        publish(&self.shared, msg)
    }

    /// Current counters, summed over lanes.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the broker has stopped.
    pub async fn stats(&self) -> Result<Stats> {
        collect(&self.shared).await
    }

    /// Stops the broker and waits until every lane has closed its
    /// listeners and connections. With [`BrokerOptions::state_file`], it
    /// first closes every connection as lost, so wills go out as usual,
    /// and writes the sessions there. Dropping the broker stops it too,
    /// without waiting or writing state.
    ///
    /// # Errors
    ///
    /// [`Error::State`] if the state file could not be written, and
    /// [`Error::Closed`] if a lane stopped before it could. The broker stops
    /// anyway.
    pub async fn shutdown(self) -> Result<()> {
        let path = self.shared.opts.state_file.as_deref();
        // A node of a cluster dumps what its replica holds, which runs
        // until then.
        let saved = if self.shared.clustered() {
            crate::cluster::save(&self.shared, path).await
        } else {
            self.abort_tasks();
            match path {
                Some(path) => crate::dump::save(&self.shared, path).await,
                // Connections end as lost: journals get their ends, and
                // wills go out once.
                None if self.shared.data.is_some() => crate::dump::quiesce(&self.shared).await,
                None => Ok(()),
            }
        };
        self.abort_tasks();
        let mut done = Vec::with_capacity(self.shared.lanes.len());
        for lane in &self.shared.lanes {
            let (tx, rx) = oneshot::channel();
            if lane.control.push(Control::Shutdown(Some(tx))).is_ok() {
                done.push(rx);
            }
        }
        for rx in done {
            let _ = rx.await;
        }
        // Changes up to here, such as wills, reach the disk, and the data
        // directory is free for the next broker.
        if self.shared.data.is_some() {
            let shared = self.shared.clone();
            let _ = blocking(move || {
                for log in &shared.lane_logs {
                    log.close();
                }
                shared.close_cursors();
                shared.retained.close();
            })
            .await;
        }
        saved
    }

    pub(crate) fn abort_tasks(&self) {
        let tasks = self
            .shared
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        for a in tasks.into_iter().flatten() {
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
            .push(Control::Stats(tx))
            .map_err(|_| Error::Closed)?;
        let s: LaneStats = rx.await.map_err(|_| Error::Closed)?;
        total.connections += s.connections;
        total.sessions += s.sessions;
        total.subscriptions = total.subscriptions.max(s.subscriptions);
        total.publishes += s.publishes;
        total.deliveries += s.deliveries;
        total.dropped += s.dropped;
        total.state_bytes += s.state_bytes;
    }
    let r = &shared.retained;
    total.retained = r.len() as u64;
    total.retained_rejected = r.rejected();
    total.write_errors = r.write_errors()
        + shared
            .lane_logs
            .iter()
            .map(|l| l.write_errors.load(Ordering::Relaxed))
            .sum::<u64>();
    total.full_journals =
        r.full_journals() + shared.lane_logs.iter().filter(|l| l.full()).count() as u64;
    total.state_bytes += r.bytes() as u64;
    total.state_limit = u64::try_from(shared.state_limit()).unwrap_or(u64::MAX);
    Ok(total)
}

/// Routes a message from the broker itself. Messages to one topic go
/// through one lane, so they keep their order.
pub(crate) fn publish(shared: &Shared, msg: Msg) -> Result<()> {
    let lane = lane_for(msg.topic(), shared.lanes.len());
    shared.lanes[lane]
        .control
        .push(Control::Publish(msg))
        .map_err(|_| Error::Closed)
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.abort_tasks();
        for lane in &self.shared.lanes {
            let _ = lane.control.push(Control::Shutdown(None));
        }
        // The data directory is free for another broker once the writes
        // and trims under way are done; one opened sooner waits for them.
        for log in &self.shared.lane_logs {
            log.release();
        }
        self.shared.close_cursors();
        self.shared.retained.release();
    }
}

fn install(
    id: u16,
    shared: Arc<Shared>,
    ctl_rx: mailbox::Receiver<Control>,
    data_rx: mailbox::Receiver<Vec<Remote>>,
) {
    let broker = shared.id;
    let lane: LaneRef = Rc::new(RefCell::new(Lane::new(id, shared.clone())));
    LANES.with(|l| l.borrow_mut().insert((broker, id), lane.clone()));
    if shared.journals() {
        let task = tokio::task::spawn_local(crate::lane::complete(lane.clone()));
        shared.own_task(task.abort_handle());
    }

    let l = lane.clone();
    tokio::task::spawn_local(async move {
        let mut controls = Vec::new();
        while ctl_rx.recv_all(&mut controls).await {
            for (i, c) in controls.drain(..).enumerate() {
                let last = matches!(c, Control::Shutdown(_));
                l.borrow_mut().control(c);
                ship(&l).await;
                if i % CONTROL_RUN == CONTROL_RUN - 1 {
                    tokio::task::yield_now().await;
                }
                if last {
                    LANES.with(|lanes| lanes.borrow_mut().remove(&(broker, id)));
                    // Ends the data loop, which would otherwise keep the
                    // lane alive through the senders in `Shared`.
                    shared.lanes[usize::from(id)].data.close();
                    return;
                }
            }
        }
    });

    // A run takes at most `DATA_BATCHES`, each at most one read per
    // connection of the sending lane, then lets this lane's connections
    // run. A byte cap on top cost cross-lane QoS 0 fan-in up to 25%.
    let l = lane.clone();
    tokio::task::spawn_local(async move {
        let mut batches = Vec::new();
        while data_rx.recv_all(&mut batches).await {
            {
                let mut lane = l.borrow_mut();
                if lane.phase == Phase::ShutDown {
                    break;
                }
                for batch in batches.drain(..) {
                    for r in batch {
                        lane.deliver_remote(r);
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
                if l.phase == Phase::ShutDown {
                    break;
                }
                l.sweep(crate::time::now());
                l.flush();
            }
            ship(&lane).await;
        }
    });
}

/// The lane that owns sessions of `client_id`. Every connection of a client
/// ID runs there, so a session never moves between lanes.
#[doc(hidden)]
pub fn lane_for(client_id: &str, lanes: usize) -> usize {
    use std::hash::{BuildHasher, BuildHasherDefault};
    let hash = BuildHasherDefault::<rustc_hash::FxHasher>::default().hash_one(client_id.as_bytes());
    (hash % lanes.max(1) as u64) as usize
}

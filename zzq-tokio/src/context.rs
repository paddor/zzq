//! IO lanes.

use std::fmt;
use std::io;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{self, Poll, Wake, Waker};
use std::thread::{self, JoinHandle, ThreadId};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::signal::DataSignal;
use crate::transport::inproc::Registry;
use crate::{Error, Result};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Context settings.
#[derive(Debug, Clone)]
pub struct Config {
    /// Number of IO lanes, each on its own thread. 0 counts as 1.
    pub io_threads: usize,
    /// Thread name prefix.
    pub name: String,
    /// Lanes poll for IO without sleeping: lower latency, at the cost of
    /// one busy core per lane.
    pub busy_poll: bool,
    /// After its tasks ran, a lane polls on for this long before it sleeps,
    /// so a wake from another thread finds it awake. Costs CPU while
    /// traffic is sparse. Zero sleeps at once.
    pub spin: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            io_threads: 1,
            name: "zzq".to_owned(),
            busy_poll: false,
            spin: Duration::from_micros(40),
        }
    }
}

/// Handle to run work on one lane.
#[derive(Clone)]
#[doc(hidden)]
pub struct LaneHandle {
    index: usize,
    tx: mpsc::UnboundedSender<Job>,
}

impl fmt::Debug for LaneHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LaneHandle")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl LaneHandle {
    /// Lane index within its context.
    pub const fn index(&self) -> usize {
        self.index
    }

    /// Runs `job` on the lane thread, inside its local task set.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the lane has stopped.
    pub fn run(&self, job: impl FnOnce() + Send + 'static) -> Result<()> {
        self.tx.send(Box::new(job)).map_err(|_| Error::Closed)
    }

    /// Spawns a local task on the lane. `f` builds the future on the lane, so
    /// the future does not need to be `Send`.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the lane has stopped.
    pub fn spawn<F, Fut>(&self, f: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        self.run(move || {
            tokio::task::spawn_local(f());
        })
    }

    /// Runs `f` on the lane and returns its result.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the lane has stopped.
    pub async fn call<R, F>(&self, f: F) -> Result<R>
    where
        R: Send + 'static,
        F: FnOnce() -> R + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.run(move || {
            let _ = tx.send(f());
        })?;
        rx.await.map_err(|_| Error::Closed)
    }
}

struct Lane {
    handle: LaneHandle,
    stop: Arc<DataSignal>,
}

struct Inner {
    lanes: Box<[Lane]>,
    next: AtomicUsize,
    threads: Mutex<Vec<(ThreadId, JoinHandle<()>)>>,
    inproc: Arc<Registry>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        for lane in &self.lanes {
            lane.stop.mark();
        }
        let me = thread::current().id();
        let threads = std::mem::take(
            self.threads
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for (id, handle) in threads {
            if id != me {
                let _ = handle.join();
            }
        }
    }
}

/// Owns the IO lanes. Cheap to clone; lanes stop when the last clone drops.
#[derive(Clone)]
pub struct Context {
    inner: Arc<Inner>,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("lanes", &self.inner.lanes.len())
            .finish_non_exhaustive()
    }
}

impl Context {
    /// A context with one lane.
    ///
    /// # Errors
    ///
    /// As [`Context::with_config`].
    pub fn new() -> Result<Context> {
        Self::with_config(&Config::default())
    }

    /// A context with the given settings.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if a lane thread or its runtime cannot start.
    pub fn with_config(config: &Config) -> Result<Context> {
        let n = config.io_threads.max(1);
        let mut lanes = Vec::with_capacity(n);
        let mut threads = Vec::with_capacity(n);
        for index in 0..n {
            // Unbounded: jobs come from this process, such as an accepted
            // connection, which holds its socket, so open files bound them.
            let (tx, rx) = mpsc::unbounded_channel();
            let stop = Arc::new(DataSignal::new());
            let stop2 = stop.clone();
            let (busy, spin) = (config.busy_poll, config.spin);
            // Built here, so a failure to build reaches the caller.
            let rt = LaneRuntime(Some(runtime(busy)?));
            let handle = thread::Builder::new()
                .name(format!("{}-io-{index}", config.name))
                .spawn(move || {
                    if let Some(rt) = &rt.0 {
                        lane_main(rt, rx, &stop2, busy, spin);
                    }
                })?;
            threads.push((handle.thread().id(), handle));
            lanes.push(Lane {
                handle: LaneHandle { index, tx },
                stop,
            });
        }
        Ok(Context {
            inner: Arc::new(Inner {
                lanes: lanes.into_boxed_slice(),
                next: AtomicUsize::new(0),
                threads: Mutex::new(threads),
                inproc: Arc::default(),
            }),
        })
    }

    /// A context whose `lanes` lanes are tasks on the caller's local task
    /// set, not threads, for a simulation that runs everything on one
    /// thread under one clock. Call it inside a `LocalSet`; lanes run as it
    /// polls them. Busy polling and spin do not apply.
    #[doc(hidden)]
    pub fn local(lanes: usize) -> Context {
        let lanes = (0..lanes.max(1))
            .map(|index| {
                let (tx, mut rx) = mpsc::unbounded_channel::<Job>();
                let stop = Arc::new(DataSignal::new());
                let stop2 = stop.clone();
                tokio::task::spawn_local(async move {
                    loop {
                        tokio::select! {
                            biased;
                            () = stop2.ready() => break,
                            job = rx.recv() => match job {
                                Some(job) => job(),
                                None => break,
                            },
                        }
                    }
                });
                Lane {
                    handle: LaneHandle { index, tx },
                    stop,
                }
            })
            .collect();
        Context {
            inner: Arc::new(Inner {
                lanes,
                next: AtomicUsize::new(0),
                threads: Mutex::new(Vec::new()),
                inproc: Arc::default(),
            }),
        }
    }

    /// Number of lanes.
    pub fn lanes(&self) -> usize {
        self.inner.lanes.len()
    }

    /// The lane with the given index, wrapping around.
    #[doc(hidden)]
    pub fn lane(&self, index: usize) -> &LaneHandle {
        &self.inner.lanes[index % self.inner.lanes.len()].handle
    }

    /// The next lane in round-robin order.
    #[doc(hidden)]
    pub fn next_lane(&self) -> &LaneHandle {
        // Relaxed: only spreads work, no data rides on it.
        self.lane(self.inner.next.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn inproc(&self) -> &Arc<Registry> {
        &self.inner.inproc
    }

    /// Stops lane `index`, as a lane that failed would be.
    #[cfg(test)]
    pub(crate) fn stop_lane(&self, index: usize) {
        self.inner.lanes[index].stop.mark();
    }
}

/// A lane's runtime. Dropping it does not wait for blocking work, such as
/// the DNS lookup of a connect, and does not panic in an async context, as
/// on the caller's thread when a lane thread fails to start.
struct LaneRuntime(Option<tokio::runtime::Runtime>);

impl Drop for LaneRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.0.take() {
            rt.shutdown_background();
        }
    }
}

/// A lane's runtime. A busy lane checks for IO after every task.
fn runtime(busy: bool) -> io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all();
    if busy {
        builder.event_interval(1);
    }
    builder.build()
}

fn lane_main(
    rt: &tokio::runtime::Runtime,
    mut rx: mpsc::UnboundedReceiver<Job>,
    stop: &DataSignal,
    busy: bool,
    spin: Duration,
) {
    let local = tokio::task::LocalSet::new();
    let lane = pin!(local.run_until(async move {
        if busy {
            // Always runnable, so the runtime polls IO without parking.
            tokio::task::spawn_local(async {
                loop {
                    tokio::task::yield_now().await;
                }
            });
        }
        loop {
            tokio::select! {
                biased;
                () = stop.ready() => break,
                job = rx.recv() => match job {
                    Some(job) => job(),
                    None => break,
                },
            }
        }
    }));
    if busy || spin.is_zero() {
        rt.block_on(lane);
    } else {
        rt.block_on(Spin {
            lane,
            window: spin,
            until: Instant::now(),
            activity: None,
        });
    }
}

/// Runs a lane, polling on for `window` after its tasks last ran instead of
/// sleeping at once.
struct Spin<'a, F> {
    lane: Pin<&'a mut F>,
    window: Duration,
    until: Instant,
    /// Wraps the runtime's waker. Built on the first poll.
    activity: Option<(Arc<Activity>, Waker)>,
}

/// Notes that the lane's tasks were woken, then wakes the runtime.
struct Activity {
    woken: AtomicBool,
    runtime: Waker,
}

impl Wake for Activity {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Relaxed: a hint for the spin window; the runtime wake below
        // orders the work itself.
        self.woken.store(true, Ordering::Relaxed);
        self.runtime.wake_by_ref();
    }
}

impl<F: Future> Future for Spin<'_, F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<F::Output> {
        let this = &mut *self;
        // The runtime's waker stays the same for the whole `block_on`.
        let (activity, waker) = this.activity.get_or_insert_with(|| {
            let a = Arc::new(Activity {
                woken: AtomicBool::new(false),
                runtime: cx.waker().clone(),
            });
            (a.clone(), Waker::from(a))
        });
        // Relaxed: see `Activity::wake_by_ref`.
        let woken = activity.woken.swap(false, Ordering::Relaxed);
        if let Poll::Ready(out) = this
            .lane
            .as_mut()
            .poll(&mut task::Context::from_waker(waker))
        {
            return Poll::Ready(out);
        }
        let now = Instant::now();
        if woken {
            this.until = now + this.window;
        }
        if now < this.until {
            // Stays runnable, so the runtime polls IO without sleeping. A
            // wake from within the runtime costs no syscall.
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn local_lanes_run_on_the_callers_thread_under_its_clock() {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let ctx = Context::local(2);
                assert_eq!(ctx.lanes(), 2);
                let me = thread::current().id();
                let there = ctx.lane(1).call(|| thread::current().id()).await.unwrap();
                assert_eq!(there, me);
                let (tx, rx) = oneshot::channel();
                ctx.lane(0)
                    .spawn(move || async move {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        let _ = tx.send(());
                    })
                    .unwrap();
                let start = tokio::time::Instant::now();
                rx.await.unwrap();
                assert_eq!(start.elapsed(), Duration::from_secs(3600), "virtual time");
                drop(ctx);
            })
            .await;
    }

    #[tokio::test]
    async fn runs_on_lanes() {
        let ctx = Context::with_config(&Config {
            io_threads: 2,
            ..Config::default()
        })
        .unwrap();
        let a = ctx
            .lane(0)
            .call(|| thread::current().name().map(str::to_owned))
            .await
            .unwrap();
        let b = ctx
            .lane(1)
            .call(|| thread::current().name().map(str::to_owned))
            .await
            .unwrap();
        assert_eq!(a.as_deref(), Some("zzq-io-0"));
        assert_eq!(b.as_deref(), Some("zzq-io-1"));
        let (tx, rx) = oneshot::channel();
        ctx.next_lane()
            .spawn(move || async move {
                tokio::task::yield_now().await;
                let _ = tx.send(7);
            })
            .unwrap();
        assert_eq!(rx.await.unwrap(), 7);
    }

    #[tokio::test]
    async fn busy_lanes_run_timers_and_stop() {
        let ctx = Context::with_config(&Config {
            busy_poll: true,
            ..Config::default()
        })
        .unwrap();
        let (tx, rx) = oneshot::channel();
        ctx.next_lane()
            .spawn(move || async move {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                let _ = tx.send(());
            })
            .unwrap();
        rx.await.unwrap();
        drop(ctx);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_spinning_lane_sleeps_once_idle() {
        // CPU time of a thread, in clock ticks.
        fn cpu(task: &str) -> u64 {
            let stat = std::fs::read_to_string(format!("/proc/self/task/{task}/stat")).unwrap();
            let fields: Vec<&str> = stat.rsplit_once(") ").unwrap().1.split(' ').collect();
            fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
        }

        let ctx = Context::with_config(&Config {
            spin: Duration::from_millis(50),
            ..Config::default()
        })
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Wakes the parked lane, which then spins.
        let task = ctx
            .lane(0)
            .call(|| std::fs::read_link("/proc/thread-self").unwrap())
            .await
            .unwrap();
        let task = task.file_name().unwrap().to_str().unwrap().to_owned();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let before = cpu(&task);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(cpu(&task) - before <= 3, "idle lane still spins");
    }

    #[test]
    fn dropping_the_context_does_not_wait_for_blocking_work() {
        let ctx = Context::new().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        ctx.lane(0)
            .spawn(move || async move {
                // As a DNS lookup of a connect does.
                tokio::task::spawn_blocking(move || {
                    let _ = tx.send(());
                    thread::sleep(Duration::from_secs(5));
                });
            })
            .unwrap();
        rx.recv().unwrap();
        let started = std::time::Instant::now();
        drop(ctx);
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[tokio::test]
    async fn a_lane_runtime_drops_in_an_async_context() {
        // As when its thread fails to start, on the caller's runtime.
        drop(LaneRuntime(Some(runtime(false).unwrap())));
    }
}

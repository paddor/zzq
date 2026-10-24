//! IO lanes.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle, ThreadId};

use tokio::sync::{Notify, mpsc, oneshot};

use crate::transport::inproc::Registry;
use crate::{Error, Result};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// Context settings.
#[derive(Debug, Clone)]
pub struct Config {
    /// Number of IO lanes, each on its own thread.
    pub io_threads: usize,
    /// Thread name prefix.
    pub name: String,
    /// Lanes poll for IO without sleeping: lower latency, at the cost of
    /// one busy core per lane.
    pub busy_poll: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            io_threads: 1,
            name: "zzq".to_owned(),
            busy_poll: false,
        }
    }
}

/// Handle to run work on one lane.
#[derive(Clone)]
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
    pub fn run(&self, job: impl FnOnce() + Send + 'static) -> Result<()> {
        self.tx.send(Box::new(job)).map_err(|_| Error::Closed)
    }

    /// Spawns a local task on the lane. `f` builds the future on the lane, so
    /// the future does not need to be `Send`.
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
    stop: Arc<Notify>,
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
            lane.stop.notify_one();
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
    pub fn new() -> Result<Context> {
        Self::with_config(&Config::default())
    }

    /// A context with the given settings.
    pub fn with_config(config: &Config) -> Result<Context> {
        let n = config.io_threads.max(1);
        let mut lanes = Vec::with_capacity(n);
        let mut threads = Vec::with_capacity(n);
        for index in 0..n {
            let (tx, rx) = mpsc::unbounded_channel();
            let stop = Arc::new(Notify::new());
            let stop2 = stop.clone();
            let busy = config.busy_poll;
            let handle = thread::Builder::new()
                .name(format!("{}-io-{index}", config.name))
                .spawn(move || lane_main(rx, &stop2, busy))?;
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

    /// Number of lanes.
    pub fn lanes(&self) -> usize {
        self.inner.lanes.len()
    }

    /// The lane with the given index, wrapping around.
    pub fn lane(&self, index: usize) -> &LaneHandle {
        &self.inner.lanes[index % self.inner.lanes.len()].handle
    }

    /// The next lane in round-robin order.
    pub fn next_lane(&self) -> &LaneHandle {
        self.lane(self.inner.next.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn inproc(&self) -> &Arc<Registry> {
        &self.inner.inproc
    }
}

fn lane_main(mut rx: mpsc::UnboundedReceiver<Job>, stop: &Notify, busy: bool) {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_all();
    if busy {
        builder.event_interval(1);
    }
    let Ok(rt) = builder.build() else {
        return;
    };
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async move {
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
                () = stop.notified() => break,
                job = rx.recv() => match job {
                    Some(job) => job(),
                    None => break,
                },
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

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
}

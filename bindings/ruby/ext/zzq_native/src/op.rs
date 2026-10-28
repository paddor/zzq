//! `ZZQ::Native::Op`: a Rust future that outlives one native call, such as
//! a connect or the wait for a PUBACK. Ruby polls it with a fiber's
//! [`Wake`](crate::wake::Wake) until it is done.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::task::Poll;

use rb_sys::VALUE;
use zzq_client::{Acked, ReasonCode};

use crate::error::Error;
use crate::rb::{self, DataType, RbResult};
use crate::{broker, client, wake};

static TYPE: DataType = DataType::new::<Op>(c"ZZQ::Native::Op");
static CLASS: OnceLock<usize> = OnceLock::new();
static PENDING: OnceLock<usize> = OnceLock::new();

/// What a finished op returns to Ruby.
pub enum Out {
    Nil,
    Codes(Vec<ReasonCode>),
    Client(zzq_client::Client),
    Broker(zzq_broker::Broker),
    Endpoint(String),
    Stats(zzq_broker::Stats),
}

pub type Fut = Pin<Box<dyn Future<Output = Result<Out, Error>> + Send>>;

enum Task {
    Acked(Acked),
    Fut(Fut),
}

/// A future Ruby polls until it is done.
pub struct Op {
    task: Mutex<Option<Task>>,
}

impl std::fmt::Debug for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Op").finish_non_exhaustive()
    }
}

/// `ZZQ::Native::PENDING`: a poll found the op not done.
pub fn pending() -> VALUE {
    *PENDING.get().expect("registered") as VALUE
}

/// A new op for `fut`.
pub fn new(fut: impl Future<Output = Result<Out, Error>> + Send + 'static) -> RbResult<VALUE> {
    wrap(Task::Fut(Box::pin(fut)))
}

/// A new op for the ack of a queued publish.
pub fn acked(acked: Acked) -> RbResult<VALUE> {
    wrap(Task::Acked(acked))
}

fn wrap(task: Task) -> RbResult<VALUE> {
    let op = Op {
        task: Mutex::new(Some(task)),
    };
    TYPE.wrap(*CLASS.get().expect("registered") as VALUE, op)
}

fn get(value: VALUE) -> RbResult<&'static Op> {
    unsafe { TYPE.get(value) }
}

fn poll(this: VALUE, wake: VALUE) -> RbResult<VALUE> {
    let op = get(this)?;
    let wake = wake::get(wake)?;
    let mut task = op.task.lock().unwrap_or_else(PoisonError::into_inner);
    let res = match task.as_mut() {
        Some(Task::Acked(acked)) => match wake.poll(Pin::new(acked)) {
            Poll::Ready(res) => res.map(|()| Out::Nil).map_err(Error::from),
            Poll::Pending => return Ok(pending()),
        },
        Some(Task::Fut(fut)) => match wake.poll(fut.as_mut()) {
            Poll::Ready(res) => res,
            Poll::Pending => return Ok(pending()),
        },
        None => return Err(Error::Client(zzq_client::Error::Closed).into()),
    };
    *task = None;
    drop(task);
    to_ruby(res?)
}

fn to_ruby(out: Out) -> RbResult<VALUE> {
    match out {
        Out::Nil => Ok(rb::qnil()),
        Out::Codes(codes) => {
            let array = rb::array_new_capa(codes.len())?;
            for code in codes {
                rb::array_push(array, rb::u64_value(u8::from(code).into()))?;
            }
            Ok(array)
        }
        Out::Client(c) => client::wrap(c),
        Out::Broker(b) => broker::wrap(b),
        Out::Endpoint(e) => rb::new_utf8_string(&e),
        Out::Stats(s) => broker::stats_hash(&s),
    }
}

unsafe extern "C" fn op_poll(this: VALUE, wake: VALUE) -> VALUE {
    rb::wrap(|| poll(this, wake))
}

unsafe extern "C" fn op_cancel(this: VALUE) -> VALUE {
    rb::wrap(|| {
        let task = get(this)?
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        drop(task);
        Ok(rb::qnil())
    })
}

pub fn register(native: VALUE) -> RbResult<()> {
    let class = rb::define_class_under(native, c"Op")?;
    let _ = CLASS.set(class as usize);
    rb::define_method(class, c"poll", op_poll as *const (), 1)?;
    rb::define_method(class, c"cancel", op_cancel as *const (), 0)?;
    let pending = rb::freeze(rb::protect(|| unsafe {
        rb_sys::rb_obj_alloc(rb_sys::rb_cObject)
    })?);
    rb::define_const(native, c"PENDING", pending)?;
    let _ = PENDING.set(pending as usize);
    Ok(())
}

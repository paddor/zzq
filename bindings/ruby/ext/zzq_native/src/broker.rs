//! `ZZQ::Native::Broker`: a running broker.

use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use rb_sys::VALUE;
use zzq_broker::{Broker, FilePersistence, QoS, Stats};

use crate::error::Error;
use crate::op::{self, Out};
use crate::rb::{self, DataType, RbResult, RubyErr};
use crate::{context, options};

static TYPE: DataType = DataType::new::<NativeBroker>(c"ZZQ::Native::Broker");
static CLASS: OnceLock<usize> = OnceLock::new();

#[derive(Debug)]
struct NativeBroker {
    /// `None` once shut down.
    broker: Mutex<Option<Arc<Broker>>>,
    /// Flushed after a shutdown, so a broker started next on the same
    /// file finds every retained message.
    store: Option<Arc<FilePersistence>>,
}

fn get(value: VALUE) -> RbResult<Arc<Broker>> {
    let native: &NativeBroker = unsafe { TYPE.get(value) }?;
    native
        .broker
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        .ok_or_else(|| Error::Broker(zzq_broker::Error::Closed).into())
}

/// A Ruby object for a running broker.
pub fn wrap(b: Broker, store: Option<Arc<FilePersistence>>) -> RbResult<VALUE> {
    let native = NativeBroker {
        broker: Mutex::new(Some(Arc::new(b))),
        store,
    };
    TYPE.wrap(*CLASS.get().expect("registered") as VALUE, native)
}

pub fn stats_hash(s: &Stats) -> RbResult<VALUE> {
    let h = rb::hash_new()?;
    for (key, value) in [
        ("connections", s.connections),
        ("sessions", s.sessions),
        ("subscriptions", s.subscriptions),
        ("retained", s.retained),
        ("publishes", s.publishes),
        ("deliveries", s.deliveries),
        ("dropped_buffer_full", s.dropped_buffer_full),
        ("dropped_offline", s.dropped_offline),
        ("dropped_queue_full", s.dropped_queue_full),
        ("dropped_too_large", s.dropped_too_large),
        ("dropped_expired", s.dropped_expired),
        ("retained_rejected", s.retained_rejected),
        // TODO: add `dropped_session_ended` once Cargo.toml requires the
        // next zzq-broker release, the first one that has it.
    ] {
        rb::hash_aset(h, rb::symbol(key)?, rb::u64_value(value))?;
    }
    Ok(h)
}

fn start(opts: VALUE) -> RbResult<VALUE> {
    let (opts, store) = options::broker(opts)?;
    let ctx = context::get()?;
    op::new(async move { Ok(Out::Broker(Broker::new(&ctx, opts).await?, store)) })
}

fn bind(this: VALUE, uri: VALUE) -> RbResult<VALUE> {
    let b = get(this)?;
    let uri = rb::value_to_string(uri)?;
    op::new(async move { Ok(Out::Endpoint(b.bind(&uri).await?.to_string())) })
}

fn publish(
    this: VALUE,
    topic: VALUE,
    payload: VALUE,
    qos: VALUE,
    retain: VALUE,
) -> RbResult<VALUE> {
    let b = get(this)?;
    let qos: QoS = options::qos(rb::value_to_i64(qos)?)?;
    let retain = rb::value_to_bool(retain)?;
    let topic = rb::string_value(topic)?;
    let payload = rb::string_value(payload)?;
    // No calls into Ruby while the string slices are in use.
    let (topic, payload) = unsafe { (rb::string_bytes(topic), rb::string_bytes(payload)) };
    let topic = std::str::from_utf8(topic).map_err(|_| RubyErr::arg("topic must be UTF-8"))?;
    let res = if retain {
        b.publish_retained(topic, payload, qos)
    } else {
        b.publish(topic, payload, qos)
    };
    res.map_err(Error::from)?;
    Ok(rb::qnil())
}

fn stats(this: VALUE) -> RbResult<VALUE> {
    let b = get(this)?;
    op::new(async move { Ok(Out::Stats(b.stats().await?)) })
}

/// An op that waits for the shutdown, or nil if an op still holds the
/// broker: it stops once that op is done.
fn shutdown(this: VALUE) -> RbResult<VALUE> {
    let native: &NativeBroker = unsafe { TYPE.get(this) }?;
    let taken = native
        .broker
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    let store = native.store.clone();
    match taken.map(Arc::try_unwrap) {
        Some(Ok(b)) => op::new(async move {
            b.shutdown().await;
            if let Some(store) = store {
                store.flush();
            }
            Ok(Out::Nil)
        }),
        _ => Ok(rb::qnil()),
    }
}

unsafe extern "C" fn broker_start(_class: VALUE, opts: VALUE) -> VALUE {
    rb::wrap(|| start(opts))
}

unsafe extern "C" fn broker_bind(this: VALUE, uri: VALUE) -> VALUE {
    rb::wrap(|| bind(this, uri))
}

unsafe extern "C" fn broker_publish(
    this: VALUE,
    topic: VALUE,
    payload: VALUE,
    qos: VALUE,
    retain: VALUE,
) -> VALUE {
    rb::wrap(|| publish(this, topic, payload, qos, retain))
}

unsafe extern "C" fn broker_stats(this: VALUE) -> VALUE {
    rb::wrap(|| stats(this))
}

unsafe extern "C" fn broker_shutdown(this: VALUE) -> VALUE {
    rb::wrap(|| shutdown(this))
}

pub fn register(native: VALUE) -> RbResult<()> {
    let class = rb::define_class_under(native, c"Broker")?;
    let _ = CLASS.set(class as usize);
    rb::define_singleton_method(class, c"start", broker_start as *const (), 1)?;
    rb::define_method(class, c"bind", broker_bind as *const (), 1)?;
    rb::define_method(class, c"publish", broker_publish as *const (), 4)?;
    rb::define_method(class, c"stats", broker_stats as *const (), 0)?;
    rb::define_method(class, c"shutdown", broker_shutdown as *const (), 0)?;
    Ok(())
}

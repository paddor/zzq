//! `ZZQ::Native::Client`: a connected client.
//!
//! Publishes go through a [`Handle`] and copy the Ruby string straight
//! into the send queue. Receiving takes what the inbox holds without a
//! stored future. Only once the inbox is empty does a future wait, kept in
//! `recv`, so that a wake reaches the fiber waiting for it.

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll, Waker};

use rb_sys::VALUE;
use zzq_client::{Client, Handle, Message, QoS, ReasonCode, SubscriptionOptions};

use crate::error::{self, Error};
use crate::op::{self, Out};
use crate::rb::{self, DataType, RbResult, RubyErr};
use crate::{context, options, wake};

static TYPE: DataType = DataType::new::<NativeClient>(c"ZZQ::Native::Client");
static CLASS: OnceLock<usize> = OnceLock::new();
/// `ZZQ::Message` and a frozen empty Array for messages without
/// Subscription Identifiers.
static MESSAGE: OnceLock<(usize, usize)> = OnceLock::new();

type Recv = Pin<Box<dyn Future<Output = Option<Message>> + Send>>;

struct NativeClient {
    handle: Handle,
    client: Arc<tokio::sync::Mutex<Client>>,
    /// The wait for the next message, once the inbox ran empty.
    recv: Mutex<Option<Recv>>,
    session_present: bool,
    assigned_client_id: Option<String>,
}

impl std::fmt::Debug for NativeClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeClient")
            .field("session_present", &self.session_present)
            .finish_non_exhaustive()
    }
}

fn get(value: VALUE) -> RbResult<&'static NativeClient> {
    unsafe { TYPE.get(value) }
}

/// A Ruby object for a connected client.
pub fn wrap(c: Client) -> RbResult<VALUE> {
    let connected = c.connected();
    let native = NativeClient {
        handle: c.handle(),
        session_present: connected.session_present,
        assigned_client_id: connected.assigned_client_id.clone(),
        client: Arc::new(tokio::sync::Mutex::new(c)),
        recv: Mutex::new(None),
    };
    TYPE.wrap(*CLASS.get().expect("registered") as VALUE, native)
}

fn connect(uri: VALUE, opts: VALUE) -> RbResult<VALUE> {
    let uri = rb::value_to_string(uri)?;
    let opts = options::client(opts)?;
    let ctx = context::get()?;
    op::new(async move { Ok(Out::Client(Client::connect(&ctx, &uri, opts).await?)) })
}

fn publish(
    this: VALUE,
    topic: VALUE,
    payload: VALUE,
    qos: VALUE,
    retain: VALUE,
) -> RbResult<VALUE> {
    let c = get(this)?;
    let qos = options::qos(rb::value_to_i64(qos)?)?;
    let retain = rb::value_to_bool(retain)?;
    let topic = rb::string_value(topic)?;
    let payload = rb::string_value(payload)?;
    // No calls into Ruby while the string slices are in use.
    let (topic, payload) = unsafe { (rb::string_bytes(topic), rb::string_bytes(payload)) };
    let topic = std::str::from_utf8(topic).map_err(|_| RubyErr::arg("topic must be UTF-8"))?;
    let queued = {
        let fut = pin!(c.handle.enqueue(topic, payload, qos, retain));
        fut.poll(&mut Context::from_waker(Waker::noop()))
    };
    match queued {
        Poll::Ready(Ok(_)) if qos == QoS::AtMostOnce => Ok(rb::qnil()),
        Poll::Ready(Ok(acked)) => op::acked(acked),
        Poll::Ready(Err(e)) => Err(Error::from(e).into()),
        Poll::Pending => {
            // The queue is full: wait with a copy.
            let (topic, payload, h) = (topic.to_owned(), payload.to_vec(), c.handle.clone());
            op::new(async move {
                h.enqueue(&topic, &payload, qos, retain).await?.await?;
                Ok(Out::Nil)
            })
        }
    }
}

fn subscribe(this: VALUE, filters: VALUE, sub_id: VALUE) -> RbResult<VALUE> {
    let h = get(this)?.handle.clone();
    let sub_id = u32::try_from(rb::value_to_i64(sub_id)?)
        .map_err(|_| RubyErr::arg("subscription_id out of range"))?;
    let mut parsed = Vec::new();
    for f in rb::array_entries(filters)? {
        let [filter, qos, no_local, rap, rh] = rb::array_entries(f)?[..] else {
            return Err(RubyErr::arg("expected [filter, qos, no_local, rap, rh]"));
        };
        let opts = SubscriptionOptions {
            qos: options::qos(rb::value_to_i64(qos)?)?,
            no_local: rb::value_to_bool(no_local)?,
            retain_as_published: rb::value_to_bool(rap)?,
            retain_handling: u8::try_from(rb::value_to_i64(rh)?)
                .ok()
                .filter(|n| *n <= 2)
                .ok_or_else(|| RubyErr::arg("retain_handling must be 0, 1 or 2"))?,
        };
        parsed.push((rb::value_to_string(filter)?, opts));
    }
    op::new(async move {
        let refs: Vec<_> = parsed.iter().map(|(f, o)| (f.as_str(), *o)).collect();
        Ok(Out::Codes(h.subscribe_with(&refs, sub_id).await?))
    })
}

fn unsubscribe(this: VALUE, filters: VALUE) -> RbResult<VALUE> {
    let h = get(this)?.handle.clone();
    let filters = rb::array_entries(filters)?
        .into_iter()
        .map(rb::value_to_string)
        .collect::<RbResult<Vec<_>>>()?;
    op::new(async move {
        let refs: Vec<_> = filters.iter().map(String::as_str).collect();
        Ok(Out::Codes(h.unsubscribe(&refs).await?))
    })
}

/// Up to `max` messages as an Array, `PENDING` if none came yet, or nil
/// once the client is closed.
fn receive(this: VALUE, wake: VALUE, max: VALUE) -> RbResult<VALUE> {
    let c = get(this)?;
    let wake = wake::get(wake)?;
    let max = usize::try_from(rb::value_to_i64(max)?).unwrap_or(1).max(1);
    let mut got = Vec::new();
    let mut slot = c.recv.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(fut) = slot.as_mut() {
        let Poll::Ready(m) = wake.poll(fut.as_mut()) else {
            return Ok(op::pending());
        };
        *slot = None;
        match m {
            Some(m) => got.push(m),
            None => return Ok(rb::qnil()),
        }
    }
    {
        let Ok(mut client) = c.client.try_lock() else {
            return Err(error::zzq("another receive is in progress"));
        };
        let mut cx = Context::from_waker(Waker::noop());
        while got.len() < max {
            match pin!(client.recv()).poll(&mut cx) {
                Poll::Ready(Some(m)) => got.push(m),
                Poll::Ready(None) if got.is_empty() => return Ok(rb::qnil()),
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
    }
    if got.is_empty() {
        let Ok(mut client) = c.client.clone().try_lock_owned() else {
            return Err(error::zzq("another receive is in progress"));
        };
        let mut fut: Recv = Box::pin(async move { client.recv().await });
        match wake.poll(fut.as_mut()) {
            Poll::Pending => {
                *slot = Some(fut);
                return Ok(op::pending());
            }
            Poll::Ready(None) => return Ok(rb::qnil()),
            Poll::Ready(Some(m)) => got.push(m),
        }
    }
    drop(slot);
    messages(got)
}

fn messages(got: Vec<Message>) -> RbResult<VALUE> {
    let &(class, no_ids) = MESSAGE.get().expect("registered");
    let array = rb::array_new_capa(got.len())?;
    for m in got {
        let ids = if m.subscription_ids.is_empty() {
            no_ids as VALUE
        } else {
            let ids = rb::array_new_capa(m.subscription_ids.len())?;
            for id in &m.subscription_ids {
                rb::array_push(ids, rb::u64_value((*id).into()))?;
            }
            rb::freeze(ids)
        };
        let args = [
            rb::interned_utf8_string(m.topic.as_str())?,
            rb::new_binary_string(&m.payload)?,
            rb::u64_value(m.qos as u64),
            rb::bool_value(m.retain),
            ids,
        ];
        rb::array_push(array, rb::call(class as VALUE, c"new", &args)?)?;
    }
    Ok(array)
}

fn disconnect(this: VALUE, reason: VALUE) -> RbResult<VALUE> {
    let h = get(this)?.handle.clone();
    let reason = u8::try_from(rb::value_to_i64(reason)?)
        .map_err(|_| RubyErr::arg("reason code out of range"))?;
    op::new(async move {
        h.disconnect_with(ReasonCode::from(reason)).await;
        Ok(Out::Nil)
    })
}

unsafe extern "C" fn client_connect(_class: VALUE, uri: VALUE, opts: VALUE) -> VALUE {
    rb::wrap(|| connect(uri, opts))
}

unsafe extern "C" fn client_publish(
    this: VALUE,
    topic: VALUE,
    payload: VALUE,
    qos: VALUE,
    retain: VALUE,
) -> VALUE {
    rb::wrap(|| publish(this, topic, payload, qos, retain))
}

unsafe extern "C" fn client_subscribe(this: VALUE, filters: VALUE, sub_id: VALUE) -> VALUE {
    rb::wrap(|| subscribe(this, filters, sub_id))
}

unsafe extern "C" fn client_unsubscribe(this: VALUE, filters: VALUE) -> VALUE {
    rb::wrap(|| unsubscribe(this, filters))
}

unsafe extern "C" fn client_receive(this: VALUE, wake: VALUE, max: VALUE) -> VALUE {
    rb::wrap(|| receive(this, wake, max))
}

unsafe extern "C" fn client_disconnect(this: VALUE, reason: VALUE) -> VALUE {
    rb::wrap(|| disconnect(this, reason))
}

unsafe extern "C" fn client_session_present(this: VALUE) -> VALUE {
    rb::wrap(|| Ok(rb::bool_value(get(this)?.session_present)))
}

unsafe extern "C" fn client_assigned_client_id(this: VALUE) -> VALUE {
    rb::wrap(|| match &get(this)?.assigned_client_id {
        Some(id) => rb::new_utf8_string(id),
        None => Ok(rb::qnil()),
    })
}

pub fn register(zzq: VALUE, native: VALUE) -> RbResult<()> {
    let message = rb::const_get(zzq, c"Message")?;
    let no_ids = rb::freeze(rb::array_new_capa(0)?);
    rb::define_const(native, c"NO_SUBSCRIPTION_IDS", no_ids)?;
    let _ = MESSAGE.set((message as usize, no_ids as usize));

    let class = rb::define_class_under(native, c"Client")?;
    let _ = CLASS.set(class as usize);
    rb::define_singleton_method(class, c"connect", client_connect as *const (), 2)?;
    rb::define_method(class, c"publish", client_publish as *const (), 4)?;
    rb::define_method(class, c"subscribe", client_subscribe as *const (), 2)?;
    rb::define_method(class, c"unsubscribe", client_unsubscribe as *const (), 1)?;
    rb::define_method(class, c"receive", client_receive as *const (), 2)?;
    rb::define_method(class, c"disconnect", client_disconnect as *const (), 1)?;
    rb::define_method(
        class,
        c"session_present?",
        client_session_present as *const (),
        0,
    )?;
    rb::define_method(
        class,
        c"assigned_client_id",
        client_assigned_client_id as *const (),
        0,
    )?;
    Ok(())
}

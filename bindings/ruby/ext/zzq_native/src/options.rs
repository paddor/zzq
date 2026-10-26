//! Client and broker options from the String-keyed Hash that the Ruby
//! side checked. A nil value keeps the default, except where nil means
//! "none".

use std::sync::Arc;
use std::time::Duration;

use rb_sys::VALUE;
use zzq_broker::{BrokerOptions, FilePersistence};
use zzq_client::{ClientOptions, LastWill, QoS, Reconnect, TlsOptions, TransportOptions, Version};

use crate::error;
use crate::rb::{self, RbResult, RubyErr};

/// The value under `key`, unless missing or nil.
fn get(h: VALUE, key: &str) -> RbResult<Option<VALUE>> {
    Ok(rb::hash_get(h, key)?.filter(|v| *v != rb::qnil()))
}

fn int<T: TryFrom<i64>>(h: VALUE, key: &str) -> RbResult<Option<T>> {
    get(h, key)?
        .map(|v| {
            let n = rb::value_to_i64(v)?;
            T::try_from(n).map_err(|_| RubyErr::arg(format!("{key} out of range: {n}")))
        })
        .transpose()
}

fn secs(key: &str, v: VALUE) -> RbResult<Duration> {
    let s = rb::value_to_f64(v)?;
    Duration::try_from_secs_f64(s).map_err(|_| RubyErr::arg(format!("{key} must be seconds >= 0")))
}

fn duration(h: VALUE, key: &str) -> RbResult<Option<Duration>> {
    get(h, key)?.map(|v| secs(key, v)).transpose()
}

fn string(h: VALUE, key: &str) -> RbResult<Option<String>> {
    get(h, key)?.map(rb::value_to_string).transpose()
}

fn bytes(h: VALUE, key: &str) -> RbResult<Option<Vec<u8>>> {
    get(h, key)?.map(rb::value_to_bytes).transpose()
}

fn flag(h: VALUE, key: &str) -> RbResult<Option<bool>> {
    get(h, key)?.map(rb::value_to_bool).transpose()
}

pub fn qos(n: i64) -> RbResult<QoS> {
    u8::try_from(n)
        .ok()
        .and_then(|n| QoS::try_from(n).ok())
        .ok_or_else(|| RubyErr::arg(format!("QoS must be 0, 1 or 2, not {n}")))
}

fn transport(h: VALUE) -> RbResult<TransportOptions> {
    let mut t = TransportOptions::default();
    if let Some(tls) = get(h, "tls")? {
        t.tls = Some(TlsOptions {
            cert_chain_pem: bytes(tls, "cert")?,
            key_pem: bytes(tls, "key")?,
            trust_pem: bytes(tls, "ca")?,
            client_ca_pem: bytes(tls, "client_ca")?,
            server_name: string(tls, "server_name")?,
            danger_accept_invalid_certs: flag(tls, "insecure")?.unwrap_or(false),
            ..TlsOptions::default()
        });
    }
    Ok(t)
}

pub fn client(h: VALUE) -> RbResult<ClientOptions> {
    let mut o = ClientOptions::new(string(h, "client_id")?.unwrap_or_default());
    if let Some(v) = int::<i64>(h, "version")? {
        o.version = match v {
            3 | 4 => Version::V311,
            5 => Version::V5,
            _ => return Err(RubyErr::arg(format!("version must be 3 or 5, not {v}"))),
        };
    }
    if let Some(v) = flag(h, "clean_start")? {
        o.clean_start = v;
    }
    if let Some(v) = int(h, "keep_alive")? {
        o.keep_alive = v;
    }
    if let Some(v) = int(h, "session_expiry")? {
        o.session_expiry = v;
    }
    o.username = string(h, "username")?;
    o.password = bytes(h, "password")?.map(Into::into);
    if let Some(w) = get(h, "will")? {
        o.will = Some(LastWill {
            topic: string(w, "topic")?.ok_or_else(|| RubyErr::arg("will needs a topic"))?,
            payload: bytes(w, "payload")?.unwrap_or_default().into(),
            qos: qos(int(w, "qos")?.unwrap_or(0))?,
            retain: flag(w, "retain")?.unwrap_or(false),
            delay: int(w, "delay")?.unwrap_or(0),
        });
    }
    if let Some(v) = int(h, "inbox")? {
        o.inbox = v;
    }
    if let Some(r) = get(h, "reconnect")? {
        o.reconnect = if r == rb::qfalse() {
            None
        } else if r == rb::qtrue() {
            Some(Reconnect::default())
        } else {
            let d = Reconnect::default();
            Some(Reconnect {
                min: duration(r, "min")?.unwrap_or(d.min),
                max: duration(r, "max")?.unwrap_or(d.max),
            })
        };
    }
    if let Some(v) = duration(h, "connect_timeout")? {
        o.connect_timeout = v;
    }
    o.transport = transport(h)?;
    Ok(o)
}

/// Broker options, and the retained-message store they use.
pub fn broker(h: VALUE) -> RbResult<(BrokerOptions, Option<Arc<FilePersistence>>)> {
    let mut o = BrokerOptions::default();
    macro_rules! ints {
        ($($field:ident),*) => {$(
            if let Some(v) = int(h, stringify!($field))? {
                o.$field = v;
            }
        )*};
    }
    ints!(
        max_packet_size,
        max_inflight,
        max_queued,
        receive_maximum,
        max_subscriptions,
        max_retained,
        max_topic_levels,
        topic_alias_maximum,
        max_session_expiry,
        max_connections
    );
    // TODO: add `max_queued_bytes` and `max_state_bytes` once Cargo.toml
    // requires the next zzq-broker release, the first one that has them.
    // Present but nil means no limit.
    if rb::hash_get(h, "max_keep_alive")?.is_some() {
        o.max_keep_alive = int(h, "max_keep_alive")?;
    }
    if rb::hash_get(h, "sys_interval")?.is_some() {
        o.sys_interval = duration(h, "sys_interval")?;
    }
    if let Some(v) = duration(h, "connect_timeout")? {
        o.connect_timeout = v;
    }
    let store = match string(h, "persistence")? {
        Some(path) => {
            Some(Arc::new(FilePersistence::open(&path).map_err(|e| {
                error::zzq(format!("opening {path} failed: {e}"))
            })?))
        }
        None => None,
    };
    o.persistence = store.clone().map(|s| s as Arc<dyn zzq_broker::Persistence>);
    o.transport = transport(h)?;
    Ok((o, store))
}

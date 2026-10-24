//! Retained messages.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use rustc_hash::FxHashMap;

use crate::msg::{Msg, expired};
use crate::persist::{Persistence, Retained};

/// Retained messages in a tree of topic levels, so a filter visits only
/// the branches it can match.
pub(crate) struct RetainedStore {
    root: Node,
    len: usize,
    /// Most topics stored; new ones beyond it are refused.
    max: usize,
    /// Retained messages refused because `max` was reached.
    pub(crate) rejected: u64,
    /// When messages with a Message Expiry Interval run out, soonest first.
    /// One entry per stored topic with a deadline.
    expiring: BTreeSet<(Instant, Box<str>)>,
    persist: Option<Arc<dyn Persistence>>,
}

#[derive(Default)]
struct Node {
    children: FxHashMap<Box<str>, Node>,
    msg: Option<Msg>,
}

impl Default for RetainedStore {
    fn default() -> Self {
        RetainedStore {
            root: Node::default(),
            len: 0,
            max: usize::MAX,
            rejected: 0,
            expiring: BTreeSet::new(),
            persist: None,
        }
    }
}

impl fmt::Debug for RetainedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetainedStore")
            .field("len", &self.len)
            .field("persist", &self.persist.is_some())
            .finish_non_exhaustive()
    }
}

impl RetainedStore {
    /// A store with the messages `persist` kept from earlier runs.
    pub(crate) fn new(
        persist: Option<Arc<dyn Persistence>>,
        max: usize,
    ) -> std::io::Result<RetainedStore> {
        let mut store = RetainedStore {
            max,
            persist,
            ..RetainedStore::default()
        };
        if let Some(p) = store.persist.clone() {
            let (now, wall) = (Instant::now(), SystemTime::now());
            for r in p.load_retained()? {
                match from_stored(&r, now, wall) {
                    Some(m) => store.insert(m),
                    // Expired while the broker was down.
                    None => p.remove_retained(&r.topic),
                }
            }
        }
        Ok(store)
    }

    /// Stores a compact copy of `msg`, or clears the topic for an empty
    /// payload.
    pub(crate) fn set(&mut self, msg: &Msg) {
        if msg.payload().is_empty() {
            if let Some(old) = remove(&mut self.root, msg.topic().split('/')) {
                self.len -= 1;
                reschedule(&mut self.expiring, msg.topic(), old.expires_at, None);
            }
        } else if self.len >= self.max && self.stored(msg.topic()).is_none() {
            self.rejected += 1;
            return;
        } else {
            self.insert(msg.compact());
        }
        // The broker writes `$SYS/` itself on every start.
        if let Some(p) = &self.persist
            && !msg.topic().starts_with("$SYS/")
        {
            if msg.payload().is_empty() {
                p.remove_retained(msg.topic());
            } else {
                p.set_retained(&to_stored(msg));
            }
        }
    }

    fn insert(&mut self, msg: Msg) {
        let mut node = &mut self.root;
        for level in msg.topic().split('/') {
            node = node.children.entry(level.into()).or_default();
        }
        let new = msg.expires_at;
        let old = node.msg.replace(msg);
        if old.is_none() {
            self.len += 1;
        }
        if let Some(m) = &node.msg {
            let old = old.and_then(|o| o.expires_at);
            reschedule(&mut self.expiring, m.topic(), old, new);
        }
    }

    /// Retained messages matching `filter`.
    pub(crate) fn matching(&self, filter: &str) -> Vec<Msg> {
        let levels: Vec<&str> = filter.split('/').collect();
        let mut out = Vec::new();
        collect(&self.root, &levels, true, Instant::now(), &mut out);
        out
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Drops messages whose Message Expiry Interval ran out.
    pub(crate) fn sweep(&mut self, now: Instant) {
        while let Some((t, _)) = self.expiring.first()
            && *t <= now
        {
            let Some((_, topic)) = self.expiring.pop_first() else {
                break;
            };
            if remove(&mut self.root, topic.split('/')).is_some() {
                self.len -= 1;
            }
            if let Some(p) = &self.persist {
                p.remove_retained(&topic);
            }
        }
    }

    /// The message retained for `topic`.
    fn stored(&self, topic: &str) -> Option<&Msg> {
        let mut node = &self.root;
        for level in topic.split('/') {
            node = node.children.get(level)?;
        }
        node.msg.as_ref()
    }
}

/// Adds the messages under `node` that match `levels`. Wildcards on the
/// first level skip topics that start with `$`.
fn collect(node: &Node, levels: &[&str], first: bool, now: Instant, out: &mut Vec<Msg>) {
    let visible = |name: &str| !(first && name.starts_with('$'));
    match levels.split_first() {
        None => push(node, now, out),
        Some((&"#", _)) => {
            push(node, now, out);
            for (name, child) in &node.children {
                if visible(name) {
                    collect(child, levels, false, now, out);
                }
            }
        }
        Some((&"+", rest)) => {
            for (name, child) in &node.children {
                if visible(name) {
                    collect(child, rest, false, now, out);
                }
            }
        }
        Some((level, rest)) => {
            if let Some(child) = node.children.get(*level) {
                collect(child, rest, false, now, out);
            }
        }
    }
}

fn push(node: &Node, now: Instant, out: &mut Vec<Msg>) {
    if let Some(m) = &node.msg
        && !expired(m.expires_at, now)
    {
        out.push(m.clone());
    }
}

/// Moves the expiry entry of `topic` from deadline `old` to `new`.
fn reschedule(
    expiring: &mut BTreeSet<(Instant, Box<str>)>,
    topic: &str,
    old: Option<Instant>,
    new: Option<Instant>,
) {
    let Some(at) = old.or(new).filter(|_| old != new) else {
        return;
    };
    let mut entry = (at, Box::<str>::from(topic));
    if old.is_some() {
        expiring.remove(&entry);
    }
    if let Some(t) = new {
        entry.0 = t;
        expiring.insert(entry);
    }
}

/// Removes the message at `levels` and prunes empty nodes. Returns the
/// message, if there was one.
fn remove<'a>(node: &mut Node, mut levels: impl Iterator<Item = &'a str>) -> Option<Msg> {
    let Some(level) = levels.next() else {
        return node.msg.take();
    };
    let child = node.children.get_mut(level)?;
    let removed = remove(child, levels);
    if child.msg.is_none() && child.children.is_empty() {
        node.children.remove(level);
    }
    removed
}

fn to_stored(m: &Msg) -> Retained {
    let (now, wall) = (Instant::now(), SystemTime::now());
    Retained {
        topic: m.topic().to_string(),
        payload: m.payload_bytes(),
        qos: m.qos,
        properties: m.props_bytes(),
        expires_at: m
            .expires_at
            .map(|t| wall + t.saturating_duration_since(now)),
    }
}

/// A stored message, unless it expired while the broker was down.
fn from_stored(r: &Retained, now: Instant, wall: SystemTime) -> Option<Msg> {
    let expires_at = match r.expires_at {
        Some(t) => Some(now + t.duration_since(wall).ok()?),
        None => None,
    };
    let mut m = Msg::new(&r.topic, &r.payload, &r.properties, r.qos, true);
    m.expires_at = expires_at;
    Some(m)
}

#[cfg(test)]
mod tests {
    use zzq_proto::QoS;

    use super::*;

    fn msg(topic: &str) -> Msg {
        Msg::new(topic, b"x", &[], QoS::AtMostOnce, true)
    }

    /// The tree agrees with matching every topic against the filter.
    #[test]
    fn matches_like_a_scan() {
        let topics = [
            "a", "a/b", "a/b/c", "a//c", "/a", "/", "b/b", "$SYS/x", "$SYS", "a/$b", "x/y/z/w",
        ];
        let filters = [
            "#", "+", "a/#", "a/+", "a/+/c", "+/b", "+/+", "/#", "+/#", "$SYS/#", "$SYS/+",
            "a/b/c/#", "a//+", "x/+/+/w", "a/$b", "+/$b",
        ];
        let mut store = RetainedStore::default();
        for t in topics {
            store.set(&msg(t));
        }
        store.set(&msg("gone"));
        let cleared = Msg::new("gone", &[], &[], QoS::AtMostOnce, true);
        store.set(&cleared);
        assert_eq!(store.len(), topics.len());
        for f in filters {
            let mut got: Vec<String> = store
                .matching(f)
                .iter()
                .map(|m| m.topic().to_string())
                .collect();
            got.sort();
            let mut want: Vec<String> = topics
                .iter()
                .filter(|t| zzq_proto::topic::matches(f, t))
                .map(|t| (*t).to_owned())
                .collect();
            want.sort();
            assert_eq!(got, want, "filter {f}");
        }
    }
    #[test]
    fn sweep_drops_expired_but_not_replaced() {
        let mut store = RetainedStore::default();
        let now = Instant::now();
        let soon = |topic: &str| {
            let mut m = msg(topic);
            m.expires_at = Some(now);
            m
        };
        store.set(&soon("e/gone"));
        store.set(&soon("e/kept"));
        store.set(&msg("e/kept"));
        store.set(&msg("e/plain"));
        assert_eq!(store.len(), 3);
        store.sweep(now + std::time::Duration::from_millis(1));
        assert_eq!(store.len(), 2);
        let mut left: Vec<String> = store
            .matching("e/#")
            .iter()
            .map(|m| m.topic().to_string())
            .collect();
        left.sort();
        assert_eq!(left, ["e/kept", "e/plain"]);
    }

    #[test]
    fn republished_topic_keeps_one_expiry_entry() {
        let mut store = RetainedStore::default();
        let now = Instant::now();
        for i in 0..100 {
            let mut m = msg("status");
            m.expires_at = Some(now + std::time::Duration::from_secs(60 + i));
            store.set(&m);
        }
        assert_eq!((store.len(), store.expiring.len()), (1, 1));
        store.set(&Msg::new("status", &[], &[], QoS::AtMostOnce, true));
        assert_eq!((store.len(), store.expiring.len()), (0, 0));
    }
}

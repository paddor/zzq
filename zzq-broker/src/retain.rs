//! Retained messages.

use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use rustc_hash::FxHashMap;

use crate::budget;
use crate::msg::{Msg, expired};
use crate::persist::{Persistence, Retained};

/// Retained messages in a tree of topic levels, so a filter visits only
/// the branches it can match.
pub(crate) struct RetainedStore {
    root: Node,
    len: usize,
    /// Most topics stored; new ones beyond it are refused.
    max: usize,
    /// Retained messages refused because `max` or the state budget was
    /// reached.
    pub(crate) rejected: u64,
    /// Bytes counted against `max_state_bytes`: messages, also released
    /// ones, and tree nodes.
    bytes: usize,
    /// Messages the store let go that sessions still hold. They count until
    /// the last session drops them.
    released: Vec<Msg>,
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
            bytes: 0,
            released: Vec::new(),
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
        max_levels: usize,
    ) -> std::io::Result<RetainedStore> {
        let mut store = RetainedStore {
            max,
            persist,
            ..RetainedStore::default()
        };
        if let Some(p) = store.persist.clone() {
            let (now, wall) = (Instant::now(), SystemTime::now());
            for r in p.load_retained()? {
                // Deeper than clients may publish now: the store walks topics
                // a level at a time.
                if crate::msg::too_deep(&r.topic, max_levels) {
                    continue;
                }
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
    /// payload. While the state budget is `full`, only topics already stored
    /// change.
    pub(crate) fn set(&mut self, msg: &Msg, full: bool) {
        if msg.payload().is_empty() {
            self.remove(msg.topic());
        } else if (full || self.len >= self.max) && self.stored(msg.topic()).is_none() {
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
            node = match node.children.entry(level.into()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => {
                    self.bytes += budget::NODE + level.len();
                    e.insert(Node::default())
                }
            };
        }
        let new = msg.expires_at;
        self.bytes += msg.size();
        let old = node.msg.replace(msg);
        if let Some(m) = &node.msg {
            let old = old.as_ref().and_then(|o| o.expires_at);
            reschedule(&mut self.expiring, m.topic(), old, new);
        }
        match old {
            Some(o) if o.is_shared() => self.released.push(o),
            Some(o) => self.bytes -= o.size(),
            None => self.len += 1,
        }
    }

    /// Retained messages matching `filter`.
    pub(crate) fn matching(&self, filter: &str) -> Vec<Msg> {
        let levels: Vec<&str> = filter.split('/').collect();
        let mut out = Vec::new();
        collect(&self.root, &levels, true, Instant::now(), &mut out);
        out
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// Every stored message, except the `$SYS/` ones the broker writes
    /// itself.
    pub(crate) fn messages(&self) -> Vec<Msg> {
        let mut out = Vec::with_capacity(self.len);
        for (name, child) in &self.root.children {
            if &**name != "$SYS" {
                every(child, &mut out);
            }
        }
        out
    }

    /// Bytes counted against `max_state_bytes`.
    pub(crate) const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Removes the message of `topic`, its expiry entry and the tree nodes
    /// left empty.
    fn remove(&mut self, topic: &str) {
        let Some(old) = remove(&mut self.root, topic.split('/'), &mut self.bytes) else {
            return;
        };
        self.len -= 1;
        reschedule(&mut self.expiring, topic, old.expires_at, None);
        self.release(old);
    }

    /// Takes a message the store let go off the count, unless a session
    /// still holds it.
    fn release(&mut self, old: Msg) {
        if old.is_shared() {
            self.released.push(old);
        } else {
            self.bytes -= old.size();
        }
    }

    /// Drops messages whose Message Expiry Interval ran out, and released
    /// ones no session holds anymore.
    pub(crate) fn sweep(&mut self, now: Instant) {
        let bytes = &mut self.bytes;
        self.released.retain(|m| {
            let held = m.is_shared();
            if !held {
                *bytes -= m.size();
            }
            held
        });
        while let Some((t, _)) = self.expiring.first()
            && *t <= now
        {
            let Some((_, topic)) = self.expiring.pop_first() else {
                break;
            };
            self.remove(&topic);
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

fn every(node: &Node, out: &mut Vec<Msg>) {
    out.extend(node.msg.clone());
    for child in node.children.values() {
        every(child, out);
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

/// Removes the message at `levels` and prunes empty nodes, taking their
/// cost off `bytes`. Returns the message, if there was one.
fn remove<'a>(
    node: &mut Node,
    mut levels: impl Iterator<Item = &'a str>,
    bytes: &mut usize,
) -> Option<Msg> {
    let Some(level) = levels.next() else {
        return node.msg.take();
    };
    let child = node.children.get_mut(level)?;
    let removed = remove(child, levels, bytes);
    if child.msg.is_none() && child.children.is_empty() {
        node.children.remove(level);
        *bytes -= budget::NODE + level.len();
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
        // Past what an `Instant` holds on some platforms: as good as never.
        Some(t) => now.checked_add(t.duration_since(wall).ok()?),
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

    /// Keeps what the store persists.
    #[derive(Default)]
    struct Recorded(std::sync::Mutex<Vec<Retained>>);

    impl Persistence for Recorded {
        fn load_retained(&self) -> std::io::Result<Vec<Retained>> {
            Ok(Vec::new())
        }

        fn set_retained(&self, msg: &Retained) {
            self.0.lock().unwrap().push(msg.clone());
        }

        fn remove_retained(&self, _: &str) {}
    }

    #[test]
    fn persisted_messages_keep_their_deadline() {
        let p = Arc::new(Recorded::default());
        let persist: Arc<dyn Persistence> = p.clone();
        let mut store = RetainedStore::new(Some(persist), usize::MAX, usize::MAX).unwrap();
        let mut m = msg("kept");
        m.expires_at = Some(Instant::now() + std::time::Duration::from_secs(60));
        store.set(&m, false);
        let at = p.0.lock().unwrap()[0].expires_at.unwrap();
        assert!(at > SystemTime::now() + std::time::Duration::from_secs(50));
        assert_eq!(
            format!("{store:?}"),
            "RetainedStore { len: 1, persist: true, .. }"
        );
    }

    /// A stored deadline as far ahead as a log can say loads, without one.
    #[test]
    fn a_deadline_too_far_ahead_loads_as_none() {
        let r = Retained {
            topic: "far".into(),
            payload: bytes::Bytes::from_static(b"x"),
            qos: QoS::AtMostOnce,
            properties: bytes::Bytes::new(),
            expires_at: Some(std::time::UNIX_EPOCH + std::time::Duration::from_millis(u64::MAX)),
        };
        let m = from_stored(&r, Instant::now(), SystemTime::now()).unwrap();
        assert!(m.expires_at.is_none_or(|t| t > Instant::now()));
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
            store.set(&msg(t), false);
        }
        store.set(&msg("gone"), false);
        let cleared = Msg::new("gone", &[], &[], QoS::AtMostOnce, true);
        store.set(&cleared, false);
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
    fn bytes_follow_topics_and_a_full_budget_keeps_new_ones_out() {
        let mut store = RetainedStore::default();
        store.set(&msg("a/b/c"), false);
        store.set(&msg("a/b/d"), false);
        let two = store.bytes();
        let longer = Msg::new("a/b/c", b"longer", &[], QoS::AtMostOnce, true);
        store.set(&longer, false);
        assert_eq!(store.bytes(), two + 5);
        // A full budget refuses new topics, not updates.
        store.set(&msg("a/x"), true);
        store.set(&msg("a/b/c"), true);
        assert_eq!((store.len(), store.rejected, store.bytes()), (2, 1, two));
        for topic in ["a/b/c", "a/b/d"] {
            store.set(&Msg::new(topic, &[], &[], QoS::AtMostOnce, true), false);
        }
        assert_eq!((store.len(), store.bytes()), (0, 0));
    }

    #[test]
    fn a_message_counts_until_no_session_holds_it() {
        let mut store = RetainedStore::default();
        store.set(&msg("a"), false);
        let one = store.bytes();
        let held = store.matching("a").pop().unwrap();
        // Replaced, the old message counts while a session holds it.
        store.set(&msg("a"), false);
        assert_eq!(store.bytes(), one + held.size());
        drop(held);
        store.sweep(Instant::now());
        assert_eq!(store.bytes(), one);
        let held = store.matching("a").pop().unwrap();
        store.set(&Msg::new("a", &[], &[], QoS::AtMostOnce, true), false);
        store.sweep(Instant::now());
        assert_eq!(store.bytes(), held.size());
        drop(held);
        store.sweep(Instant::now());
        assert_eq!(store.bytes(), 0);
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
        store.set(&soon("e/gone"), false);
        store.set(&soon("e/kept"), false);
        store.set(&msg("e/kept"), false);
        store.set(&msg("e/plain"), false);
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
            store.set(&m, false);
        }
        assert_eq!((store.len(), store.expiring.len()), (1, 1));
        store.set(&Msg::new("status", &[], &[], QoS::AtMostOnce, true), false);
        assert_eq!((store.len(), store.expiring.len()), (0, 0));
    }
}

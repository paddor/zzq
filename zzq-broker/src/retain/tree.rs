//! A stripe's retained messages, in a tree of topic levels.

use std::collections::BTreeSet;
use std::collections::hash_map::Entry;
use std::time::Instant;

use rustc_hash::FxHashMap;

use crate::budget;
use crate::msg::{Msg, expired};

/// Retained messages in a tree of topic levels, so a filter visits only
/// the branches it can match.
#[derive(Debug, Default)]
pub(super) struct Tree {
    root: Node,
    len: usize,
    /// Bytes counted against `max_state_bytes`: messages, also released
    /// ones, and tree nodes.
    bytes: usize,
    /// Messages the tree let go that sessions still hold. They count until
    /// the last session drops them.
    released: Vec<Msg>,
    /// When messages with a Message Expiry Interval run out, soonest first.
    /// One entry per stored topic with a deadline.
    expiring: BTreeSet<(Instant, Box<str>)>,
}

#[derive(Debug, Default)]
struct Node {
    children: FxHashMap<Box<str>, Node>,
    msg: Option<Msg>,
}

impl Tree {
    /// Stores `msg` for its topic. True for a topic new to the tree.
    pub(super) fn insert(&mut self, msg: Msg) -> bool {
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
        if let Some(o) = old {
            self.release(o);
            false
        } else {
            self.len += 1;
            true
        }
    }

    /// Clears `topic`, its expiry entry and the tree nodes left empty.
    /// False if it held no message.
    pub(super) fn remove(&mut self, topic: &str) -> bool {
        let Some(old) = remove(&mut self.root, topic.split('/'), &mut self.bytes) else {
            return false;
        };
        self.len -= 1;
        reschedule(&mut self.expiring, topic, old.expires_at, None);
        self.release(old);
        true
    }

    /// Takes a message the tree let go off the count, unless a session
    /// still holds it.
    fn release(&mut self, old: Msg) {
        if old.is_shared() {
            self.released.push(old);
        } else {
            self.bytes -= old.size();
        }
    }

    /// Adds the retained messages matching `filter` to `out`.
    pub(super) fn matching(&self, filter: &str, out: &mut Vec<Msg>) {
        let levels: Vec<&str> = filter.split('/').collect();
        collect(&self.root, &levels, true, crate::time::now(), out);
    }

    pub(super) const fn len(&self) -> usize {
        self.len
    }

    /// Every stored message, except the `$SYS/` ones the broker writes
    /// itself.
    pub(super) fn messages(&self) -> Vec<Msg> {
        let mut out = Vec::with_capacity(self.len);
        for (name, child) in &self.root.children {
            if &**name != "$SYS" {
                every(child, &mut out);
            }
        }
        out
    }

    /// Bytes counted against `max_state_bytes`.
    pub(super) const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Drops messages whose Message Expiry Interval ran out, and released
    /// ones no session holds anymore. Returns how many topics it cleared.
    pub(super) fn sweep(&mut self, now: Instant) -> usize {
        let bytes = &mut self.bytes;
        self.released.retain(|m| {
            let held = m.is_shared();
            if !held {
                *bytes -= m.size();
            }
            held
        });
        let mut cleared = 0;
        while let Some((t, _)) = self.expiring.first()
            && *t <= now
        {
            let Some((_, topic)) = self.expiring.pop_first() else {
                break;
            };
            cleared += usize::from(self.remove(&topic));
        }
        cleared
    }

    /// The message retained for `topic`.
    pub(super) fn stored(&self, topic: &str) -> Option<&Msg> {
        let mut node = &self.root;
        for level in topic.split('/') {
            node = node.children.get(level)?;
        }
        node.msg.as_ref()
    }

    #[cfg(test)]
    pub(super) fn expiring(&self) -> usize {
        self.expiring.len()
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

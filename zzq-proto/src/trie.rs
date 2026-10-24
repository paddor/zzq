//! Topic filter trie.

use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::vec::Vec;

/// Maps topic filters to values and finds the values matching a topic.
#[derive(Debug)]
pub struct TopicTrie<T> {
    root: Node<T>,
    len: usize,
}

#[derive(Debug)]
struct Node<T> {
    children: BTreeMap<Box<str>, Node<T>>,
    plus: Option<Box<Node<T>>>,
    /// Values of filters ending in `#` at this level.
    hash: Vec<T>,
    /// Values of filters ending at this level.
    values: Vec<T>,
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Node {
            children: BTreeMap::new(),
            plus: None,
            hash: Vec::new(),
            values: Vec::new(),
        }
    }
}

impl<T> Node<T> {
    fn is_empty(&self) -> bool {
        self.values.is_empty()
            && self.hash.is_empty()
            && self.plus.is_none()
            && self.children.is_empty()
    }
}

impl<T> Default for TopicTrie<T> {
    fn default() -> Self {
        TopicTrie {
            root: Node::default(),
            len: 0,
        }
    }
}

impl<T> TopicTrie<T> {
    /// An empty trie.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored values.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether no values are stored.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds `value` under a valid `filter`.
    pub fn insert(&mut self, filter: &str, value: T) {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            match level {
                "#" => {
                    node.hash.push(value);
                    self.len += 1;
                    return;
                }
                "+" => node = node.plus.get_or_insert_with(Box::default),
                _ => node = node.children.entry(level.into()).or_default(),
            }
        }
        node.values.push(value);
        self.len += 1;
    }

    /// Removes the values under `filter` for which `pred` returns true.
    /// Returns how many were removed.
    pub fn remove(&mut self, filter: &str, mut pred: impl FnMut(&T) -> bool) -> usize {
        let n = remove_in(&mut self.root, filter, &mut pred);
        self.len -= n;
        n
    }

    /// Mutable access to the values stored exactly under `filter`.
    pub fn get_mut(&mut self, filter: &str) -> Option<&mut Vec<T>> {
        let mut node = &mut self.root;
        for level in filter.split('/') {
            match level {
                "#" => return Some(&mut node.hash),
                "+" => node = node.plus.as_deref_mut()?,
                _ => node = node.children.get_mut(level)?,
            }
        }
        Some(&mut node.values)
    }

    /// Calls `f` for every value whose filter matches `topic`.
    pub fn for_each_match(&self, topic: &str, mut f: impl FnMut(&T)) {
        let dollar = topic.starts_with('$');
        walk(&self.root, Some(topic), true, dollar, &mut f);
    }
}

fn walk<T>(node: &Node<T>, rest: Option<&str>, root: bool, dollar: bool, f: &mut impl FnMut(&T)) {
    let wildcards = !(root && dollar);
    if wildcards {
        node.hash.iter().for_each(&mut *f);
    }
    let Some(rest) = rest else {
        node.values.iter().for_each(f);
        return;
    };
    if node.children.is_empty() && (node.plus.is_none() || !wildcards) {
        return;
    }
    let (level, next) = split_level(rest);
    if let Some(child) = node.children.get(level) {
        walk(child, next, false, dollar, f);
    }
    if wildcards && let Some(plus) = &node.plus {
        walk(plus, next, false, dollar, f);
    }
}

/// First level of `topic` and the rest after its `/`. A byte scan: levels
/// are short, and `/` is ASCII, so the split is on a char boundary.
#[inline]
fn split_level(topic: &str) -> (&str, Option<&str>) {
    match topic.bytes().position(|b| b == b'/') {
        Some(i) => (&topic[..i], Some(&topic[i + 1..])),
        None => (topic, None),
    }
}

fn remove_in<T>(node: &mut Node<T>, rest: &str, pred: &mut impl FnMut(&T) -> bool) -> usize {
    let (level, next) = match rest.split_once('/') {
        Some((l, n)) => (l, Some(n)),
        None => (rest, None),
    };
    let removed = match (level, next) {
        ("#", _) => retain_count(&mut node.hash, pred),
        ("+", None) => node
            .plus
            .as_deref_mut()
            .map_or(0, |n| retain_count(&mut n.values, pred)),
        ("+", Some(next)) => node
            .plus
            .as_deref_mut()
            .map_or(0, |n| remove_in(n, next, pred)),
        (l, None) => node
            .children
            .get_mut(l)
            .map_or(0, |n| retain_count(&mut n.values, pred)),
        (l, Some(next)) => node
            .children
            .get_mut(l)
            .map_or(0, |n| remove_in(n, next, pred)),
    };
    if node.plus.as_deref().is_some_and(Node::is_empty) {
        node.plus = None;
    }
    if level != "#" && level != "+" && node.children.get(level).is_some_and(Node::is_empty) {
        node.children.remove(level);
    }
    removed
}

fn retain_count<T>(v: &mut Vec<T>, pred: &mut impl FnMut(&T) -> bool) -> usize {
    let before = v.len();
    v.retain(|x| !pred(x));
    before - v.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic::matches;
    use alloc::vec;

    fn found(t: &TopicTrie<u32>, topic: &str) -> Vec<u32> {
        let mut v = Vec::new();
        t.for_each_match(topic, |x| v.push(*x));
        v.sort_unstable();
        v
    }

    #[test]
    fn matches_like_topic_matches() {
        let filters = [
            "a/b", "a/+", "a/#", "#", "+/b", "+/+", "$SYS/#", "a/b/c", "/", "+",
        ];
        let topics = ["a/b", "a", "a/b/c", "x/b", "$SYS/up", "/", "b"];
        let mut t = TopicTrie::new();
        for (i, f) in filters.iter().enumerate() {
            t.insert(f, i as u32);
        }
        for topic in topics {
            let want: Vec<u32> = filters
                .iter()
                .enumerate()
                .filter(|(_, f)| matches(f, topic))
                .map(|(i, _)| i as u32)
                .collect();
            assert_eq!(found(&t, topic), want, "{topic}");
        }
    }

    #[test]
    fn remove_prunes() {
        let mut t = TopicTrie::new();
        t.insert("a/+/c", 1);
        t.insert("a/+/c", 2);
        t.insert("a/#", 3);
        assert_eq!(t.remove("a/+/c", |v| *v == 1), 1);
        assert_eq!(found(&t, "a/b/c"), vec![2, 3]);
        assert_eq!(t.remove("a/+/c", |_| true), 1);
        assert_eq!(t.remove("a/#", |_| true), 1);
        assert!(t.is_empty());
        assert!(t.root.is_empty());
    }
}

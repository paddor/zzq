//! Subscription index of one lane. Every lane holds all subscriptions of
//! the broker, so publishes match without crossing lanes.

use std::cell::Cell;
use std::hash::BuildHasher;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use zzq_proto::SubscriptionOptions;
use zzq_proto::topic::SharedFilter;
use zzq_proto::trie::TopicTrie;

use crate::slab::Key;

/// Most topics kept in the match cache before it is cleared.
const CACHE_LIMIT: usize = 1 << 16;
/// Slots of recently missed topic hashes. A topic enters the cache on its
/// second miss, so streams of distinct topics do not churn it.
const SEEN: usize = 1 << 12;

/// A session on some lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SubRef {
    pub(crate) lane: u16,
    pub(crate) key: Key,
}

/// v5 Subscription Identifiers of one delivery: one per matching
/// subscription of the session, up to four. 0 marks a free slot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SubIds([u32; 4]);

impl SubIds {
    pub(crate) fn one(id: u32) -> SubIds {
        SubIds([id, 0, 0, 0])
    }

    pub(crate) fn is_empty(self) -> bool {
        self.0[0] == 0
    }

    /// Adds `id` if it is new and a slot is free.
    pub(crate) fn add(&mut self, id: u32) {
        if id == 0 || self.0.contains(&id) {
            return;
        }
        if let Some(slot) = self.0.iter_mut().find(|s| **s == 0) {
            *slot = id;
        }
    }

    pub(crate) fn iter(self) -> impl Iterator<Item = u32> {
        self.0.into_iter().take_while(|&id| id != 0)
    }
}

/// One subscription of one session.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) sub: SubRef,
    pub(crate) opts: SubscriptionOptions,
    /// v5 Subscription Identifiers.
    pub(crate) sub_id: SubIds,
    /// The whole `$share/...` filter for shared subscriptions.
    pub(crate) share: Option<Arc<str>>,
}

/// Members of one shared subscription. Each message goes to one of them.
#[derive(Debug)]
pub(crate) struct Group {
    pub(crate) members: Vec<Entry>,
    /// Shared by every topic the group matches, so the rotation does not
    /// restart per topic.
    next: Rc<Cell<usize>>,
}

impl Group {
    /// Next member in round-robin order.
    pub(crate) fn pick(&self) -> &Entry {
        let i = self.next.get();
        self.next.set(i.wrapping_add(1));
        &self.members[i % self.members.len()]
    }
}

/// Subscriptions matching one topic.
#[derive(Debug, Default)]
pub(crate) struct Matches {
    /// At most one entry per session.
    pub(crate) direct: Vec<Entry>,
    pub(crate) groups: Vec<Group>,
}

#[derive(Debug)]
pub(crate) struct SubIndex {
    trie: TopicTrie<Entry>,
    cache: FxHashMap<Box<str>, Rc<Matches>>,
    seen: Box<[u64]>,
    /// Matches of an uncached topic, reused while nobody holds them.
    scratch: Rc<Matches>,
    /// Rotation of each shared subscription.
    rotations: FxHashMap<Arc<str>, Rc<Cell<usize>>>,
    /// Bumped on every change, so callers can cache lookups.
    generation: u64,
}

impl Default for SubIndex {
    fn default() -> Self {
        SubIndex {
            trie: TopicTrie::default(),
            cache: FxHashMap::default(),
            seen: vec![0; SEEN].into_boxed_slice(),
            scratch: Rc::default(),
            rotations: FxHashMap::default(),
            generation: 0,
        }
    }
}

impl SubIndex {
    /// Adds or replaces the subscription of `entry.sub` to `filter`.
    pub(crate) fn add(&mut self, filter: &str, entry: Entry) {
        let inner = inner_filter(filter);
        self.trie
            .remove(inner, |e| e.sub == entry.sub && e.share == entry.share);
        if let Some(share) = &entry.share {
            self.rotations.entry(share.clone()).or_default();
        }
        self.trie.insert(inner, entry);
        self.cache.clear();
        self.generation += 1;
    }

    /// Removes the subscription of `sub` to `filter`.
    pub(crate) fn remove(&mut self, filter: &str, sub: SubRef) -> bool {
        let share = SharedFilter::parse(filter).map(|_| filter);
        let removed = self.trie.remove(inner_filter(filter), |e| {
            e.sub == sub && e.share.as_deref() == share
        });
        if removed > 0 {
            self.cache.clear();
            self.generation += 1;
            // Forget the rotation with the group's last member.
            if let Some(share) = share {
                let left = self
                    .trie
                    .get_mut(inner_filter(filter))
                    .is_some_and(|v| v.iter().any(|e| e.share.as_deref() == Some(share)));
                if !left {
                    self.rotations.remove(share);
                }
            }
        }
        removed > 0
    }

    /// Subscriptions matching `topic`, and whether they are cached. Uncached
    /// matches are only valid until the next lookup.
    #[inline]
    pub(crate) fn lookup(&mut self, topic: &str) -> (Rc<Matches>, bool) {
        if let Some(m) = self.cache.get(topic) {
            return (m.clone(), true);
        }
        self.lookup_slow(topic)
    }

    #[cold]
    fn lookup_slow(&mut self, topic: &str) -> (Rc<Matches>, bool) {
        let h = rustc_hash::FxBuildHasher.hash_one(topic) | 1;
        let slot = &mut self.seen[(h as usize) & (SEEN - 1)];
        let again = *slot == h;
        *slot = h;
        if again {
            let mut m = Matches::default();
            fill(&self.trie, &self.rotations, topic, &mut m);
            let m = Rc::new(m);
            if self.cache.len() >= CACHE_LIMIT {
                self.cache.clear();
            }
            self.cache.insert(topic.into(), m.clone());
            return (m, true);
        }
        if Rc::get_mut(&mut self.scratch).is_none() {
            self.scratch = Rc::default();
        }
        if let Some(m) = Rc::get_mut(&mut self.scratch) {
            fill(&self.trie, &self.rotations, topic, m);
        }
        (self.scratch.clone(), false)
    }

    pub(crate) fn len(&self) -> usize {
        self.trie.len()
    }

    /// Changes with every add or remove.
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }
}

/// Puts the subscriptions matching `topic` into `m`: one direct entry per
/// session with its highest QoS, and the shared groups.
fn fill(
    trie: &TopicTrie<Entry>,
    rotations: &FxHashMap<Arc<str>, Rc<Cell<usize>>>,
    topic: &str,
    m: &mut Matches,
) {
    m.direct.clear();
    m.groups.clear();
    let mut groups: Option<FxHashMap<Arc<str>, Vec<Entry>>> = None;
    trie.for_each_match(topic, |e| match &e.share {
        Some(s) => groups
            .get_or_insert_default()
            .entry(s.clone())
            .or_default()
            .push(e.clone()),
        None => m.direct.push(e.clone()),
    });
    if m.direct.len() > 1 {
        m.direct
            .sort_by(|a, b| a.sub.cmp(&b.sub).then(b.opts.qos.cmp(&a.opts.qos)));
        // One delivery per session with the highest QoS. No Local holds only
        // if every matching subscription has it.
        m.direct.dedup_by(|b, a| {
            if a.sub != b.sub {
                return false;
            }
            a.opts.no_local &= b.opts.no_local;
            a.opts.retain_as_published |= b.opts.retain_as_published;
            for id in b.sub_id.iter() {
                a.sub_id.add(id);
            }
            true
        });
    }
    for (share, mut members) in groups.into_iter().flatten() {
        members.sort_by_key(|e| e.sub);
        let next = rotations.get(&share).cloned().unwrap_or_default();
        m.groups.push(Group { members, next });
    }
    m.groups
        .sort_by(|a, b| a.members[0].share.cmp(&b.members[0].share));
}

fn inner_filter(filter: &str) -> &str {
    match SharedFilter::parse(filter) {
        Some(Ok(s)) => s.filter,
        _ => filter,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zzq_proto::QoS;

    fn sub(n: u32) -> SubRef {
        SubRef {
            lane: 0,
            key: Key {
                slot: n,
                generation: 0,
            },
        }
    }

    fn entry(n: u32, qos: QoS, share: Option<&str>) -> Entry {
        Entry {
            sub: sub(n),
            opts: SubscriptionOptions::qos(qos),
            sub_id: SubIds::default(),
            share: share.map(Arc::from),
        }
    }

    #[test]
    fn dedups_and_groups() {
        let mut idx = SubIndex::default();
        idx.add("a/+", entry(1, QoS::AtMostOnce, None));
        idx.add("a/b", entry(1, QoS::ExactlyOnce, None));
        idx.add("#", entry(2, QoS::AtLeastOnce, None));
        idx.add(
            "$share/g/a/#",
            entry(3, QoS::AtMostOnce, Some("$share/g/a/#")),
        );
        idx.add(
            "$share/g/a/#",
            entry(4, QoS::AtMostOnce, Some("$share/g/a/#")),
        );
        let m = idx.lookup("a/b").0;
        assert_eq!(m.direct.len(), 2);
        assert_eq!(m.direct[0].opts.qos, QoS::ExactlyOnce);
        assert_eq!(m.groups.len(), 1);
        let first = m.groups[0].pick().sub;
        let second = m.groups[0].pick().sub;
        assert_ne!(first, second);
        assert!(idx.remove("$share/g/a/#", sub(3)));
        assert!(!idx.remove("$share/g/a/#", sub(3)));
        assert_eq!(idx.lookup("a/b").0.groups[0].members.len(), 1);
        assert!(idx.remove("a/+", sub(1)));
        assert_eq!(idx.len(), 3);
    }
    #[test]
    fn shared_rotation_spans_topics() {
        let mut idx = SubIndex::default();
        let share = Some("$share/g/t/#");
        idx.add("$share/g/t/#", entry(1, QoS::AtMostOnce, share));
        idx.add("$share/g/t/#", entry(2, QoS::AtMostOnce, share));
        let picks: Vec<SubRef> = (0..4)
            .map(|i| idx.lookup(&format!("t/{i}")).0.groups[0].pick().sub)
            .collect();
        assert_eq!(picks, [sub(1), sub(2), sub(1), sub(2)]);
        assert!(idx.remove("$share/g/t/#", sub(1)));
        assert_eq!(idx.rotations.len(), 1);
        assert!(idx.remove("$share/g/t/#", sub(2)));
        assert!(idx.rotations.is_empty());
    }
}

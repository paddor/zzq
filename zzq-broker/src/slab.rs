//! Generational slab for lane-local sessions.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Stable key of a slab entry. Stale keys never resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Key {
    pub(crate) slot: u32,
    pub(crate) generation: u32,
}

#[derive(Debug)]
struct Slot<T> {
    generation: u32,
    value: Option<T>,
}

#[derive(Debug)]
pub(crate) struct Slab<T> {
    slots: Vec<Slot<T>>,
    /// Free slots, lowest first, so entries gather at the start and
    /// [`Slab::trim`] can give back the end.
    free: BinaryHeap<Reverse<u32>>,
    len: usize,
    /// Generation of a new slot: past every slot trimmed, so their keys
    /// stay stale.
    floor: u32,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Slab {
            slots: Vec::new(),
            free: BinaryHeap::new(),
            len: 0,
            floor: 0,
        }
    }
}

impl<T> Slab<T> {
    pub(crate) fn insert(&mut self, value: T) -> Key {
        self.len += 1;
        if let Some(Reverse(slot)) = self.free.pop() {
            let s = &mut self.slots[slot as usize];
            s.generation = s.generation.wrapping_add(1);
            s.value = Some(value);
            return Key {
                slot,
                generation: s.generation,
            };
        }
        let slot = self.slots.len() as u32;
        let generation = self.floor;
        self.slots.push(Slot {
            generation,
            value: Some(value),
        });
        Key { slot, generation }
    }

    #[inline]
    pub(crate) fn get(&self, key: Key) -> Option<&T> {
        let s = self.slots.get(key.slot as usize)?;
        if s.generation == key.generation {
            s.value.as_ref()
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn get_mut(&mut self, key: Key) -> Option<&mut T> {
        let s = self.slots.get_mut(key.slot as usize)?;
        if s.generation == key.generation {
            s.value.as_mut()
        } else {
            None
        }
    }

    pub(crate) fn remove(&mut self, key: Key) -> Option<T> {
        let s = self.slots.get_mut(key.slot as usize)?;
        if s.generation != key.generation {
            return None;
        }
        let v = s.value.take()?;
        self.free.push(Reverse(key.slot));
        self.len -= 1;
        Some(v)
    }

    /// Gives back the free slots after the last entry, and the memory of a
    /// slot vector far larger than its entries.
    pub(crate) fn trim(&mut self) {
        let before = self.slots.len();
        while let Some(s) = self.slots.last()
            && s.value.is_none()
        {
            self.floor = self.floor.max(s.generation.wrapping_add(1));
            self.slots.pop();
        }
        if self.slots.len() < before {
            let end = self.slots.len() as u32;
            self.free.retain(|&Reverse(slot)| slot < end);
        }
        if self.slots.capacity() > 2 * self.slots.len() + 64 {
            self.slots
                .shrink_to(self.slots.len() + self.slots.len() / 2);
            self.free.shrink_to_fit();
        }
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    /// Slots used so far; keys name only slots below this.
    pub(crate) const fn slots(&self) -> u32 {
        self.slots.len() as u32
    }

    /// The key of the entry in `slot`, if any.
    pub(crate) fn key_at(&self, slot: u32) -> Option<Key> {
        let s = self.slots.get(slot as usize)?;
        s.value.as_ref().map(|_| Key {
            slot,
            generation: s.generation,
        })
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.slots.iter().enumerate().filter_map(|(i, s)| {
            s.value.as_ref().map(|_| Key {
                slot: i as u32,
                generation: s.generation,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_keys_do_not_resolve() {
        let mut s = Slab::default();
        let a = s.insert(1);
        assert_eq!(s.remove(a), Some(1));
        let b = s.insert(2);
        assert_eq!(a.slot, b.slot);
        assert_eq!(s.get(a), None);
        assert_eq!(s.get(b), Some(&2));
        assert_eq!(s.len(), 1);
    }

    /// After a spike, trimming gives back the slots past the last entry,
    /// and keys from before stay stale once slots are made again.
    #[test]
    fn trim_gives_back_the_slots_of_a_spike() {
        let mut s = Slab::default();
        let keep = s.insert(0);
        let spike: Vec<Key> = (1..1000).map(|i| s.insert(i)).collect();
        for k in &spike {
            s.remove(*k);
        }
        // New entries take the lowest free slots, so the spike's stay free.
        let again = s.insert(1);
        assert_eq!(again.slot, 1);
        s.trim();
        assert_eq!(s.slots(), 2);
        assert!(s.slots.capacity() < 100);
        assert_eq!((s.get(keep), s.get(again)), (Some(&0), Some(&1)));
        for i in 2..1000 {
            s.insert(i);
        }
        assert!(spike.iter().all(|k| s.get(*k).is_none()));
        assert_eq!(s.len(), 1000);
    }
}

//! Generational slab for lane-local sessions.

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
    free: Vec<u32>,
    len: usize,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Slab {
            slots: Vec::new(),
            free: Vec::new(),
            len: 0,
        }
    }
}

impl<T> Slab<T> {
    pub(crate) fn insert(&mut self, value: T) -> Key {
        self.len += 1;
        if let Some(slot) = self.free.pop() {
            let s = &mut self.slots[slot as usize];
            s.generation = s.generation.wrapping_add(1);
            s.value = Some(value);
            return Key {
                slot,
                generation: s.generation,
            };
        }
        let slot = self.slots.len() as u32;
        self.slots.push(Slot {
            generation: 0,
            value: Some(value),
        });
        Key {
            slot,
            generation: 0,
        }
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
        self.free.push(key.slot);
        self.len -= 1;
        Some(v)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
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
}

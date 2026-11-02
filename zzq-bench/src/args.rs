//! `key=value` arguments of the bench tools.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

/// `key=value` arguments. [`Kv::finish`] refuses keys no lookup read, so a
/// misspelled key fails instead of running with the default.
#[derive(Debug)]
pub struct Kv<'a> {
    map: HashMap<&'a str, &'a str>,
    read: RefCell<HashSet<&'a str>>,
}

impl<'a> Kv<'a> {
    /// The arguments, or `None` if one has no `=`.
    pub fn parse(args: &'a [String]) -> Option<Kv<'a>> {
        let map = args
            .iter()
            .map(|a| a.split_once('='))
            .collect::<Option<_>>()?;
        Some(Kv {
            map,
            read: RefCell::default(),
        })
    }

    /// The value of `key`, if given.
    pub fn get(&self, key: &str) -> Option<&'a str> {
        let (&k, &v) = self.map.get_key_value(key)?;
        self.read.borrow_mut().insert(k);
        Some(v)
    }

    /// `None` if a key was given that no lookup read. Names them on
    /// stderr.
    pub fn finish(&self) -> Option<()> {
        let read = self.read.borrow();
        let mut unknown: Vec<&str> = self
            .map
            .keys()
            .filter(|k| !read.contains(*k))
            .copied()
            .collect();
        if unknown.is_empty() {
            return Some(());
        }
        unknown.sort_unstable();
        eprintln!("unknown keys: {}", unknown.join(", "));
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(a: &[&str]) -> Vec<String> {
        a.iter().map(|s| (*s).to_owned()).collect()
    }

    #[test]
    fn keys_nothing_read_fail_the_finish() {
        let a = args(&["qos=1", "persit=1"]);
        let kv = Kv::parse(&a).unwrap();
        assert_eq!(kv.get("qos"), Some("1"));
        assert_eq!(kv.get("persist"), None);
        assert_eq!(kv.finish(), None);
        assert_eq!(kv.get("persit"), Some("1"));
        assert_eq!(kv.finish(), Some(()));
    }

    #[test]
    fn an_argument_without_a_value_is_refused() {
        assert!(Kv::parse(&args(&["qos"])).is_none());
        assert!(Kv::parse(&[]).unwrap().finish().is_some());
    }
}

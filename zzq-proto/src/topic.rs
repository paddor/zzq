//! Topic name and filter rules.

use crate::DecodeError;

/// Checks a topic name: not empty, no wildcards, at most 65535 bytes.
#[inline]
pub fn validate_name(topic: &str) -> Result<(), DecodeError> {
    let b = topic.as_bytes();
    if b.is_empty() || b.len() > 65_535 || b.iter().any(|&c| c == b'+' || c == b'#') {
        return Err(DecodeError::InvalidTopic);
    }
    Ok(())
}

/// Checks a topic filter: `+` fills a whole level, `#` fills the last one.
pub fn validate_filter(filter: &str) -> Result<(), DecodeError> {
    if filter.is_empty() || filter.len() > 65_535 {
        return Err(DecodeError::InvalidTopic);
    }
    let body = match SharedFilter::parse(filter) {
        Some(Ok(s)) => s.filter,
        Some(Err(e)) => return Err(e),
        None => filter,
    };
    let mut levels = body.split('/').peekable();
    while let Some(level) = levels.next() {
        let last = levels.peek().is_none();
        match level {
            "+" => {}
            "#" if last => {}
            _ if level.contains(['+', '#']) => return Err(DecodeError::InvalidTopic),
            _ => {}
        }
    }
    Ok(())
}

/// Whether `filter` matches `topic`. Both must be valid. Topics starting with
/// `$` do not match filters starting with a wildcard.
pub fn matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) | (None, None) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            _ => return false,
        }
    }
}

/// A `$share/{group}/{filter}` subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SharedFilter<'a> {
    pub group: &'a str,
    pub filter: &'a str,
}

impl<'a> SharedFilter<'a> {
    /// `None` if `filter` is not shared, an error if it is malformed.
    pub fn parse(filter: &'a str) -> Option<Result<Self, DecodeError>> {
        let rest = filter.strip_prefix("$share/")?;
        let Some((group, inner)) = rest.split_once('/') else {
            return Some(Err(DecodeError::InvalidTopic));
        };
        if group.is_empty() || group.contains(['+', '#']) || inner.is_empty() {
            return Some(Err(DecodeError::InvalidTopic));
        }
        Some(Ok(SharedFilter {
            group,
            filter: inner,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_reject_empty_and_wildcards() {
        assert!(validate_name("a/b").is_ok());
        assert!(validate_name("/").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("a/+").is_err());
        assert!(validate_name("a/#").is_err());
    }

    #[test]
    fn filters_take_wildcards_only_as_whole_levels() {
        for ok in ["#", "+", "a/+/b", "a/#", "+/+", "/", "$share/g/a/#"] {
            assert!(validate_filter(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "a/#/b",
            "a+",
            "a/b#",
            "$share/g",
            "$share//a",
            "$share/g+/a",
        ] {
            assert!(validate_filter(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn wildcards_match_levels_but_not_dollar_topics() {
        let cases = [
            ("a/b", "a/b", true),
            ("a/+", "a/b", true),
            ("a/+", "a/b/c", false),
            ("a/#", "a", true),
            ("a/#", "a/b/c", true),
            ("#", "a/b", true),
            ("+/+", "/a", true),
            ("+", "$SYS", false),
            ("#", "$SYS/x", false),
            ("$SYS/#", "$SYS/x", true),
            ("a/b", "a/c", false),
            ("a/+/c", "a/b/c", true),
        ];
        for (f, t, want) in cases {
            assert_eq!(matches(f, t), want, "{f} {t}");
        }
    }

    #[test]
    fn share_prefix_splits_into_group_and_filter() {
        let s = SharedFilter::parse("$share/g1/a/+").unwrap().unwrap();
        assert_eq!((s.group, s.filter), ("g1", "a/+"));
        assert!(SharedFilter::parse("a/b").is_none());
    }
}

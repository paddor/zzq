//! The result cache: JSONL rows that `tmp/mqtt-bench/run.rb` appends.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::path::PathBuf;

use serde::Deserialize;
use serde::de::DeserializeOwned;

/// `$XDG_CACHE_HOME/zzq`, else `~/.cache/zzq`.
pub(crate) fn cache_dir() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .filter(|dir| !dir.is_empty())
        .map_or_else(
            || PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".cache"),
            PathBuf::from,
        )
        .join("zzq")
}

/// The rows of one cache file, in file order. A missing file has none, and
/// lines that do not parse are reported and skipped.
pub(crate) fn load<T: DeserializeOwned>(name: &str) -> Vec<T> {
    let path = cache_dir().join(name);
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .filter_map(|(i, line)| {
            serde_json::from_str(line)
                .map_err(|e| eprintln!("{}:{}: {e}", path.display(), i + 1))
                .ok()
        })
        .collect()
}

/// One broker result: throughput or round trips.
#[derive(Debug, Deserialize)]
pub(crate) struct BrokerRow {
    pub(crate) run_id: String,
    #[serde(rename = "impl")]
    pub(crate) impl_name: String,
    pub(crate) version: Option<String>,
    pub(crate) config: String,
    pub(crate) kind: String,
    pub(crate) scenario: Option<String>,
    pub(crate) pubs: Option<u32>,
    pub(crate) subs: Option<u32>,
    pub(crate) qos: u8,
    pub(crate) msg_size: u64,
    /// Delivered messages per second.
    pub(crate) msgs_s: Option<f64>,
    pub(crate) p50_us: Option<f64>,
    pub(crate) p99_us: Option<f64>,
    pub(crate) p999_us: Option<f64>,
    /// Broker CPU over the measurement, in percent of one core.
    pub(crate) cpu_pct: Option<f64>,
}

/// One client result.
#[derive(Debug, Deserialize)]
pub(crate) struct ClientRow {
    pub(crate) run_id: String,
    #[serde(rename = "impl")]
    pub(crate) impl_name: String,
    pub(crate) version: Option<String>,
    /// `pub` or `sub`.
    pub(crate) kind: String,
    pub(crate) qos: u8,
    pub(crate) msg_size: u64,
    pub(crate) msgs_s: f64,
    /// Client CPU over its run, in percent of one core.
    pub(crate) cpu_pct: Option<f64>,
}

/// One step of a fixed-load run.
#[derive(Debug, Deserialize)]
pub(crate) struct FixedRow {
    pub(crate) run_id: String,
    #[serde(rename = "impl")]
    pub(crate) impl_name: String,
    pub(crate) version: Option<String>,
    pub(crate) config: String,
    pub(crate) scenario: String,
    /// Offered deliveries per second.
    pub(crate) offered_s: u64,
    /// Delivered messages per second.
    pub(crate) msgs_s: f64,
    pub(crate) p50_us: f64,
    pub(crate) p99_us: f64,
    pub(crate) p999_us: f64,
    pub(crate) cpu_pct: Option<f64>,
    pub(crate) rss_mib: Option<f64>,
    /// Connections the broker closed, or that never connected.
    #[serde(default)]
    pub(crate) closed: u64,
    pub(crate) failed: bool,
}

/// Keeps the row of the newest run per key. A tie goes to the later row.
pub(crate) fn newest<K: Ord, R>(
    rows: impl IntoIterator<Item = (K, R)>,
    run_id: impl Fn(&R) -> &str,
) -> BTreeMap<K, R> {
    let mut out = BTreeMap::new();
    for (key, row) in rows {
        match out.entry(key) {
            Entry::Vacant(e) => {
                e.insert(row);
            }
            Entry::Occupied(mut e) => {
                if run_id(&row) >= run_id(e.get()) {
                    e.insert(row);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_newest_run_wins_and_a_tie_goes_to_the_later_row() {
        let rows = [
            ("a", ("2026-10-24T10:00:00Z", 1)),
            ("a", ("2026-10-23T10:00:00Z", 2)),
            ("b", ("2026-10-24T10:00:00Z", 3)),
            ("b", ("2026-10-24T10:00:00Z", 4)),
        ];
        let kept = newest(rows, |r| r.0);
        assert_eq!(kept["a"].1, 1, "an older run comes later in the file");
        assert_eq!(kept["b"].1, 4);
    }
}

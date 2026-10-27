//! Who a result belongs to, and the rows it adds to the cache.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::parse::Row;

/// The fields that name a cell, without its numbers.
const CELL: [&str; 8] = [
    "impl", "config", "kind", "scenario", "qos", "msg_size", "pubs", "subs",
];

/// A UTC date and time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Civil {
    year: i64,
    month: u32,
    day: u32,
    hour: u64,
    min: u64,
    sec: u64,
}

impl Civil {
    /// The time of Unix second `secs`.
    pub(crate) fn of(secs: u64) -> Civil {
        let rem = secs % 86_400;
        // Howard Hinnant's civil_from_days.
        let shifted = i64::try_from(secs / 86_400).unwrap_or(0) + 719_468;
        let era = shifted.div_euclid(146_097);
        let doe = shifted.rem_euclid(146_097);
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
        Civil {
            year: yoe + era * 400 + i64::from(month <= 2),
            month,
            day: (doy - (153 * mp + 2) / 5 + 1) as u32,
            hour: rem / 3600,
            min: rem / 60 % 60,
            sec: rem % 60,
        }
    }

    fn now() -> Civil {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Civil::of(secs)
    }

    fn date(&self) -> String {
        format!("{}-{:02}-{:02}", self.year, self.month, self.day)
    }

    fn time(&self) -> String {
        format!("{:02}:{:02}:{:02}", self.hour, self.min, self.sec)
    }
}

/// `2026-10-27T22:52:31Z`: a run's ID.
pub(crate) fn stamp() -> String {
    let t = Civil::now();
    format!("{}T{}Z", t.date(), t.time())
}

/// `2026-10-27 22:52:31 UTC`, for log headers.
pub(crate) fn now() -> String {
    let t = Civil::now();
    format!("{} {} UTC", t.date(), t.time())
}

/// `2026-10-27`.
pub(crate) fn today() -> String {
    Civil::now().date()
}

/// The short revision of the repo at `repo`, with `-dirty` if `paths` have
/// uncommitted changes.
pub(crate) fn revision(repo: &std::path::Path, paths: &[&str]) -> String {
    let git = |args: &[&str]| {
        Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default()
    };
    let rev = git(&["rev-parse", "--short", "HEAD"]);
    let mut status = vec!["status", "--porcelain", "--untracked-files=no", "--"];
    status.extend_from_slice(paths);
    if git(&status).is_empty() {
        rev
    } else {
        format!("{rev}-dirty")
    }
}

/// A row from `(key, value)` pairs.
pub(crate) fn fields(pairs: &[(&str, Value)]) -> Row {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

/// Where results go.
#[derive(Debug)]
pub(crate) struct Cache {
    dir: PathBuf,
    run_id: String,
    /// Print rows instead of recording them.
    dry: bool,
}

impl Cache {
    pub(crate) fn new(dir: PathBuf, dry: bool) -> Cache {
        Cache {
            dir,
            run_id: stamp(),
            dry,
        }
    }

    /// Appends `row` for `who` to `<file>.jsonl`, synced.
    pub(crate) fn record(&self, file: &str, who: &Row, row: Row) -> Result<(), String> {
        let mut all = fields(&[("run_id", self.run_id.clone().into())]);
        all.extend(who.clone());
        all.extend(row);
        let line = Value::Object(all).to_string();
        if self.dry {
            println!("row {line}");
            return Ok(());
        }
        let path = self.dir.join(format!("{file}.jsonl"));
        fs::create_dir_all(&self.dir)
            .and_then(|()| OpenOptions::new().create(true).append(true).open(&path))
            .and_then(|mut f| {
                writeln!(f, "{line}")?;
                f.sync_all()
            })
            .map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Whether `<file>.jsonl` has a row for `who` and `cell` already.
    pub(crate) fn has(&self, file: &str, who: &Row, cell: &Row) -> bool {
        let mut want = who.clone();
        want.extend(cell.clone());
        let want = key(&want);
        let Ok(text) = fs::read_to_string(self.dir.join(format!("{file}.jsonl"))) else {
            return false;
        };
        text.lines()
            .filter_map(|l| serde_json::from_str::<Row>(l).ok())
            .any(|r| key(&r) == want)
    }
}

fn key(row: &Row) -> Vec<(&str, &Value)> {
    CELL.iter()
        .filter_map(|&k| row.get(k).map(|v| (k, v)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_time_matches_known_dates() {
        let at = |secs| {
            let t = Civil::of(secs);
            format!("{} {}", t.date(), t.time())
        };
        assert_eq!(at(0), "1970-01-01 00:00:00");
        assert_eq!(at(951_782_400), "2000-02-29 00:00:00");
        assert_eq!(at(1_793_141_551), "2026-10-27 22:52:31");
        assert_eq!(stamp().len(), 20);
        assert!(now().ends_with(" UTC") && today().len() == 10);
    }

    #[test]
    fn the_revision_names_this_repo() {
        let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let rev = revision(repo, &["zzq-bench"]);
        assert!(rev.len() >= 7, "{rev}");
    }

    #[test]
    fn recorded_rows_are_found_by_cell() {
        let dir = std::env::temp_dir().join(format!("zzq-suite-cache-{}", std::process::id()));
        let cache = Cache::new(dir.clone(), false);
        let who = fields(&[("impl", "zzq-1io".into()), ("config", "default".into())]);
        let cell = fields(&[("kind", "rtt".into()), ("qos", 1.into())]);
        assert!(!cache.has("brokers", &who, &cell));
        let mut row = cell.clone();
        row.insert("p50_us".into(), 20.5.into());
        cache.record("brokers", &who, row).unwrap();
        assert!(cache.has("brokers", &who, &cell));
        let other = fields(&[("kind", "rtt".into()), ("qos", 0.into())]);
        assert!(!cache.has("brokers", &who, &other));
        let text = fs::read_to_string(dir.join("brokers.jsonl")).unwrap();
        assert!(text.contains("\"run_id\"") && text.contains("\"p50_us\":20.5"));
        Cache::new(dir.clone(), true)
            .record("brokers", &who, other)
            .unwrap();
        assert_eq!(fs::read_to_string(dir.join("brokers.jsonl")).unwrap(), text);
        fs::remove_dir_all(dir).unwrap();
    }
}

//! Where the suite finds its tools, the rivals and its configs.

use std::path::{Path, PathBuf};

/// Locations, from the environment.
#[derive(Debug, Clone)]
pub(crate) struct Paths {
    /// The zzq tools, `zzq_broker` and the rival client benches:
    /// `ZZQ_BENCH_BIN`, else the directory of this binary.
    pub(crate) bin: PathBuf,
    /// Rival brokers and emqtt, laid out as `RUNNING_BENCHMARKS.md` says:
    /// `ZZQ_RIVALS`.
    pub(crate) rivals: Option<PathBuf>,
    /// Where broker output goes: `ZZQ_BROKER_LOG`, else nowhere.
    pub(crate) broker_log: Option<PathBuf>,
    /// `zzq-bench/harness`.
    pub(crate) harness: PathBuf,
    /// The repo, for the zzq revision.
    pub(crate) repo: PathBuf,
}

impl Paths {
    pub(crate) fn from_env() -> Paths {
        let env = |k: &str| {
            std::env::var_os(k)
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
        };
        let here = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf))
            .unwrap_or_default();
        let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        Paths {
            bin: env("ZZQ_BENCH_BIN").unwrap_or(here),
            rivals: env("ZZQ_RIVALS"),
            broker_log: env("ZZQ_BROKER_LOG"),
            harness: crate_dir.join("harness"),
            repo: crate_dir.parent().unwrap_or(crate_dir).to_path_buf(),
        }
    }

    /// A binary in [`Paths::bin`].
    pub(crate) fn tool(&self, name: &str) -> PathBuf {
        self.bin.join(name)
    }

    /// `rel` under [`Paths::rivals`].
    ///
    /// # Errors
    ///
    /// If `ZZQ_RIVALS` is not set.
    pub(crate) fn rival(&self, rel: &str) -> Result<PathBuf, String> {
        self.rivals
            .as_ref()
            .map(|r| r.join(rel))
            .ok_or_else(|| format!("set ZZQ_RIVALS to find {rel}"))
    }

    /// A broker config in `harness/conf`.
    pub(crate) fn conf(&self, name: &str) -> PathBuf {
        self.harness.join("conf").join(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_point_into_the_repo_and_the_rivals() {
        let mut p = Paths::from_env();
        assert!(p.conf("rmqtt-tuned.toml").exists());
        assert!(p.repo.join("Cargo.toml").exists());
        assert_eq!(p.tool("zzq_broker"), p.bin.join("zzq_broker"));
        p.rivals = None;
        assert!(p.rival("bin/rmqttd").is_err());
        p.rivals = Some("/r".into());
        assert_eq!(p.rival("bin/rmqttd").unwrap(), Path::new("/r/bin/rmqttd"));
    }
}

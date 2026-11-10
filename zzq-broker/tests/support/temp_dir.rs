//! A directory for one test's files, removed when the test ends.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// A path for one test's files, removed on drop, so a failed assert leaves
/// nothing behind. Not created: a broker creates its data directory.
///
/// Integration tests put it under Cargo's target tmp dir, on a disk file
/// system that takes `O_DIRECT`; library tests under the temp dir.
#[derive(Debug)]
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(name: &str) -> TempDir {
        static N: AtomicUsize = AtomicUsize::new(0);
        // Relaxed: only uniqueness matters.
        let n = N.fetch_add(1, Ordering::Relaxed);
        let base =
            option_env!("CARGO_TARGET_TMPDIR").map_or_else(std::env::temp_dir, PathBuf::from);
        let path = base.join(format!("zzq-{name}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        TempDir(path)
    }
}

impl std::ops::Deref for TempDir {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

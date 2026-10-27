//! The result cache: append-only JSONL files that the suite writes and the
//! charts read.

use std::path::PathBuf;

/// `$XDG_CACHE_HOME/zzq`, else `~/.cache/zzq`.
pub fn dir() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .filter(|dir| !dir.is_empty())
        .map_or_else(
            || PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".cache"),
            PathBuf::from,
        )
        .join("zzq")
}

//! Entry points for the fuzz targets in `fuzz/afl`. Not part of the API.

/// Replays `data` as a retained message log.
pub fn retained_log(data: &[u8]) {
    let _ = crate::persist::parse_log(data);
}

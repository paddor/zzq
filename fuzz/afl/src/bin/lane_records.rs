fn main() {
    afl::fuzz!(|data: &[u8]| zzq_fuzz::lane_records(data));
}

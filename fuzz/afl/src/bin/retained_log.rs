fn main() {
    afl::fuzz!(|data: &[u8]| zzq_fuzz::retained_log(data));
}

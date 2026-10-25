fn main() {
    afl::fuzz!(|data: &[u8]| zzq_fuzz::endpoint(data));
}

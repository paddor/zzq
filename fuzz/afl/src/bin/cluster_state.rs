fn main() {
    afl::fuzz!(|data: &[u8]| zzq_fuzz::cluster_state(data));
}

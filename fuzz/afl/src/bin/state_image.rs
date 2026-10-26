fn main() {
    afl::fuzz!(|data: &[u8]| zzq_fuzz::state_image(data));
}

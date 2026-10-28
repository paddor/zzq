fn main() {
    afl::fuzz!(|data: &[u8]| zzq_fuzz::journal_segment(data));
}

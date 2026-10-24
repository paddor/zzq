fn main() {
    afl::fuzz!(|data: &[u8]| zzq_fuzz::mqtt_sn(data));
}

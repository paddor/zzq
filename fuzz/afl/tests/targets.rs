//! Replays the seeds, their truncations and one-byte flips through every
//! target, plus inputs that once failed. Run with `cargo afl test`.

/// Minimized inputs that made a target fail, by target name.
const REGRESSIONS: &[(&str, &[u8])] = &[
    // Hosts that parsed but printed as URIs that did not.
    ("endpoint", b"tcp://[]"),
    ("endpoint", b"tcp://[[[1T]"),
    ("endpoint", b"tcp://q-:1]:0"),
    ("endpoint", b"udp://0.::1]:88::8::::4::::::::::0"),
    ("endpoint", b"wss://[]/8"),
];

fn variants(seed: &[u8]) -> Vec<Vec<u8>> {
    let mut out = vec![seed.to_vec()];
    out.extend((0..seed.len()).map(|n| seed[..n].to_vec()));
    out.extend((0..seed.len()).map(|i| {
        let mut v = seed.to_vec();
        v[i] ^= 0xff;
        v
    }));
    out
}

#[test]
fn every_target_survives_its_seeds_and_their_neighbors() {
    for (name, body) in zzq_fuzz::TARGETS {
        let seeds = zzq_fuzz::seeds::for_target(name);
        assert!(!seeds.is_empty(), "{name} has no seeds");
        for seed in &seeds {
            for input in variants(seed) {
                body(&input);
            }
        }
    }
}

#[test]
fn regressions_stay_fixed() {
    for (name, input) in REGRESSIONS {
        let (_, body) = zzq_fuzz::TARGETS
            .iter()
            .find(|(n, _)| n == name)
            .expect("known target");
        body(input);
    }
}

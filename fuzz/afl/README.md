# zzq fuzz targets

AFL targets for the inputs zzq reads from outside. The package is not in
the workspace, so AFL's instrumentation stays out of normal builds.

| Target | Input | Checks |
| --- | --- | --- |
| `mqtt` | MQTT packets, read as v3.1.1 and v5 | no panic, PUBLISH round trip |
| `mqtt_sn` | One MQTT-SN datagram | no panic, encode round trip |
| `websocket` | Upgrade request or response, and the frames after it | no panic |
| `retained_record` | A record of a retained message journal | no panic |
| `state_image` | A state image | no panic, restored sessions encode and decode again |
| `lane_records` | Records of a lane journal, each with its kind and length | no panic |
| `journal_segment` | A journal segment file | no panic, written records read back, damage keeps a prefix or fails |
| `endpoint` | An endpoint URI | no panic, print and parse round trip |
| `topics` | A filter and a topic, one per line | trie and `topic::matches` agree |

Install the runner with `cargo install cargo-afl`. Then:

```sh
cargo afl build --release
target/release/seeds
cargo afl fuzz -i seeds/mqtt -o out/mqtt target/release/mqtt
```

`seeds` builds each target's seeds with the real encoders into `seeds/`.
`cargo afl test --release` replays the seeds, their truncations and
one-byte flips, and the inputs in `REGRESSIONS` in `tests/targets.rs`.
Add a minimized crash there before fixing it. `seeds/` and `out/` are not
checked in.

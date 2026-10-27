# Running Benchmarks

How to produce the results in [BENCHMARKS.md](BENCHMARKS.md). Run one
benchmark at a time on an idle machine. Stop on warnings or timeouts and fix
the cause before running again.

## Build

```sh
cargo build --release -p zzq-bench --bins -p zzq-broker --bin zzq_broker
cargo build --release --manifest-path zzq-bench/harness/clients/Cargo.toml
```

The second command builds the rumqttc and ntex-mqtt benches. They form a
workspace of their own, so the main workspace never builds those clients.
With one `CARGO_TARGET_DIR` for both, every binary ends up in one directory,
and `zzq_suite` finds them next to itself. Otherwise set `ZZQ_BENCH_BIN` to
the directory that holds them all.

## Suites

```sh
zzq_suite zzq 1            # broker suite, zzq with 1 IO lane
zzq_suite rmqtt tuned      # a rival, tuned or default config
zzq_suite clients          # rumqttc, ntex-mqtt and emqtt against zzq_fakebroker
zzq_suite zzqclient        # zzq-client against zzq_fakebroker
zzq_suite fixed zzq 2      # fixed load, zzq with 2 IO lanes
zzq_suite fixed nanomq fixed
zzq_suite all              # every suite, one log each
zzq_chart                  # redraw doc/charts from the cache
```

- Brokers run on CPUs 0-3 and port 1883, the load generators on CPUs 4-5.
- Each run waits for at least 90% idle CPU and no compiler. A run that a
  compiler overlapped runs again.
- Results are appended to `brokers.jsonl`, `clients.jsonl` and
  `fixed.jsonl` in `$XDG_CACHE_HOME/zzq` (`~/.cache/zzq`). The charts show
  the newest run per cell.
- `all` logs each suite to `~/.cache/zzq/logs/<date>/<suite>.log` and
  stops at the first that fails. Name suites to run only those, such as
  `zzq_suite all zzq-1 fixed-zzq-1`.
- `--fill` skips cells the cache already has for that implementation and
  config.
- `--smoke` runs the first cell of each part briefly, without waiting for
  a quiet system, and prints the rows instead of recording them.
- `ZZQ_BROKER_LOG` keeps the brokers' output.

## Rivals

Broker configs are in `zzq-bench/harness/conf`, one per broker and config
(`default`, `tuned`, or `fixed` for the fixed-load test). EMQX, Mosquitto,
NanoMQ and XMQ run as podman containers. The others are builds under
`ZZQ_RIVALS`:

| Path | What |
| --- | --- |
| `bin/rmqttd`, `rmqtt/` | rmqtt 0.24.0, and its source tree, where it finds its plugin configs |
| `rumqttd/bin/rumqttd` | rumqttd 0.20.0 |
| `flashmq/bin/flashmq` | FlashMQ 1.27.3 |
| `fluxmq/bin/fluxmq` | FluxMQ 1.0.0 |
| `saguin/bin/saguin` | Sagüin 0.1.0-rc.1 |
| `vernemq-src/_build/default/rel/vernemq` | VerneMQ 2.2.1 built from source, with its generated `etc/vernemq.conf` copied to `etc/vernemq.conf.orig` |
| `emqtt-bench/lib` | emqtt 1.14.6 and its dependencies, from an emqtt-bench release |

- VerneMQ is built from source: its official images and packages come
  under an EULA that forbids competitive analysis.
- FluxMQ keeps its store in `$TMPDIR/zzq-suite`, so set `TMPDIR` to a disk.

## Perf gate

`zzq_perf_verify` measures zzq alone and compares throughput, latency and
memory with the thresholds in `.perf_hw` (ignored). Without that file it runs
a smaller smoke gate.

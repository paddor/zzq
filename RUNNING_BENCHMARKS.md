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
zzq_suite durable zzq 1    # storage on, zzq with 1 IO lane and a data dir
zzq_suite durable emqx     # a rival with its most durable storage
zzq_suite failover zzq     # a cluster of three, its leader killed under load
zzq_suite failover vernemq netsplit
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

## Storage

`durable` runs QoS 1 and 2 to subscribers whose sessions outlive their
connection, with each broker's storage on (`durable` configs), and times
the PUBACK. Each cell gets a fresh broker with empty stores, in
`$TMPDIR/zzq-suite/<name>`: set `TMPDIR` to the disk under test. Stores
that containers wrote are removed with `podman unshare`. XMQ and rmqtt
keep messages in a Redis container that syncs every write. FluxMQ 1.0.0
refuses persistent sessions on one node, so it has no durable run.

## Failover

`failover` starts a fresh cluster of three for each cell, node `i` on CPU
`i` with MQTT on port 1883 + `i`, and runs `zzq_failover` on the load
CPUs. It kills or stops the node the publishers use mid-run and records
the gap without PUBACKs, and what the subscribers lost, got twice or out
of order, into `failover.jsonl`. zzq nodes keep state files in
`$TMPDIR/zzq-suite/failover`. VerneMQ runs as three copies of its release
in `$TMPDIR/zzq-suite/failover-vernemq`, joined to node 0, with the
`cluster` config (its netsplit defaults) or `netsplit` (it serves during
one). TBMQ runs as three containers beside one PostgreSQL, Kafka and
Valkey, with 2 GiB heaps; node 0 installs the database. NATS runs as
three processes routed to each other, with the MQTT streams on all three
(`nats-cluster.conf`) and their stores in
`$TMPDIR/zzq-suite/failover-nats`.

## Rivals

Broker configs are in `zzq-bench/harness/conf`, one per broker and config
(`default`, `tuned`, `durable`, or `fixed` for the fixed-load test). EMQX,
Mosquitto, NanoMQ, XMQ, HiveMQ CE and TBMQ run as podman containers. The
others are builds under `ZZQ_RIVALS`:

| Path | What |
| --- | --- |
| `bin/rmqttd`, `rmqtt/` | rmqtt 0.24.0, and its source tree, where it finds its plugin configs |
| `rumqttd/bin/rumqttd` | rumqttd 0.20.0 |
| `flashmq/bin/flashmq` | FlashMQ 1.27.3 |
| `fluxmq/bin/fluxmq` | FluxMQ 1.0.0 |
| `saguin/bin/saguin` | Sagüin 0.1.0-rc.1 |
| `nats/bin/nats-server` | NATS 2.15.2, the `linux-amd64` release binary |
| `vernemq-src/_build/default/rel/vernemq` | VerneMQ 2.2.1 built from source, with its generated `etc/vernemq.conf` copied to `etc/vernemq.conf.orig` |
| `emqtt-bench/lib` | emqtt 1.14.6 and its dependencies, from an emqtt-bench release |

- VerneMQ is built from source: its official images and packages come
  under an EULA that forbids competitive analysis.
- FluxMQ, HiveMQ CE, TBMQ and NATS keep their stores in
  `$TMPDIR/zzq-suite` in every config, so set `TMPDIR` to a disk. NATS
  needs JetStream for MQTT, and speaks MQTT 3.1.1 only, so its runs do
  too.
- TBMQ runs with PostgreSQL 17, Kafka 4.0 and Valkey 8 containers, all on
  the broker CPUs, and needs ports 5432, 6379, 8083, 8084, 9092 and 9093.
  After start, the suite turns off its basic auth provider over the REST
  API, so clients without credentials get in, and waits until a QoS 1
  message gets through: TBMQ listens before its Kafka consumers are ready.

## Perf gate

`zzq_perf_verify` measures zzq alone and compares throughput, latency and
memory with the thresholds in `.perf_hw` (ignored). Without that file it runs
a smaller smoke gate.

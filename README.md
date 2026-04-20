# ZZQ — async-native MQTT for Ruby

> Codename **Zanzara** — Italian for "mosquito", in the same spirit as
> the Mosquitto broker that popularised MQTT. `zzq` keeps the
> three-letter gem-name symmetry with its siblings `omq` and `nnq`.

[![CI](https://github.com/paddor/zzq/actions/workflows/ci.yml/badge.svg)](https://github.com/paddor/zzq/actions/workflows/ci.yml)
[![Gem Version](https://img.shields.io/gem/v/zzq?color=e9573f)](https://rubygems.org/gems/zzq)
[![License: ISC](https://img.shields.io/badge/License-ISC-blue.svg)](LICENSE)
[![Ruby](https://img.shields.io/badge/Ruby-%3E%3D%204.0-CC342D?logo=ruby&logoColor=white)](https://www.ruby-lang.org)

ZZQ is a pure-Ruby MQTT 3.1.1 + 5.0 implementation — **client and
broker in one gem** — built on [async](https://github.com/socketry/async),
[io-stream](https://github.com/socketry/io-stream), and
[protocol-mqtt](https://github.com/paddor/protocol-mqtt). It applies
the [omq](https://github.com/paddor/omq) / [nnq](https://github.com/paddor/nnq)
architecture to MQTT.

Status: pre-alpha. See [the design plan](../../.claude/plans/omq-and-nnq-s-design-robust-dolphin.md) for scope and roadmap.

## Quickstart

```ruby
require "zzq"
require "async"

# Client
Async do
  client = ZZQ::Client.new(client_id: "sensor-42", version: 5, keepalive: 30)
  client.connect("mqtt://broker.example:1883") do |task|
    sub = client.subscribe("cmd/#", qos: 1)
    task.async do
      sub.each { |msg| puts "#{msg.topic}: #{msg.payload}" }
    end
    client.publish("status/sensor-42", "online", qos: 1, retain: true)
  end
end

# Broker
Async do
  ctx = OpenSSL::SSL::SSLContext.new
  ctx.cert = OpenSSL::X509::Certificate.new(File.read("server.crt"))
  ctx.key  = OpenSSL::PKey.read(File.read("server.key"))
  # Recommended: TLS 1.3 with DJB-family AEAD.
  ctx.min_version  = OpenSSL::SSL::TLS1_3_VERSION
  ctx.max_version  = OpenSSL::SSL::TLS1_3_VERSION
  ctx.ciphersuites = "TLS_CHACHA20_POLY1305_SHA256"

  broker = ZZQ::Broker.new
  broker.bind("mqtt://0.0.0.0:1883")
  broker.bind("mqtts://0.0.0.0:8883", tls_context: ctx)
  # ... bind more transports on the same broker
end
```

## Transports

- `mqtt://` — plain MQTT over TCP (default port 1883).
- `mqtts://` — MQTT over TLS (default port 8883). Takes any
  `OpenSSL::SSL::SSLContext` via `tls_context:`; the recommended
  configuration is TLS 1.3 pinned to `TLS_CHACHA20_POLY1305_SHA256`
  as shown above. The transport drops the connection at 128 GiB
  of application data — well below ChaCha20-Poly1305's ~256 GiB
  per-key integrity bound — so reconnects force fresh keys before
  AEAD nonce counters approach their unsafe range.
- `ws://` / `wss://` — MQTT over WebSockets.
- `ipc:///path` — Unix-domain socket, filesystem path.
- `ipc://@abstract` — Linux abstract-namespace Unix socket.

## Persistence

By default the broker keeps retained messages in process memory — fast,
zero-config, and lost when the process exits. That's the right choice
for tests and ephemeral workloads.

For durability across restarts, pass a `PStore`-backed persistence
object. It uses stdlib [`PStore`](https://docs.ruby-lang.org/en/master/PStore.html)
with `ultra_safe = true` so every commit fsyncs the data file and its
parent directory — retained state survives power loss, not just clean
shutdowns.

```ruby
# Default: in-memory, ephemeral
broker = ZZQ::Broker.new

# Explicit in-memory (same as default)
broker = ZZQ::Broker.new persistence: ZZQ::Persistence::Memory.new

# Durable: retained messages persist to ./zzq-data/
persistence = ZZQ::Persistence::PStore.new data_dir: "./zzq-data"
broker = ZZQ::Broker.new persistence: persistence
```

PStore is single-process only — file locking doesn't coordinate across
processes. For HA deployments, a future external adapter (Redis,
SQLite) will ship as a separate gem.

## Bridging two brokers

ZZQ does not ship a `Bridge` class — broker-to-broker forwarding is a
short composition of a `Client` and a local `Broker`. Subscribe on one
side, publish to the other:

```ruby
require "zzq"
require "async"

Async do |task|
  upstream = ZZQ::Client.new(client_id: "site-42-bridge", version: 5)
  local    = ZZQ::Broker.new

  upstream.connect("mqtts://cloud.example:8883")
  local.bind("mqtt://0.0.0.0:1883")

  # local → upstream: forward telemetry out.
  task.async do
    local.subscribe("telemetry/#", qos: 1).each do |m|
      upstream.publish(m.topic, m.payload, qos: m.qos, retain: m.retain)
    end
  end

  # upstream → local: pull commands in. `Broker#<<` fans the Message
  # straight into the local broker's delivery path — no need to pick
  # apart and re-pack fields (alias of `#ingest`).
  task.async do
    upstream.subscribe("cmd/#", qos: 1, no_local: true).each do |m|
      local << m
    end
  end
end
```

Loop guard for two-way routes:

* MQTT v5: pass `no_local: true` on the subscribe call — the spec
  forbids the broker from echoing back to the publisher of the
  subscription, so messages originating on one side don't bounce.
* MQTT v3.1.1: no `no_local`. Use non-overlapping filters on each
  direction, or tag topics with a side-specific prefix.

## When MQTT, when OMQ/NNQ

MQTT bridging makes sense only when the remote speaks MQTT and nothing
else (AWS IoT Core, HiveMQ Cloud, partner Mosquitto). For inter-service
comm where both ends are under your control, reach for
[omq](https://github.com/paddor/omq) or [nnq](https://github.com/paddor/nnq)
— cleaner wire format, no protocol overhead. ZZQ exposes
`ZZQ::Message#to_wire` / `.from_wire` so you can ship MQTT messages
across an OMQ/NNQ socket without re-encoding.

## License

ISC. See [LICENSE](LICENSE).

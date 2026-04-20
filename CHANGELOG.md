# Changelog

## v0.1.0 — 2026-04-20

- First release. Async-native MQTT 3.1.1 + 5.0 client and broker in
  one gem, built on `async`, `io-stream`, and `protocol-mqtt`.
- Transports: `mqtt://` (plain TCP, port 1883), `mqtts://` (TLS,
  port 8883), `ipc://` (Unix-domain sockets, filesystem and Linux
  abstract namespace).
- `mqtts://` accepts any `OpenSSL::SSL::SSLContext` and drops the
  connection at 128 GiB of application data so reconnects force
  fresh keys before AEAD nonce counters approach their unsafe
  range.
- Retained-message persistence: in-memory (default) or stdlib
  `PStore` with `ultra_safe = true` for fsync-on-commit durability.
- Broker-to-broker bridging via `Client` + `Broker` composition;
  MQTT v5 `no_local` loop guard supported.

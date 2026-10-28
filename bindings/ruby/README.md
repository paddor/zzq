# zzq for Ruby

MQTT 3.1.1 and 5.0 client and broker for Ruby, with an MQTT-SN gateway.
The `zzq` gem binds the [zzq](https://github.com/paddor/zzq) Rust crates:
connections run on Rust IO threads, and Ruby only hands messages in and
out.

Calls wait without blocking other fibers under a fiber scheduler such as
[Async](https://github.com/socketry/async), and release the GVL in plain
threads.

## Install

```sh
gem install zzq
```

The gem needs Ruby 3.4 or newer. It builds its native extension on
install, which needs Rust 1.93 or newer.

## Client

```ruby
require "zzq"

ZZQ::Client.connect("mqtt://localhost", client_id: "sensor-42") do |client|
  client.subscribe("cmd/#", qos: 1)
  client.publish("status/sensor-42", "online", qos: 1, retain: true)
  client.each { |message| puts "#{message.topic}: #{message.payload}" }
end
```

- `publish(topic, payload, qos:, retain:)` returns once queued for QoS 0,
  and once acked for QoS 1 and 2.
- `subscribe(filter, qos:)` returns the granted QoS. MQTT 5 options:
  `no_local:`, `retain_as_published:`, `retain_handling:`,
  `subscription_id:`.
- `receive(timeout:)` returns the next `ZZQ::Message`, or nil once the
  client is closed, and raises `IO::TimeoutError` if none came in time. `each` yields messages until then.
- Publishing works from any fiber or thread; one fiber at a time
  receives.

Options of `ZZQ::Client.new` and `.connect`: `client_id:`, `version:` (5 or
3), `clean_start:`, `keep_alive:`, `session_expiry:`, `username:`,
`password:`, `will:` (`topic:`, `payload:`, `qos:`, `retain:`, `delay:`),
`inbox:`, `reconnect:` (true, or `min:`/`max:` seconds), `connect_timeout:`
and `tls:` (`ca:`, `cert:`, `key:` as PEM strings, `server_name:`,
`insecure:`).

## Broker

```ruby
broker = ZZQ::Broker.new(data_dir: "/var/lib/zzq")
broker.bind("mqtt://0.0.0.0:1883")
broker.bind("ws://0.0.0.0:8080")
broker.publish("status/broker", "up", retain: true)
broker.stats # => {connections: 0, sessions: 0, ...}
broker.close
```

Options: the limits `max_packet_size:`, `max_inflight:`, `max_queued:`,
`max_queued_bytes:`, `receive_maximum:`, `max_subscriptions:`,
`max_retained:`, `max_topic_levels:`, `topic_alias_maximum:`,
`max_session_expiry:`, `max_connections:`, `max_state_bytes:` and
`max_keep_alive:` of zzq-broker's `BrokerOptions`,
plus `sys_interval:`, `connect_timeout:`, `data_dir:`, `state_file:` and
`tls:` (`cert:`, `key:`, `client_ca:`). With `data_dir:` (Linux only),
retained messages, sessions and their QoS 1/2 messages stay on disk, and
acks wait until what they stand for is there. With `state_file:`, `#close` writes the sessions there,
and the next broker started with it resumes them.

## Async

```ruby
require "async"

Async do |task|
  client = ZZQ::Client.connect("mqtt://localhost")
  client.subscribe("jobs")
  task.async { client.each { |message| handle(message) } }
  10.times { |i| task.async { client.publish("jobs", i.to_s, qos: 1) } }
end
```

## Transports

`mqtt://` and `tcp://`, `mqtts://` and `tls://`, `ws://`, `wss://`,
`quic://`, `ipc://`, `inproc://`, and `udp://` for an MQTT-SN gateway on
the broker.

## IO threads

Clients and brokers of a process share its IO lanes, one by default. Set
`ZZQ.io_threads = n` before the first client or broker to use more.

## License

Licensed under either of [ISC](LICENSE-ISC) or
[Apache-2.0](LICENSE-APACHE), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

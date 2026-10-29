# zzq for Ruby

MQTT 3.1.1 and 5.0 client and broker for Ruby, with an MQTT-SN gateway.
The gem binds the [zzq](https://github.com/paddor/zzq) Rust crates.

## Why zzq

- **Rust IO threads.** Connections run in Rust. Ruby only hands messages
  in and out.
- **Fiber-friendly.** Calls wait without blocking other fibers under a
  fiber scheduler such as [Async](https://github.com/socketry/async).
- **Thread-friendly.** Calls release the GVL in plain threads.
- **Correct under load.** A slow subscriber slows its publishers, and the
  broker avoids acking messages it would later drop.
- **Durable.** With `data_dir:`, the broker keeps sessions and messages
  on disk across restarts and crashes.

## Install

```sh
gem install zzq
```

Needs Ruby 3.4 or newer, and Rust 1.93 or newer to build the native
extension.

## Example

```ruby
require "zzq"

broker = ZZQ::Broker.new
broker.bind("mqtt://0.0.0.0:1883")

ZZQ::Client.connect("mqtt://localhost", client_id: "sensor-42") do |client|
  client.subscribe("sensors/#", qos: 1)
  client.publish("sensors/temp", "21.5", qos: 1)
  message = client.receive
  puts "#{message.topic}: #{message.payload}"
end

broker.close
```

## Further reading

- [API docs](https://rubydoc.info/gems/zzq): methods and options of
  `ZZQ::Client` and `ZZQ::Broker`.
- [zzq](https://github.com/paddor/zzq): why zzq, transports, durability,
  benchmarks and architecture.

## License

Licensed under either of [ISC](LICENSE-ISC) or
[Apache-2.0](LICENSE-APACHE), at your option.

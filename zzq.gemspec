# frozen_string_literal: true

require_relative "lib/zzq/version"

Gem::Specification.new do |s|
  s.name     = "zzq"
  s.version  = ZZQ::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "Pure Ruby MQTT 3.1.1 + 5.0 client and broker on Async"
  s.description = "Pure Ruby implementation of MQTT — client and broker " \
                  "in one gem — built on async, io-stream, and " \
                  "protocol-mqtt. Fiber-first, pluggable transports " \
                  "(tcp, tls, ws, ipc), pluggable persistence " \
                  "(in-memory, PStore)."
  s.homepage = "https://github.com/paddor/zzq"
  s.license  = "ISC"

  s.required_ruby_version = ">= 4.0"

  s.files = Dir["lib/**/*.rb", "README.md", "LICENSE", "CHANGELOG.md"]

  s.add_dependency "async", "~> 2.38"
  s.add_dependency "io-stream", "~> 0.11"
  s.add_dependency "protocol-mqtt", ">= 0.1.0"
  s.add_dependency "pstore", "~> 0.2"
end

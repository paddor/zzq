# frozen_string_literal: true

require_relative "lib/zzq/version"

Gem::Specification.new do |spec|
  spec.name        = "zzq"
  spec.version     = ZZQ::VERSION
  spec.authors     = ["Patrik Wenger"]
  spec.email       = ["paddor@gmail.com"]
  spec.summary     = "MQTT 3.1.1 and 5.0 client and broker for Ruby, backed by Rust"
  spec.description = "Ruby binding for zzq: an MQTT 3.1.1 and 5.0 client and broker with an " \
                     "MQTT-SN gateway, running on Rust IO threads. Works in threads and " \
                     "under fiber schedulers such as Async."
  spec.homepage    = "https://github.com/paddor/zzq/tree/main/bindings/ruby"
  spec.license     = "ISC"
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.required_ruby_version = ">= 3.3"

  spec.files = Dir[
    "lib/**/*.rb",
    "ext/**/*.{rs,rb}",
    "ext/**/Cargo.toml",
    "README.md",
    "LICENSE",
    "CHANGELOG.md",
  ]
  spec.require_paths = ["lib"]
  spec.extensions    = ["ext/zzq_native/extconf.rb"]

  spec.add_dependency "rb_sys", "~> 0.9"
end

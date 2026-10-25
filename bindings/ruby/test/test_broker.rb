# frozen_string_literal: true

require_relative "test_helper"
require "openssl"
require "tmpdir"

class BrokerTest < Minitest::Test
  def test_publish_as_the_broker_and_count
    b = broker
    c = client(tcp_endpoint(b))
    c.subscribe("news/#")
    b.publish("news/1", "from the broker")
    b.publish("news/kept", "retained", retain: true)
    assert_equal "from the broker", c.receive(timeout: 2).payload
    assert_equal "retained", c.receive(timeout: 2).payload
    stats = b.stats
    assert_equal 1, stats[:connections]
    assert_equal 1, stats[:retained]
    assert_equal 2, stats[:deliveries]
  end

  def test_retained_messages_survive_a_restart
    Dir.mktmpdir do |dir|
      path = File.join(dir, "retained.log")
      ZZQ::Broker.new(persistence: path, sys_interval: nil).then do |b|
        b.publish("keep", "me", retain: true)
        b.close
      end
      b = broker(persistence: path)
      c = client(tcp_endpoint(b))
      c.subscribe("keep")
      assert_equal "me", c.receive(timeout: 2).payload
    end
  end

  def test_a_closed_broker_refuses_calls
    b = ZZQ::Broker.new(sys_interval: nil)
    b.close
    assert_predicate b, :closed?
    assert_raises(ZZQ::ClosedError) { b.publish("t", "x") }
    assert_nil b.close
  end

  def test_unknown_option
    assert_raises(ArgumentError) { ZZQ::Broker.new(max_clients: 1) }
  end

  def test_tls
    key = OpenSSL::PKey::EC.generate("prime256v1")
    cert = self_signed(key, "localhost")
    b = broker(tls: { cert: cert.to_pem, key: key.private_to_pem })
    endpoint = b.bind("mqtts://127.0.0.1:0")
    assert_match %r{\Atls://}, endpoint
    c = client(endpoint, tls: { ca: cert.to_pem, server_name: "localhost" })
    c.subscribe("secure")
    c.publish("secure", "yes")
    assert_equal "yes", c.receive(timeout: 2).payload
    assert_raises(ZZQ::TransportError) { ZZQ::Client.connect(endpoint) }
  end

  private

  def self_signed(key, name)
    cert = OpenSSL::X509::Certificate.new
    cert.version = 2
    cert.serial = 1
    cert.subject = cert.issuer = OpenSSL::X509::Name.parse("/CN=#{name}")
    cert.public_key = key
    cert.not_before = Time.now - 60
    cert.not_after = Time.now + 3600
    ext = OpenSSL::X509::ExtensionFactory.new(cert, cert)
    cert.add_extension(ext.create_extension("basicConstraints", "CA:FALSE", true))
    cert.add_extension(ext.create_extension("subjectAltName", "DNS:#{name}"))
    cert.sign(key, OpenSSL::Digest.new("SHA256"))
    cert
  end
end

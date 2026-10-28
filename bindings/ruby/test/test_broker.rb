# frozen_string_literal: true

require_relative "test_helper"
require "openssl"
require "tmpdir"

class BrokerTest < Minitest::Spec
  WAIT = 2

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

  it "publishes as the broker and counts" do
    b = broker
    c = client(tcp_endpoint(b))
    c.subscribe("news/#")
    b.publish("news/1", "from the broker")
    b.publish("news/kept", "retained", retain: true)
    assert_equal "from the broker", c.receive(timeout: WAIT).payload
    assert_equal "retained", c.receive(timeout: WAIT).payload
    stats = b.stats
    assert_equal 1, stats[:connections]
    assert_equal 1, stats[:retained]
    assert_equal 2, stats[:deliveries]
    assert_equal 0, stats[:dropped_session_ended]
    assert_equal 0, stats[:dropped_state_full]
    assert_equal 0, stats[:full_journals]
    assert_operator stats[:state_bytes], :>, 0
  end

  it "drops the oldest queued messages past max_queued_bytes" do
    b = broker(max_queued_bytes: 300, max_state_bytes: 1 << 20)
    endpoint = tcp_endpoint(b)
    opts = { client_id: "away", clean_start: false, session_expiry: 60 }
    ZZQ::Client.connect(endpoint, **opts).then do |c|
      c.subscribe("q", qos: 1)
      c.close
    end
    eventually { b.stats[:connections].zero? }
    5.times { |i| b.publish("q", i.to_s * 100, qos: 1) }
    eventually { b.stats[:dropped_queue_full].positive? }
    again = client(endpoint, **opts)
    got = []
    got << again.receive(timeout: WAIT).payload[0] until got.last == "4"
    refute_includes got, "0"
  end

  it "keeps retained messages across a restart" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "data")
      ZZQ::Broker.new(data_dir: path, sys_interval: nil).then do |b|
        b.publish("keep", "me", retain: true)
        b.close
      end
      c = client(tcp_endpoint(broker(data_dir: path)))
      c.subscribe("keep")
      assert_equal "me", c.receive(timeout: WAIT).payload
    end
  end

  it "keeps sessions across a restart with a state file" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "state")
      opts = { client_id: "kept", clean_start: false, session_expiry: 60 }
      b = broker(state_file: path)
      endpoint = tcp_endpoint(b)
      ZZQ::Client.connect(endpoint, **opts).then do |c|
        c.subscribe("s", qos: 1)
        c.close
      end
      ZZQ::Client.connect(endpoint).then do |c|
        c.publish("s", "while down", qos: 1)
        c.close
      end
      b.close
      b = broker(state_file: path)
      assert_path_exists "#{path}.loaded"
      c = ZZQ::Client.connect(tcp_endpoint(b), **opts)
      assert_predicate c, :session_present?
      assert_equal "while down", c.receive(timeout: WAIT).payload
      c.close
      b.close
    end
  end

  it "writes the state file once a call in flight finishes" do
    Dir.mktmpdir do |dir|
      path = File.join(dir, "state")
      b = ZZQ::Broker.new(state_file: path, sys_interval: nil)
      in_flight = b.send(:native).stats
      closer = Thread.new { b.close }
      refute closer.join(0.2), "close went on while a call held the broker"
      ZZQ::Waiting.await(in_flight)
      assert closer.join(WAIT), "close did not finish"
      assert_path_exists path
    end
  end

  it "raises when it cannot write the state file" do
    Dir.mktmpdir do |dir|
      b = ZZQ::Broker.new(state_file: File.join(dir, "gone", "state"), sys_interval: nil)
      assert_raises(ZZQ::Error) { b.close }
      assert_predicate b, :closed?
    end
  end

  it "refuses calls once closed" do
    b = ZZQ::Broker.new(sys_interval: nil)
    b.close
    assert_predicate b, :closed?
    assert_raises(ZZQ::ClosedError) { b.publish("t", "x") }
    assert_nil b.close
  end

  it "raises on an unknown or out-of-range option" do
    assert_raises(ArgumentError) { ZZQ::Broker.new(max_clients: 1) }
    assert_raises(ArgumentError) { ZZQ::Broker.new(max_state_bytes: -1) }
  end

  it "serves TLS" do
    key = OpenSSL::PKey::EC.generate("prime256v1")
    cert = self_signed(key, "localhost")
    b = broker(tls: { cert: cert.to_pem, key: key.private_to_pem })
    endpoint = b.bind("mqtts://127.0.0.1:0")
    assert_match %r{\Atls://}, endpoint
    c = client(endpoint, tls: { ca: cert.to_pem, server_name: "localhost" })
    c.subscribe("secure")
    c.publish("secure", "yes")
    assert_equal "yes", c.receive(timeout: WAIT).payload
    assert_raises(ZZQ::TransportError) { ZZQ::Client.connect(endpoint) }
  end
end

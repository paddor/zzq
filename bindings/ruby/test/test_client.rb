# frozen_string_literal: true

require_relative "test_helper"

class ClientTest < Minitest::Spec
  WAIT = 2

  it "delivers every QoS on both versions" do
    endpoint = tcp_endpoint
    [3, 5].each do |version|
      sub = client(endpoint, client_id: "sub#{version}", version:)
      pub = client(endpoint, client_id: "pub#{version}", version:)
      sub.subscribe("q/#", qos: 2)
      [0, 1, 2].each { |qos| pub.publish("q/#{qos}", "m#{qos}", qos:) }
      got = 3.times.map { sub.receive(timeout: WAIT) }
      assert_equal %w[q/0 q/1 q/2], got.map(&:topic)
      assert_equal %w[m0 m1 m2], got.map(&:payload)
      assert_equal [0, 1, 2], got.map(&:qos)
    end
  end

  it "receives frozen messages with UTF-8 topics and binary payloads" do
    c = client(tcp_endpoint)
    c.subscribe("t")
    c.publish("t", "\xFF\x00".b)
    m = c.receive(timeout: WAIT)
    assert_predicate m.topic, :frozen?
    assert_equal Encoding::UTF_8, m.topic.encoding
    assert_predicate m.payload, :frozen?
    assert_equal Encoding::BINARY, m.payload.encoding
    assert_equal "\xFF\x00".b, m.payload
  end

  it "gives retained messages to later subscribers until cleared" do
    endpoint = tcp_endpoint
    pub = client(endpoint)
    pub.publish("r/a", "kept", qos: 1, retain: true)
    first = client(endpoint)
    first.subscribe("r/#", qos: 1)
    m = first.receive(timeout: WAIT)
    assert_equal ["r/a", "kept", true], [m.topic, m.payload, m.retain?]
    pub.publish("r/a", "", qos: 1, retain: true)
    later = client(endpoint)
    later.subscribe("r/#")
    assert_raises(IO::TimeoutError) { later.receive(timeout: 0.2) }
  end

  it "sends the will when a disconnect asks for it" do
    endpoint = tcp_endpoint
    watcher = client(endpoint)
    watcher.subscribe("status")
    will = { topic: "status", payload: "gone", qos: 1 }
    leaving = ZZQ::Client.connect(endpoint, client_id: "leaving", will:)
    leaving.disconnect(reason: 0x04)
    assert_equal "gone", watcher.receive(timeout: WAIT).payload
  end

  it "stops delivery after unsubscribe" do
    c = client(tcp_endpoint)
    c.subscribe("a")
    c.subscribe("b")
    c.unsubscribe("a")
    c.publish("a", "1")
    c.publish("b", "2")
    assert_equal "b", c.receive(timeout: WAIT).topic
  end

  it "resumes a session" do
    endpoint = tcp_endpoint
    opts = { client_id: "resume", clean_start: false, session_expiry: 60 }
    first = ZZQ::Client.connect(endpoint, **opts)
    refute_predicate first, :session_present?
    first.subscribe("s", qos: 1)
    first.close
    client(endpoint).publish("s", "while away", qos: 1)
    again = client(endpoint, **opts)
    assert_predicate again, :session_present?
    assert_equal "while away", again.receive(timeout: WAIT).payload
  end

  it "passes v5 subscription IDs with messages" do
    c = client(tcp_endpoint)
    c.subscribe("x/#", subscription_id: 7)
    c.publish("x/1", "v")
    assert_equal [7], c.receive(timeout: WAIT).subscription_ids
  end

  it "raises the reason code of a refused connect" do
    endpoint = tcp_endpoint(broker(max_connections: 1))
    client(endpoint, client_id: "first")
    error = assert_raises(ZZQ::ConnectionRefused) do
      ZZQ::Client.connect(endpoint, client_id: "second")
    end
    assert_equal 0x97, error.reason_code
  end

  it "raises on bad input" do
    c = client(tcp_endpoint)
    assert_raises(ZZQ::InvalidTopic) { c.publish("a/+", "x") }
    assert_raises(ZZQ::InvalidTopic) { c.subscribe("a/#/b") }
    assert_raises(ArgumentError) { c.publish("a", "x", qos: 3) }
    assert_raises(ArgumentError) { ZZQ::Client.new(keepalive: 30) }
    assert_raises(ZZQ::TransportError) { ZZQ::Client.connect("tcp://127.0.0.1:1") }
  end

  it "times out a receive" do
    c = client(tcp_endpoint)
    c.subscribe("quiet")
    assert_raises(IO::TimeoutError) { c.receive(timeout: 0.1) }
  end

  it "ends a waiting receive and each on close" do
    c = ZZQ::Client.connect(tcp_endpoint)
    c.subscribe("t")
    c.publish("t", "one")
    reader = Thread.new { c.each.map(&:payload) }
    sleep 0.1
    c.close
    assert_equal ["one"], reader.value
    assert_nil c.receive
    assert_raises(ZZQ::ClosedError) { c.publish("t", "x") }
  end

  it "lets one fiber receive at a time" do
    c = client(tcp_endpoint)
    c.subscribe("t")
    reader = Thread.new { c.receive(timeout: WAIT) }
    sleep 0.1
    assert_raises(ZZQ::Error) { c.receive(timeout: 0.1) }
    c.publish("t", "for the reader")
    assert_equal "for the reader", reader.value.payload
  end

  it "publishes from threads while one receives" do
    c = client(tcp_endpoint)
    c.subscribe("n", qos: 1)
    writers = 4.times.map do |i|
      Thread.new { 50.times { |j| c.publish("n", "#{i}-#{j}", qos: 1) } }
    end
    got = 200.times.map { c.receive(timeout: 5).payload }
    writers.each(&:join)
    assert_equal 200, got.uniq.size
  end

  it "reads mqtt:// and mqtts:// as tcp:// and tls://" do
    assert_match %r{\Atcp://127\.0\.0\.1:\d+\z}, broker.bind("mqtt://127.0.0.1:0")
    assert_equal "tcp://h:1883", ZZQ::Options.endpoint("mqtt://h:1883")
    assert_equal "tls://h", ZZQ::Options.endpoint("mqtts://h")
  end

  it "connects over ipc:// and inproc://" do
    b = broker
    path = "/tmp/zzq-rb-#{Process.pid}.sock"
    ["ipc://#{path}", "inproc://zzq-rb-test"].each do |endpoint|
      b.bind(endpoint)
      c = client(endpoint)
      c.subscribe("t")
      c.publish("t", endpoint)
      assert_equal endpoint, c.receive(timeout: WAIT).payload
    end
  ensure
    File.delete(path) if path && File.exist?(path)
  end
end

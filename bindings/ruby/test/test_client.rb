# frozen_string_literal: true

require_relative "test_helper"

class ClientTest < Minitest::Test
  def test_every_qos_on_both_versions
    endpoint = tcp_endpoint
    [3, 5].each do |version|
      sub = client(endpoint, client_id: "sub#{version}", version:)
      pub = client(endpoint, client_id: "pub#{version}", version:)
      sub.subscribe("q/#", qos: 2)
      [0, 1, 2].each { |qos| pub.publish("q/#{qos}", "m#{qos}", qos:) }
      got = 3.times.map { sub.receive(timeout: 2) }
      assert_equal %w[q/0 q/1 q/2], got.map(&:topic)
      assert_equal %w[m0 m1 m2], got.map(&:payload)
      assert_equal [0, 1, 2], got.map(&:qos)
    end
  end

  def test_messages_are_frozen_with_utf8_topics_and_binary_payloads
    endpoint = tcp_endpoint
    c = client(endpoint)
    c.subscribe("t")
    c.publish("t", "\xFF\x00".b)
    m = c.receive(timeout: 2)
    assert_predicate m.topic, :frozen?
    assert_equal Encoding::UTF_8, m.topic.encoding
    assert_predicate m.payload, :frozen?
    assert_equal Encoding::BINARY, m.payload.encoding
    assert_equal "\xFF\x00".b, m.payload
  end

  def test_retained_messages_reach_later_subscribers_until_cleared
    endpoint = tcp_endpoint
    pub = client(endpoint)
    pub.publish("r/a", "kept", qos: 1, retain: true)
    first = client(endpoint)
    first.subscribe("r/#", qos: 1)
    m = first.receive(timeout: 2)
    assert_equal ["r/a", "kept", true], [m.topic, m.payload, m.retain?]
    pub.publish("r/a", "", qos: 1, retain: true)
    later = client(endpoint)
    later.subscribe("r/#")
    assert_raises(IO::TimeoutError) { later.receive(timeout: 0.2) }
  end

  def test_will_goes_out_when_asked_for_on_disconnect
    endpoint = tcp_endpoint
    watcher = client(endpoint)
    watcher.subscribe("status")
    will = { topic: "status", payload: "gone", qos: 1 }
    leaving = ZZQ::Client.connect(endpoint, client_id: "leaving", will:)
    leaving.disconnect(reason: 0x04)
    assert_equal "gone", watcher.receive(timeout: 2).payload
  end

  def test_unsubscribe_stops_delivery
    endpoint = tcp_endpoint
    c = client(endpoint)
    c.subscribe("a")
    c.subscribe("b")
    c.unsubscribe("a")
    c.publish("a", "1")
    c.publish("b", "2")
    assert_equal "b", c.receive(timeout: 2).topic
  end

  def test_a_session_resumes
    endpoint = tcp_endpoint
    opts = { client_id: "resume", clean_start: false, session_expiry: 60 }
    first = ZZQ::Client.connect(endpoint, **opts)
    refute_predicate first, :session_present?
    first.subscribe("s", qos: 1)
    first.close
    client(endpoint).publish("s", "while away", qos: 1)
    again = client(endpoint, **opts)
    assert_predicate again, :session_present?
    assert_equal "while away", again.receive(timeout: 2).payload
  end

  def test_subscription_ids_come_with_v5_messages
    endpoint = tcp_endpoint
    c = client(endpoint)
    c.subscribe("x/#", subscription_id: 7)
    c.publish("x/1", "v")
    assert_equal [7], c.receive(timeout: 2).subscription_ids
  end

  def test_a_full_broker_refuses_with_a_reason_code
    b = broker(max_connections: 1)
    endpoint = tcp_endpoint(b)
    client(endpoint, client_id: "first")
    error = assert_raises(ZZQ::ConnectionRefused) do
      ZZQ::Client.connect(endpoint, client_id: "second")
    end
    assert_equal 0x97, error.reason_code
  end

  def test_bad_input_raises
    c = client(tcp_endpoint)
    assert_raises(ZZQ::InvalidTopic) { c.publish("a/+", "x") }
    assert_raises(ZZQ::InvalidTopic) { c.subscribe("a/#/b") }
    assert_raises(ArgumentError) { c.publish("a", "x", qos: 3) }
    assert_raises(ArgumentError) { ZZQ::Client.new(keepalive: 30) }
    assert_raises(ZZQ::TransportError) { ZZQ::Client.connect("tcp://127.0.0.1:1") }
  end

  def test_receive_times_out
    c = client(tcp_endpoint)
    c.subscribe("quiet")
    assert_raises(IO::TimeoutError) { c.receive(timeout: 0.1) }
  end

  def test_close_ends_a_waiting_receive_and_each
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

  def test_one_receiver_at_a_time
    c = client(tcp_endpoint)
    c.subscribe("t")
    reader = Thread.new { c.receive(timeout: 2) }
    sleep 0.1
    assert_raises(ZZQ::Error) { c.receive(timeout: 0.1) }
    c.publish("t", "for the reader")
    assert_equal "for the reader", reader.value.payload
  end

  def test_threads_publish_while_one_receives
    c = client(tcp_endpoint)
    c.subscribe("n", qos: 1)
    writers = 4.times.map do |i|
      Thread.new { 50.times { |j| c.publish("n", "#{i}-#{j}", qos: 1) } }
    end
    got = 200.times.map { c.receive(timeout: 5).payload }
    writers.each(&:join)
    assert_equal 200, got.uniq.size
  end

  def test_mqtt_schemes_name_tcp_and_tls
    b = broker
    assert_match %r{\Atcp://127\.0\.0\.1:\d+\z}, b.bind("mqtt://127.0.0.1:0")
    assert_equal "tcp://h:1883", ZZQ::Options.endpoint("mqtt://h:1883")
    assert_equal "tls://h", ZZQ::Options.endpoint("mqtts://h")
  end

  def test_ipc_and_inproc
    b = broker
    path = "/tmp/zzq-rb-#{Process.pid}.sock"
    ["ipc://#{path}", "inproc://zzq-rb-test"].each do |endpoint|
      b.bind(endpoint)
      c = client(endpoint)
      c.subscribe("t")
      c.publish("t", endpoint)
      assert_equal endpoint, c.receive(timeout: 2).payload
    end
  ensure
    File.delete(path) if path && File.exist?(path)
  end
end

# frozen_string_literal: true

require "test_helper"

describe ZZQ::Message do
  it "round-trips through to_wire / from_wire" do
    msg = ZZQ::Message.new(
      topic: "sensors/42/temp", payload: "23.5",
      qos: 1, retain: true, dup: false, packet_id: 7,
      properties: { user_property: [["unit", "C"]], content_type: "text/plain" },
    )
    out = ZZQ::Message.from_wire(msg.to_wire)
    assert_equal msg, out
  end


  it "freezes the payload and tags it BINARY" do
    msg = ZZQ::Message.new(topic: "t", payload: +"hi")
    assert_equal Encoding::BINARY, msg.payload.encoding
    assert msg.payload.frozen?
  end


  it "raises MalformedPacket when from_wire gets non-PUBLISH bytes" do
    bytes = Protocol::MQTT::Packet::Pingreq.new.encode(version: 5)
    assert_raises(Protocol::MQTT::MalformedPacket) { ZZQ::Message.from_wire(bytes) }
  end


  it "implements value equality across instances" do
    a = ZZQ::Message.new(topic: "t", payload: "p")
    b = ZZQ::Message.new(topic: "t", payload: "p")
    assert_equal a, b
    assert_equal a.hash, b.hash
  end


  it "is a no-op to ack a QoS 0 message" do
    acked = false
    msg = ZZQ::Message.new(topic: "t", payload: "p", qos: 0,
                           on_ack: ->(_) { acked = true })
    msg.ack!
    refute acked
    refute msg.acked?
  end
end


describe ZZQ::Routing::TopicMatch do
  it "matches exact topics" do
    assert ZZQ::Routing::TopicMatch.match?("a/b/c", "a/b/c")
    refute ZZQ::Routing::TopicMatch.match?("a/b/c", "a/b/d")
  end


  it "matches single-level + wildcard" do
    assert ZZQ::Routing::TopicMatch.match?("a/+/c", "a/b/c")
    assert ZZQ::Routing::TopicMatch.match?("+/b/+", "a/b/c")
    refute ZZQ::Routing::TopicMatch.match?("a/+", "a/b/c")
  end


  it "matches multi-level # wildcard" do
    assert ZZQ::Routing::TopicMatch.match?("a/#", "a")
    assert ZZQ::Routing::TopicMatch.match?("a/#", "a/b")
    assert ZZQ::Routing::TopicMatch.match?("a/#", "a/b/c/d")
    assert ZZQ::Routing::TopicMatch.match?("#", "a/b")
  end


  it "refuses to match $SYS topics against bare wildcards" do
    refute ZZQ::Routing::TopicMatch.match?("#", "$SYS/broker")
    refute ZZQ::Routing::TopicMatch.match?("+/broker", "$SYS/broker")
    assert ZZQ::Routing::TopicMatch.match?("$SYS/#", "$SYS/broker/uptime")
  end


  it "strips the $share/<group>/ prefix before matching" do
    assert ZZQ::Routing::TopicMatch.match?("$share/g1/a/+", "a/b")
  end
end

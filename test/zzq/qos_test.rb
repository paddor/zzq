# frozen_string_literal: true

require "test_helper"

describe "QoS 1 publish / subscribe" do
  it "delivers a QoS 1 message and completes publish on PUBACK" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("q1/#", qos: 1)

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("q1/a", "hello-1", qos: 1)

      msg = sub.pop
      assert_equal "q1/a", msg.topic
      assert_equal "hello-1".b, msg.payload
      assert_equal 1, msg.qos
      refute_nil msg.packet_id

      sub_client.close
      pub_client.close
      broker.close
    end
  end


  it "supports many concurrent QoS 1 publishes" do
    Async do |task|
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("many/#", qos: 1)

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)

      tasks = 10.times.map do |i|
        task.async { pub_client.publish("many/#{i}", i.to_s, qos: 1) }
      end
      tasks.each(&:wait)

      received = 10.times.map { sub.pop.payload }
      assert_equal (0..9).map { |i| i.to_s.b }.sort, received.sort

      sub_client.close
      pub_client.close
      broker.close
    end
  end
end


describe "QoS 2 publish / subscribe" do
  it "completes the 4-step handshake and delivers exactly once" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("q2/#", qos: 2)

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("q2/a", "hello-2", qos: 2)

      msg = sub.pop
      assert_equal "q2/a", msg.topic
      assert_equal "hello-2".b, msg.payload
      assert_equal 2, msg.qos

      sub_client.close
      pub_client.close
      broker.close
    end
  end


  it "downgrades QoS to the subscriber-granted max" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("dg/#", qos: 0)

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("dg/x", "p", qos: 2)

      msg = sub.pop
      assert_equal 0, msg.qos
      assert_nil msg.packet_id

      sub_client.close
      pub_client.close
      broker.close
    end
  end
end


describe ZZQ::Routing::PacketIdAllocator do
  it "allocates incrementing u16 ids starting at 1" do
    a = ZZQ::Routing::PacketIdAllocator.new
    assert_equal 1, a.acquire
    assert_equal 2, a.acquire
    assert_equal 3, a.acquire
  end


  it "skips released-and-reacquired ids while outstanding" do
    a = ZZQ::Routing::PacketIdAllocator.new
    a.acquire  # 1
    a.acquire  # 2
    a.release(1)
    # Next acquire picks 3 (forward walk), not 1 — the allocator
    # advances @next, only reusing released ids when it wraps.
    assert_equal 3, a.acquire
  end


  it "blocks when receive_maximum is exhausted, and resumes on release" do
    Async do |task|
      a = ZZQ::Routing::PacketIdAllocator.new(receive_maximum: 2)
      a.acquire  # 1
      a.acquire  # 2

      acquired = nil
      waiter = task.async { acquired = a.acquire }
      sleep(0.05)
      assert_nil acquired, "third acquire should block"

      a.release(1)
      waiter.wait
      assert_equal 3, acquired
    end
  end
end


describe ZZQ::Routing::QosTracker do
  it "resolves a QoS 1 publish promise on PUBACK" do
    t = ZZQ::Routing::QosTracker.new
    pending = t.track_qos1(7, :packet)
    refute pending.promise.resolved?
    t.on_puback(7)
    assert pending.promise.resolved?
    assert_equal 0, t.outbound_size
  end


  it "advances QoS 2 through PUBREC and resolves on PUBCOMP" do
    t = ZZQ::Routing::QosTracker.new
    pending = t.track_qos2(9, :packet)
    assert_equal :waiting_pubrec, pending.stage

    assert t.on_pubrec(9)
    assert_equal :waiting_pubcomp, pending.stage
    refute pending.promise.resolved?

    t.on_pubcomp(9)
    assert pending.promise.resolved?
    assert_equal 0, t.outbound_size
  end


  it "dedupes inbound QoS 2 publishes by packet id" do
    t = ZZQ::Routing::QosTracker.new
    assert t.on_qos2_publish(5)
    refute t.on_qos2_publish(5)
    t.on_pubrel(5)
    assert t.on_qos2_publish(5)
  end
end

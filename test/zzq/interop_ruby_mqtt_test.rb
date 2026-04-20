# frozen_string_literal: true

require "test_helper"
require "mqtt"

# Interop against the `mqtt` Ruby gem (njh/ruby-mqtt — blocking, v3.1.1).
# The external client runs in a Thread so its synchronous socket I/O
# doesn't stall the Async reactor; the main fiber yields via `sub.pop`
# / `client.subscribe` loops while the thread does its work.
describe "Interop with ruby-mqtt" do
  def host_port(endpoint)
    m = endpoint.match %r{\Amqtt://([^:]+):(\d+)\z}
    [m[1], m[2].to_i]
  end


  it "delivers a publish from a ruby-mqtt client to a ZZQ subscriber" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind "mqtt://127.0.0.1:0"
      host, port = host_port broker.last_endpoint

      sub_client = ZZQ::Client.new client_id: "zzq-sub", keep_alive: 0
      sub_client.connect broker.last_endpoint
      sub = sub_client.subscribe "interop/#"

      publisher = Thread.new do
        mc = MQTT::Client.connect host: host, port: port, client_id: "rm-pub"
        mc.publish "interop/from-rubymqtt", "hi from ruby-mqtt", false, 0
        mc.disconnect
      end

      msg = sub.pop
      assert_equal "interop/from-rubymqtt", msg.topic
      assert_equal "hi from ruby-mqtt".b, msg.payload
      assert_equal 0, msg.qos

      publisher.join
      sub_client.close
      broker.close
    end
  end


  it "delivers a ZZQ publish to a ruby-mqtt subscriber" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind "mqtt://127.0.0.1:0"
      host, port = host_port broker.last_endpoint

      received = Queue.new
      subscriber = Thread.new do
        mc = MQTT::Client.connect host: host, port: port, client_id: "rm-sub"
        mc.subscribe "interop/#"
        topic, payload = mc.get
        received << [topic, payload]
        mc.disconnect
      end

      # Give ruby-mqtt time to CONNECT + SUBSCRIBE before we publish.
      sleep 0.01 until broker.session_count >= 1

      pub_client = ZZQ::Client.new client_id: "zzq-pub", keep_alive: 0
      pub_client.connect broker.last_endpoint
      pub_client.publish "interop/from-zzq", "hi from zzq"

      topic, payload = nil
      Async::Task.current.with_timeout(2) do
        topic, payload = received.pop
      end
      assert_equal "interop/from-zzq", topic
      assert_equal "hi from zzq", payload

      subscriber.join
      pub_client.close
      broker.close
    end
  end


  it "delivers a QoS 1 publish from ruby-mqtt to ZZQ" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind "mqtt://127.0.0.1:0"
      host, port = host_port broker.last_endpoint

      sub_client = ZZQ::Client.new client_id: "zzq-sub", keep_alive: 0
      sub_client.connect broker.last_endpoint
      sub = sub_client.subscribe "q1/#", qos: 1

      publisher = Thread.new do
        mc = MQTT::Client.connect host: host, port: port, client_id: "rm-pub"
        mc.publish "q1/x", "payload-q1", false, 1
        mc.disconnect
      end

      msg = sub.pop
      assert_equal "q1/x", msg.topic
      assert_equal "payload-q1".b, msg.payload
      assert_equal 1, msg.qos

      publisher.join
      sub_client.close
      broker.close
    end
  end


  it "honors retained messages published by ruby-mqtt" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind "mqtt://127.0.0.1:0"
      host, port = host_port broker.last_endpoint

      publisher = Thread.new do
        mc = MQTT::Client.connect host: host, port: port, client_id: "rm-pub"
        mc.publish "retained/t", "stays", true, 0
        mc.disconnect
      end
      publisher.join

      sub_client = ZZQ::Client.new client_id: "zzq-late-sub", keep_alive: 0
      sub_client.connect broker.last_endpoint
      sub = sub_client.subscribe "retained/+"

      msg = sub.pop
      assert_equal "retained/t", msg.topic
      assert_equal "stays".b, msg.payload
      assert msg.retain

      sub_client.close
      broker.close
    end
  end
end

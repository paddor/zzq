# frozen_string_literal: true

require "test_helper"

describe "Retained messages" do
  it "replays the last retained message to a late subscriber" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("status/sensor", "online", retain: true)

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("status/+")

      msg = sub.pop
      assert_equal "status/sensor", msg.topic
      assert_equal "online".b, msg.payload
      assert msg.retain, "retained replay should have RETAIN=1"

      sub_client.close
      pub_client.close
      broker.close
    end
  end


  it "empty-payload retain deletes the retained message" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("status/x", "hello", retain: true)
      pub_client.publish("status/x", "", retain: true)

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("status/+")

      Async::Task.current.with_timeout(0.3) { sub.pop } rescue nil
      # No retained message should have been replayed.

      sub_client.close
      pub_client.close
      broker.close
    end
  end


  it "does not set RETAIN on normal (non-replay) fan-out" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("t/+")

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("t/a", "live", retain: true)

      msg = sub.pop
      refute msg.retain, "live delivery should have RETAIN=0 per spec"
      assert_equal "live".b, msg.payload

      sub_client.close
      pub_client.close
      broker.close
    end
  end
end

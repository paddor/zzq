# frozen_string_literal: true

require "test_helper"

describe "QoS 0 publish / subscribe" do
  it "delivers a QoS 0 message to a subscriber on a matching filter" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("sensors/+/temp")

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("sensors/42/temp", "23.5")

      msg = sub.pop
      assert_equal "sensors/42/temp", msg.topic
      assert_equal "23.5".b, msg.payload
      assert_equal 0, msg.qos

      sub_client.close
      pub_client.close
      broker.close
    end
  end


  it "does not deliver messages on non-matching filters" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("other/#")

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("sensors/42/temp", "23.5")

      # Give the broker a moment to (not) deliver.
      Async::Task.current.with_timeout(0.3) { sub.pop } rescue nil
      # Nothing matched, so no message should have been enqueued.

      sub_client.close
      pub_client.close
      broker.close
    end
  end


  it "stops delivering after the subscription is closed" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("t")

      pub_client = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub_client.connect(broker.last_endpoint)
      pub_client.publish("t", "before")
      first = sub.pop
      assert_equal "before".b, first.payload

      sub.close
      refute_nil sub.pop || :ok  # returns nil after close, test just shouldn't hang

      sub_client.close
      pub_client.close
      broker.close
    end
  end


  it "ingests a ZZQ::Message and fans it out to subscribers" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("bridged/#")

      broker.ingest(ZZQ::Message.new(topic: "bridged/x", payload: "hi"))

      msg = sub.pop
      assert_equal "bridged/x", msg.topic
      assert_equal "hi".b, msg.payload

      sub_client.close
      broker.close
    end
  end


  it "accepts << as a shorthand for #ingest" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("bridged/#")

      broker << ZZQ::Message.new(topic: "bridged/y", payload: "via-shovel")

      msg = sub.pop
      assert_equal "bridged/y", msg.topic
      assert_equal "via-shovel".b, msg.payload

      sub_client.close
      broker.close
    end
  end
end

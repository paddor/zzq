# frozen_string_literal: true

require "test_helper"

describe ZZQ::Socket do
  it "binds and connects over TCP" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")
      refute_nil broker.last_endpoint
      assert_match %r{\Amqtt://127\.0\.0\.1:\d+\z}, broker.last_endpoint

      client = ZZQ::Client.new(client_id: "c1")
      client.connect(broker.last_endpoint)

      broker.peer_connected.wait
      assert_equal 1, client.connection_count
      assert_equal 1, broker.connection_count

      client.close
      broker.close
    end
  end


  it "raises when bind is called outside of an Async context" do
    # Block-form #connect auto-enters Async, so we test the no-block path.
    broker = ZZQ::Broker.new
    assert_raises(ZZQ::NotInAsyncContext) { broker.bind("mqtt://127.0.0.1:0") }
  end


  it "raises on an unknown scheme" do
    Async do
      broker = ZZQ::Broker.new
      assert_raises(ZZQ::Error) { broker.bind("foo://whatever") }
    end
  end


  it "auto-closes after the bind block returns" do
    exited = false
    broker = ZZQ::Broker.new
    broker.bind("mqtt://127.0.0.1:0") do |task|
      assert_kind_of Async::Task, task
      assert_match %r{\Amqtt://}, broker.last_endpoint
      exited = true
    end
    assert exited
  end


  it "resolves peer_connected before the peer disconnects" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")
      ep = broker.last_endpoint

      client = ZZQ::Client.new(client_id: "c1")
      client.connect(ep)

      conn = broker.peer_connected.wait
      refute_nil conn

      client.close
      broker.close
      broker.all_peers_gone.wait
    end
  end


  it "coerces user-provided payloads into frozen BINARY strings" do
    Async do
      client = ZZQ::Client.new(client_id: "c1")
      out = client.coerce_binary(+"hi")  # unfrozen input gets re-tagged in place
      assert_equal Encoding::BINARY, out.encoding
      assert out.frozen?
    end
  end
end

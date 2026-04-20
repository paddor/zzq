# frozen_string_literal: true

require "test_helper"
require "socket"

describe "Broker keepalive timeout" do
  # These tests observe the 1.5-s watcher deadline at keep_alive=1, so
  # they need more than the default TEST_ASYNC_TIMEOUT (2 s). Run them
  # on a helper thread, where test_helper's Async wrapper doesn't apply.
  def on_worker_thread(&block)
    err = nil
    t = Thread.new do
      Async(&block).wait
    rescue => e
      err = e
    end
    t.join(10)
    raise err if err
  end

  # Build a CONNECT packet frame by hand. Lets us connect a raw TCP
  # socket that never sends PINGREQ, so we can observe the broker's
  # watcher-mode keepalive firing on 1.5 × keep_alive of silence.
  def raw_connect_bytes(client_id:, keep_alive:)
    Protocol::MQTT::Packet::Connect.new(
      client_id: client_id, clean_start: true, keep_alive: keep_alive,
    ).encode(version: 5)
  end


  it "closes a silent client after 1.5 × keep_alive" do
    raw = nil
    broker = nil
    on_worker_thread do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      host = broker.last_endpoint[%r{mqtt://([^:]+):}, 1]
      port = broker.last_endpoint[%r{:(\d+)\z}, 1].to_i
      raw = TCPSocket.new(host, port)
      raw.write(raw_connect_bytes(client_id: "silent", keep_alive: 1))
      # Read CONNACK so we know the broker has registered the session.
      raw.read(4)
      assert_equal 1, broker.session_count

      # Silence. Watcher should tear the session down at 1.5 s.
      sleep(2.5)
      assert_equal 0, broker.session_count
    ensure
      raw&.close rescue nil
      broker&.close
    end
  end


  it "does not close when client sends regular traffic" do
    on_worker_thread do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      client = ZZQ::Client.new(client_id: "live", keep_alive: 1)
      client.connect(broker.last_endpoint)

      # Client's own keepalive fiber will send PINGREQ every 0.5s. Wait
      # past 1.5s (the timeout window) — session must still exist.
      sleep(2.0)
      assert_equal 1, broker.session_count

      client.close
      broker.close
    end
  end
end

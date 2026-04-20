# frozen_string_literal: true

require "test_helper"

describe "Last Will and Testament" do
  # Helper: abruptly close the client's underlying TCP, bypassing
  # DISCONNECT. The broker's read loop will see EOF and tear the
  # session down without having observed a clean disconnect.
  def abrupt_close(client)
    conn = client.instance_variable_get(:@engine).connections.keys.first
    conn.io.close
  end


  it "publishes the will on abnormal teardown" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("d/#")

      will_client = ZZQ::Client.new(
        client_id: "willer", keep_alive: 0,
        will: { topic: "d/willer", payload: "bye", qos: 0, retain: false },
      )
      will_client.connect(broker.last_endpoint)

      abrupt_close(will_client)

      msg = Async::Task.current.with_timeout(2) { sub.pop }
      assert_equal "d/willer", msg.topic
      assert_equal "bye".b, msg.payload

      sub_client.close
      broker.close
    end
  end


  it "suppresses the will on clean DISCONNECT" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("d/#")

      will_client = ZZQ::Client.new(
        client_id: "willer", keep_alive: 0,
        will: { topic: "d/willer", payload: "bye", qos: 0, retain: false },
      )
      will_client.connect(broker.last_endpoint)
      will_client.close  # clean DISCONNECT

      # Give broker a beat to process any (unexpected) will publish.
      got = Async::Task.current.with_timeout(0.3) { sub.pop } rescue nil
      assert_nil got, "no will should have been published after clean DISCONNECT"

      sub_client.close
      broker.close
    end
  end


  it "retains the will when will.retain is true" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      will_client = ZZQ::Client.new(
        client_id: "willer", keep_alive: 0,
        will: { topic: "state/willer", payload: "offline", qos: 0, retain: true },
      )
      will_client.connect(broker.last_endpoint)
      abrupt_close(will_client)

      # Give the broker a tick to process the teardown + retain.
      sleep(0.05)

      late = ZZQ::Client.new(client_id: "late", keep_alive: 0)
      late.connect(broker.last_endpoint)
      sub = late.subscribe("state/+")

      msg = Async::Task.current.with_timeout(2) { sub.pop }
      assert_equal "state/willer", msg.topic
      assert_equal "offline".b, msg.payload
      assert msg.retain, "replayed retained will should have RETAIN=1"

      late.close
      broker.close
    end
  end
end

# frozen_string_literal: true

require "test_helper"

describe "MQTT handshake" do
  it "receives CONNACK on successful connect" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      client = ZZQ::Client.new(client_id: "c1", keep_alive: 0)
      client.connect(broker.last_endpoint)

      broker.peer_connected.wait
      refute_nil client.last_connack
      assert_equal 0, client.last_connack.reason_code
      assert_equal 1, broker.session_count

      client.close
      broker.close
    end
  end


  it "assigns a client id when the client sends an empty one (v5)" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      client = ZZQ::Client.new(client_id: "", keep_alive: 0, version: 5)
      client.connect(broker.last_endpoint)
      broker.peer_connected.wait

      ack = client.last_connack
      assert_equal 0, ack.reason_code
      assigned = ack.properties[:assigned_client_identifier]
      refute_nil assigned
      assert_match(/\Azzq-/, assigned)

      client.close
      broker.close
    end
  end


  it "sends a clean DISCONNECT on client close" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      client = ZZQ::Client.new(client_id: "c1", keep_alive: 0)
      client.connect(broker.last_endpoint)
      broker.peer_connected.wait

      client.close
      broker.all_peers_gone.wait
      assert_equal 0, broker.session_count
      broker.close
    end
  end


  it "exchanges PINGREQ/PINGRESP without tearing down" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      client = ZZQ::Client.new(client_id: "c1", keep_alive: 1)
      client.connect(broker.last_endpoint)
      broker.peer_connected.wait

      sleep(1.2)
      assert_equal 1, broker.session_count

      client.close
      broker.close
    end
  end


  it "negotiates v5 when the client requests it" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      client = ZZQ::Client.new(client_id: "c1", keep_alive: 0, version: 5,
                               session_expiry_interval: 60)
      client.connect(broker.last_endpoint)
      broker.peer_connected.wait

      assert_equal 0, client.last_connack.reason_code

      client.close
      broker.close
    end
  end


  it "rejects an empty v3 client id with clean_start=false" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      client = ZZQ::Client.new(client_id: "", clean_start: false,
                               keep_alive: 0, version: 3)
      err = assert_raises(ZZQ::ConnectionRefused) { client.connect(broker.last_endpoint) }
      assert_equal Protocol::MQTT::ReasonCodes::V3Connack::IDENTIFIER_REJECTED, err.reason_code

      broker.close
    end
  end
end

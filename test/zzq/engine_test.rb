# frozen_string_literal: true

require "test_helper"

describe ZZQ::Engine do
  # +handle_accepted+ / +handle_connected+ return the
  # +ConnectionLifecycle+ they just built. Frame-oriented transports
  # (e.g. +zzq-websocket+) rely on this to attach cancel-supervisors
  # to the per-connection barrier so per-connection teardown
  # propagates into the transport's request/response fiber tree.
  describe "#handle_accepted / #handle_connected return value" do
    it "returns the broker-side ConnectionLifecycle, parented on the engine's barrier" do
      Async do
        broker = ZZQ::Broker.new
        broker.bind "mqtt://127.0.0.1:0"
        engine = broker.instance_variable_get(:@engine)

        captured = nil
        original = engine.method(:handle_accepted)
        engine.define_singleton_method(:handle_accepted) do |io, endpoint:|
          captured = original.call(io, endpoint: endpoint)
        end

        client = ZZQ::Client.new(client_id: "probe-accept", keep_alive: 0)
        client.connect broker.last_endpoint
        broker.peer_connected.wait

        assert_kind_of ZZQ::Engine::ConnectionLifecycle, captured
        refute_nil captured.barrier
        assert_equal :ready, captured.state

        client.close; broker.close
      end
    end


    it "returns the client-side ConnectionLifecycle from #handle_connected" do
      Async do
        broker = ZZQ::Broker.new
        broker.bind "mqtt://127.0.0.1:0"

        client = ZZQ::Client.new(client_id: "probe-connect", keep_alive: 0)
        client_engine = client.instance_variable_get(:@engine)

        captured = nil
        original = client_engine.method(:handle_connected)
        client_engine.define_singleton_method(:handle_connected) do |io, endpoint:|
          captured = original.call(io, endpoint: endpoint)
        end

        client.connect broker.last_endpoint

        assert_kind_of ZZQ::Engine::ConnectionLifecycle, captured
        refute_nil captured.barrier

        client.close; broker.close
      end
    end
  end
end

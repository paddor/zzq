# frozen_string_literal: true

require "test_helper"

describe "v5 subscription options" do
  it "no_local suppresses echo to the publishing client" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      alice = ZZQ::Client.new(client_id: "alice", keep_alive: 0)
      alice.connect(broker.last_endpoint)
      bob = ZZQ::Client.new(client_id: "bob", keep_alive: 0)
      bob.connect(broker.last_endpoint)

      alice_sub = alice.subscribe("chat/#", no_local: true)
      bob_sub   = bob.subscribe("chat/#")

      alice.publish("chat/hi", "from-alice")

      # Bob receives it.
      msg = Async::Task.current.with_timeout(1) { bob_sub.pop }
      assert_equal "from-alice".b, msg.payload

      # Alice should NOT.
      echoed = Async::Task.current.with_timeout(0.2) { alice_sub.pop } rescue nil
      assert_nil echoed

      alice.close; bob.close; broker.close
    end
  end


  it "echoes to the publisher by default (no_local false)" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      alice = ZZQ::Client.new(client_id: "alice", keep_alive: 0)
      alice.connect(broker.last_endpoint)

      sub = alice.subscribe("loop/#")  # default no_local: false
      alice.publish("loop/x", "hello")

      msg = Async::Task.current.with_timeout(1) { sub.pop }
      assert_equal "hello".b, msg.payload

      alice.close; broker.close
    end
  end


  it "retain_as_published preserves the publisher's RETAIN flag on live fan-out" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("t/+", retain_as_published: true)

      pub = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub.connect(broker.last_endpoint)
      pub.publish("t/a", "live", retain: true)

      msg = Async::Task.current.with_timeout(1) { sub.pop }
      assert msg.retain, "retain_as_published=true should forward publisher's RETAIN bit"

      pub.close; sub_client.close; broker.close
    end
  end


  it "retain_handling=2 suppresses retained replay on SUBSCRIBE" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      pub = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub.connect(broker.last_endpoint)
      pub.publish("t/a", "stored", retain: true)

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("t/+", retain_handling: 2)

      got = Async::Task.current.with_timeout(0.2) { sub.pop } rescue nil
      assert_nil got, "retain_handling=2 should not replay retained messages"

      pub.close; sub_client.close; broker.close
    end
  end
end


describe "v5 PUBLISH properties" do
  it "round-trips content_type, response_topic, user_property end-to-end" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      sub_client = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub_client.connect(broker.last_endpoint)
      sub = sub_client.subscribe("rpc/+")

      pub = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub.connect(broker.last_endpoint)
      pub.publish("rpc/call", "{}",
                  properties: {
                    content_type: "application/json",
                    response_topic: "rpc/reply/1",
                    user_property: [["trace", "abc123"]],
                  })

      msg = Async::Task.current.with_timeout(1) { sub.pop }
      assert_equal "application/json", msg.properties[:content_type]
      assert_equal "rpc/reply/1",      msg.properties[:response_topic]
      assert_equal [["trace", "abc123"]], msg.properties[:user_property]

      pub.close; sub_client.close; broker.close
    end
  end
end


describe "v5 shared subscriptions" do
  it "round-robins $share/group/filter across group members" do
    Async do
      broker = ZZQ::Broker.new
      broker.bind("mqtt://127.0.0.1:0")

      workers = 3.times.map do |i|
        c = ZZQ::Client.new(client_id: "worker-#{i}", keep_alive: 0)
        c.connect(broker.last_endpoint)
        [c, c.subscribe("$share/workers/jobs/#")]
      end

      pub = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub.connect(broker.last_endpoint)

      6.times { |i| pub.publish("jobs/#{i}", "task-#{i}") }

      # Each worker should receive exactly 2 (round-robin across 3).
      counts = workers.map do |(_, sub)|
        n = 0
        begin
          loop do
            Async::Task.current.with_timeout(0.3) { sub.pop }
            n += 1
          end
        rescue
          n
        end
      end
      assert_equal [2, 2, 2], counts.sort

      workers.each { |(c, _)| c.close }
      pub.close; broker.close
    end
  end
end

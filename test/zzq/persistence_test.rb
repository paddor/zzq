# frozen_string_literal: true

require "test_helper"
require "tmpdir"

describe ZZQ::Persistence::Memory do
  it "round-trips retained messages through save/load" do
    p = ZZQ::Persistence::Memory.new
    msg = ZZQ::Message.new(topic: "a/b", payload: "hi", qos: 0, retain: true)
    p.save_retained(msg)

    loaded = p.load_retained.to_a
    assert_equal 1, loaded.size
    assert_equal "a/b", loaded.first.topic
    assert_equal "hi".b, loaded.first.payload
  end


  it "delete removes by topic" do
    p = ZZQ::Persistence::Memory.new
    p.save_retained(ZZQ::Message.new(topic: "a", payload: "x", retain: true))
    p.save_retained(ZZQ::Message.new(topic: "b", payload: "y", retain: true))
    p.delete_retained("a")
    assert_equal ["b"], p.load_retained.map(&:topic)
  end
end


describe ZZQ::Persistence::PStore do
  it "persists retained state across instances" do
    Dir.mktmpdir do |dir|
      p1 = ZZQ::Persistence::PStore.new(data_dir: dir)
      p1.save_retained(ZZQ::Message.new(
        topic: "sensors/temp", payload: "23.5", retain: true,
        properties: { content_type: "text/plain" },
      ))

      # Reopen in a new instance — same file.
      p2 = ZZQ::Persistence::PStore.new(data_dir: dir)
      msgs = p2.load_retained.to_a
      assert_equal 1, msgs.size
      assert_equal "sensors/temp", msgs.first.topic
      assert_equal "23.5".b, msgs.first.payload
      assert_equal "text/plain", msgs.first.properties[:content_type]
    end
  end


  it "empty-payload retain survives a restart (deletes on save)" do
    Dir.mktmpdir do |dir|
      p = ZZQ::Persistence::PStore.new(data_dir: dir)
      p.save_retained(ZZQ::Message.new(topic: "t", payload: "live", retain: true))
      p.delete_retained("t")

      reloaded = ZZQ::Persistence::PStore.new(data_dir: dir)
      assert_equal [], reloaded.load_retained.to_a
    end
  end
end


describe "Broker with persistence backend" do
  it "rehydrates retained messages from disk on startup" do
    Dir.mktmpdir do |dir|
      # Round 1: publisher sets a retained message; broker shuts down.
      Async do
        backend = ZZQ::Persistence::PStore.new(data_dir: dir)
        broker1 = ZZQ::Broker.new(persistence: backend)
        broker1.bind("mqtt://127.0.0.1:0")
        ep1 = broker1.last_endpoint

        pub = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
        pub.connect(ep1)
        pub.publish("state/x", "on", retain: true)
        pub.close
        broker1.close
      end

      # Round 2: fresh broker + same data_dir; late subscriber must
      # still see the retained message.
      Async do
        backend = ZZQ::Persistence::PStore.new(data_dir: dir)
        broker2 = ZZQ::Broker.new(persistence: backend)
        broker2.bind("mqtt://127.0.0.1:0")

        sub = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
        sub.connect(broker2.last_endpoint)
        queue = sub.subscribe("state/+")
        msg = Async::Task.current.with_timeout(1) { queue.pop }

        assert_equal "state/x", msg.topic
        assert_equal "on".b, msg.payload
        assert msg.retain

        sub.close
        broker2.close
      end
    end
  end
end

# frozen_string_literal: true

require "test_helper"
require "tmpdir"
require "securerandom"

describe "IPC transport" do
  it "round-trips publish/subscribe over a filesystem unix socket" do
    Dir.mktmpdir do |dir|
      sock_path = File.join(dir, "zzq.sock")
      endpoint = "ipc://#{sock_path}"
      Async do
        broker = ZZQ::Broker.new
        broker.bind(endpoint)

        sub = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
        sub.connect(endpoint)
        queue = sub.subscribe("hi/+")

        pub = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
        pub.connect(endpoint)
        pub.publish("hi/world", "over-ipc")

        msg = Async::Task.current.with_timeout(1) { queue.pop }
        assert_equal "over-ipc".b, msg.payload

        pub.close; sub.close; broker.close
      end
    end
  end


  it "round-trips over the Linux abstract namespace" do
    skip "abstract namespace is Linux-only" unless RUBY_PLATFORM.include?("linux")

    endpoint = "ipc://@zzq-test-#{SecureRandom.hex(4)}"
    Async do
      broker = ZZQ::Broker.new
      broker.bind(endpoint)

      sub = ZZQ::Client.new(client_id: "sub", keep_alive: 0)
      sub.connect(endpoint)
      queue = sub.subscribe("a/#")

      pub = ZZQ::Client.new(client_id: "pub", keep_alive: 0)
      pub.connect(endpoint)
      pub.publish("a/b", "abstract")

      msg = Async::Task.current.with_timeout(1) { queue.pop }
      assert_equal "abstract".b, msg.payload

      pub.close; sub.close; broker.close
    end
  end
end

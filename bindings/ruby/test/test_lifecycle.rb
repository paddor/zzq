# frozen_string_literal: true

require_relative "test_helper"

class LifecycleTest < Minitest::Spec
  FD_DIR = "/proc/self/fd"

  def fds
    Dir.children(FD_DIR).size
  end

  it "releases the fds of closed clients" do
    skip "needs /proc" unless File.directory?(FD_DIR)
    endpoint = tcp_endpoint
    client(endpoint).close
    GC.start
    before = fds
    20.times do |i|
      c = ZZQ::Client.connect(endpoint, client_id: "c#{i}")
      c.subscribe("t")
      assert_raises(IO::TimeoutError) { c.receive(timeout: 0.01) }
      c.close
    end
    GC.start
    sleep 0.1
    assert_operator fds, :<=, before + 2
  end

  it "starts new lanes in a forked child" do
    skip "needs fork" unless Process.respond_to?(:fork)
    client(tcp_endpoint).publish("warm", "up")
    pid = fork do
      b = ZZQ::Broker.new(sys_interval: nil)
      c = ZZQ::Client.connect(b.bind("tcp://127.0.0.1:0"))
      c.subscribe("t")
      c.publish("t", "child")
      exit!(c.receive(timeout: 2)&.payload == "child" ? 0 : 1)
    end
    _, status = Process.wait2(pid)
    assert_predicate status, :success?
  end

  it "takes io_threads only before first use" do
    client(tcp_endpoint)
    assert_raises(ZZQ::Error) { ZZQ.io_threads = 2 }
  end
end

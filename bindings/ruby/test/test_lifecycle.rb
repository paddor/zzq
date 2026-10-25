# frozen_string_literal: true

require_relative "test_helper"

class LifecycleTest < Minitest::Test
  def fds
    Dir.children("/proc/self/fd").size
  end

  def test_closed_clients_release_their_fds
    skip "needs /proc" unless File.directory?("/proc/self/fd")
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

  def test_a_forked_child_starts_its_own_lanes
    skip "needs fork" unless Process.respond_to?(:fork)
    endpoint = tcp_endpoint
    client(endpoint).publish("warm", "up")
    pid = fork do
      b = ZZQ::Broker.new(sys_interval: nil)
      child_endpoint = b.bind("tcp://127.0.0.1:0")
      c = ZZQ::Client.connect(child_endpoint)
      c.subscribe("t")
      c.publish("t", "child")
      exit!(c.receive(timeout: 2)&.payload == "child" ? 0 : 1)
    end
    _, status = Process.wait2(pid)
    assert_predicate status, :success?
  end

  def test_io_threads_are_set_before_first_use
    client(tcp_endpoint)
    assert_raises(ZZQ::Error) { ZZQ.io_threads = 2 }
  end
end

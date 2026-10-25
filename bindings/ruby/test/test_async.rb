# frozen_string_literal: true

require_relative "test_helper"
require "async"

class AsyncTest < Minitest::Test
  def test_a_waiting_receive_lets_other_fibers_run
    endpoint = tcp_endpoint
    Async do |task|
      sub = client(endpoint)
      pub = client(endpoint)
      sub.subscribe("t")
      ran = false
      task.async do
        sleep 0.05
        ran = true
        pub.publish("t", "from a fiber")
      end
      assert_equal "from a fiber", sub.receive(timeout: 2).payload
      assert ran
    end.wait
  end

  def test_many_fibers_wait_for_acks_at_once
    endpoint = tcp_endpoint
    Async do |task|
      sub = client(endpoint)
      pub = client(endpoint)
      sub.subscribe("n", qos: 1)
      reader = task.async { 1000.times.map { sub.receive(timeout: 5).payload } }
      50.times.map do |i|
        task.async { 20.times { |j| pub.publish("n", "#{i}-#{j}", qos: 1) } }
      end.each(&:wait)
      assert_equal 1000, reader.wait.uniq.size
    end.wait
    assert_operator ZZQ::Waiting::WAKES.size, :<=, ZZQ::Waiting::KEEP
  end

  def test_a_timeout_cancels_only_the_waiting_fiber
    endpoint = tcp_endpoint
    Async do |task|
      c = client(endpoint)
      c.subscribe("t")
      waiting = task.async { c.receive(timeout: 0.05) }
      assert_raises(IO::TimeoutError) { waiting.wait }
      c.publish("t", "after")
      assert_equal "after", c.receive(timeout: 2).payload
    end.wait
  end
end

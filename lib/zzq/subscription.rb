# frozen_string_literal: true

require "async/queue"
require "async/limited_queue"

module ZZQ
  # Handle returned by {Client#subscribe} and {Broker#subscribe}. A
  # thin wrapper around an +Async::LimitedQueue+ that also owns the
  # UNSUBSCRIBE lifecycle.
  #
  # Callers consume with {#pop} or {#each}; {#close} sends UNSUBSCRIBE
  # and wakes any blocked consumer with +nil+.
  #
  # Created internally — users should never call +.new+ directly.
  class Subscription
    # @return [Array<String>] topic filters this subscription matches.
    attr_reader :filters

    # @return [Integer] subscription identifier (v5) or nil (v3).
    attr_reader :subscription_id


    def initialize(filters:, hwm:, subscription_id: nil, on_close: nil)
      @filters         = Array(filters).freeze
      @subscription_id = subscription_id
      @queue           = Async::LimitedQueue.new(hwm)
      @on_close        = on_close
      @closed          = false
    end


    # Blocks the fiber until a message arrives. Returns +nil+ when
    # the subscription is closed.
    def pop
      return nil if @closed && @queue.empty?
      @queue.dequeue
    end


    # Yields each message until the subscription is closed.
    def each
      return enum_for(:each) unless block_given?
      while (msg = pop)
        yield msg
      end
    end


    # Sends UNSUBSCRIBE and wakes the consumer. Idempotent.
    def close
      return if @closed
      @closed = true
      @on_close&.call(self)
      @queue.enqueue(nil)
    end


    def closed?
      @closed
    end


    # Internal: deliver a message to the subscriber's queue. Blocks
    # the caller if the queue is at high-water-mark.
    def deliver(message)
      return if @closed
      @queue.enqueue(message)
    end
  end
end

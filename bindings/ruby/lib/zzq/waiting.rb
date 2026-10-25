# frozen_string_literal: true

module ZZQ
  module Native
    # A pipe a fiber waits on while a Rust future is pending.
    class Wake
      # Waits for a wake, raising IO::TimeoutError past +deadline+.
      #
      # @param deadline [Float, nil] monotonic clock time
      def wait(deadline)
        timeout = deadline && [deadline - Waiting.now, 0].max
        @io ||= IO.for_fd(fd, autoclose: false)
        raise IO::TimeoutError, "timed out" unless @io.wait_readable(timeout)

        clear
      end
    end
  end

  # Waits for native ops without blocking other fibers: under a fiber
  # scheduler only the calling fiber waits, otherwise the thread waits
  # without holding the GVL.
  #
  # @api private
  module Waiting
    # Wakes kept for reuse. More are made while more fibers wait at once.
    WAKES = []
    KEEP = 64

    module_function

    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def deadline(timeout)
      timeout && now + timeout
    end

    # Yields a wake for this call's waits.
    def with_wake
      wake = WAKES.pop || Native::Wake.new
      yield wake
    ensure
      WAKES.push(wake) if wake && WAKES.size < KEEP
    end

    # Polls +op+ until it is done and returns its result. An interrupted
    # wait cancels it.
    def await(op, timeout = nil)
      with_wake do |wake|
        limit = deadline(timeout)
        loop do
          result = op.poll(wake)
          return result unless Native::PENDING.equal?(result)

          wake.wait(limit)
        end
      end
    rescue Exception # rubocop:disable Lint/RescueException
      op.cancel
      raise
    end
  end
end

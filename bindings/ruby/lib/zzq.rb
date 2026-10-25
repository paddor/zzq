# frozen_string_literal: true

require "io/wait"

require_relative "zzq/version"
require_relative "zzq/errors"
require_relative "zzq/message"
require_relative "zzq/zzq_native"
require_relative "zzq/options"
require_relative "zzq/waiting"
require_relative "zzq/client"
require_relative "zzq/broker"

# MQTT 3.1.1 and 5.0 client and broker, backed by the zzq Rust crates.
module ZZQ
  class << self
    # @return [Integer] IO lanes: threads that run connections
    def io_threads
      Native.io_threads
    end

    # Sets the number of IO lanes, before the first client or broker.
    #
    # @param count [Integer]
    def io_threads=(count)
      Native.io_threads = count
    end
  end
end

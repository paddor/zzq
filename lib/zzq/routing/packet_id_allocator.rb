# frozen_string_literal: true

require "async/condition"

module ZZQ
  module Routing
    # u16 packet-id allocator with an optional receive-maximum cap.
    # Thread-unsafe (designed to be owned by a single connection's
    # writer fiber). Packet id 0 is reserved (MQTT-2.2.1-3); we wrap
    # 1..65535.
    #
    # When the outstanding count reaches +receive_maximum+, +#acquire+
    # blocks the caller fiber until a later +#release+ frees a slot.
    class PacketIdAllocator
      def initialize(receive_maximum: 65_535)
        @receive_maximum = receive_maximum
        @outstanding     = {}  # packet_id => true
        @next            = 0
        @free_cond       = Async::Condition.new
      end


      # Acquire a fresh packet id. Blocks the calling fiber if the
      # receive-maximum quota is exhausted.
      def acquire
        while @outstanding.size >= @receive_maximum
          @free_cond.wait
        end
        id = loop do
          @next = (@next % 0xFFFF) + 1
          break @next unless @outstanding.key?(@next)
        end
        @outstanding[id] = true
        id
      end


      # Mark +id+ free. Wakes one waiter if any.
      def release(id)
        return false unless @outstanding.delete(id)
        @free_cond.signal
        true
      end


      def outstanding?(id)
        @outstanding.key?(id)
      end


      def size = @outstanding.size
    end
  end
end

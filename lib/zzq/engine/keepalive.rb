# frozen_string_literal: true

require "async"
require "async/clock"
require "protocol/mqtt"

module ZZQ
  class Engine
    # Keepalive driver for a single connection.
    #
    # +role+:
    #   * +:sender+ (client default) — sends PINGREQ if locally idle
    #     for keep_alive/2; closes if nothing received for 1.5 × keep_alive.
    #   * +:watcher+ (broker default) — does not send; just closes if
    #     nothing received for 1.5 × keep_alive. MQTT-3.1.2-22 is
    #     enforced by the broker.
    #
    # One fiber per connection, attached to the connection's barrier
    # so it tears down with the connection.
    class Keepalive
      attr_accessor :last_sent_at, :last_received_at


      def initialize(conn, keep_alive:, on_timeout:, role: :sender)
        @conn = conn
        @keep_alive = keep_alive
        @on_timeout = on_timeout
        @role = role
        now = Async::Clock.now
        @last_sent_at = now
        @last_received_at = now
      end


      # Call this every time a packet is written, to reset the send-idle
      # timer.
      def note_sent
        @last_sent_at = Async::Clock.now
      end


      # Call this every time a packet is read, to reset the recv-idle
      # timer.
      def note_received
        @last_received_at = Async::Clock.now
      end


      def run
        return if @keep_alive.zero?  # 0 disables keepalive
        loop do
          sleep(@keep_alive / 2.0)
          now = Async::Clock.now

          if (now - @last_received_at) > (@keep_alive * 1.5)
            @on_timeout.call
            break
          end

          next unless @role == :sender

          if (now - @last_sent_at) >= (@keep_alive / 2.0)
            begin
              @conn.write_packet(Protocol::MQTT::Packet::Pingreq.new)
              note_sent
            rescue IOError, Errno::EPIPE, Errno::ECONNRESET
              break
            end
          end
        end
      rescue Async::Stop
      end
    end
  end
end

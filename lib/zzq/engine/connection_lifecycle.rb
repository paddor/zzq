# frozen_string_literal: true

require "async/barrier"
require "protocol/mqtt"

module ZZQ
  class Engine
    # Owns the full arc of one connection: new → handshaking → ready →
    # closed. Centralises side-effect ordering so routing doesn't have
    # to.
    #
    # The MQTT CONNECT/CONNACK exchange itself is NOT performed here —
    # it's role-asymmetric (client writes CONNECT first, broker reads
    # it first) and so lives in the role's routing layer, which calls
    # +handshake_succeeded!+ once the exchange is done.
    #
    #   new → handshaking → ready → closed
    #
    # #lost! / #close! are idempotent.
    class ConnectionLifecycle
      class InvalidTransition < RuntimeError; end

      STATES = %i[new handshaking ready closed].freeze

      TRANSITIONS = {
        new:         %i[handshaking ready closed].freeze,
        handshaking: %i[ready closed].freeze,
        ready:       %i[closed].freeze,
        closed:      [].freeze,
      }.freeze


      attr_reader :conn       # Protocol::MQTT::Connection
      attr_reader :endpoint
      attr_reader :state
      attr_reader :barrier


      def initialize(engine, endpoint:)
        @engine   = engine
        @endpoint = endpoint
        @state    = :new
        @conn     = nil
        @barrier  = Async::Barrier.new(parent: engine.barrier)
      end


      # Wraps the accepted/connected IO in a Protocol::MQTT::Connection
      # and returns it. The handshake itself (CONNECT/CONNACK exchange)
      # is driven by the routing layer — this is "transport ready,
      # MQTT handshake pending".
      def begin_handshake!(io)
        transition!(:handshaking)
        mqtt = Protocol::MQTT::Connection.new(
          io,
          version: @engine.options.version,
          max_packet_size: @engine.options.max_packet_size,
        )
        @conn = mqtt
        mqtt
      end


      # Called by the routing layer after CONNECT/CONNACK succeeds.
      def handshake_succeeded!
        @engine.connections[@conn] = self
        transition!(:ready)
        begin
          @engine.routing.connection_added(@conn) if @engine.routing.respond_to?(:connection_added)
        rescue ConnectionRejected
          @engine.emit_monitor_event(:connection_rejected, endpoint: @endpoint)
          tear_down!(reconnect: false)
          raise
        end
        @engine.lifecycle.peer_connected.resolve(@conn) unless @engine.lifecycle.peer_connected.resolved?
        @engine.emit_monitor_event(:connected, endpoint: @endpoint)
      end


      # Called on handshake failure — e.g. refused CONNACK, RST mid-
      # CONNECT. Tears down and requests a reconnect if eligible.
      def handshake_failed!(error:)
        @engine.emit_monitor_event(:handshake_failed, endpoint: @endpoint, detail: { error: error })
        @conn&.close rescue nil
        tear_down!(reconnect: true)
      end


      # Unexpected loss of an established connection.
      def lost!
        tear_down!(reconnect: true)
      end


      # Deliberate close (engine shutdown, kicked by broker, …).
      def close!
        tear_down!(reconnect: false)
      end


      # Starts a supervisor task that tears down when any pump exits.
      def start_supervisor!
        return if @barrier.empty?
        @engine.barrier.async(transient: true, annotation: "zzq conn supervisor") do
          @barrier.wait { |task| task.wait; break }
        rescue Async::Stop, Async::Cancel
        rescue *CONNECTION_LOST
        ensure
          lost!
        end
      end


      private def tear_down!(reconnect: false)
        return if @state == :closed
        transition!(:closed)
        if @conn
          @engine.connections.delete(@conn)
          @engine.routing.connection_removed(@conn) if @engine.routing.respond_to?(:connection_removed)
          @conn.close rescue nil
          @engine.emit_monitor_event(:disconnected, endpoint: @endpoint)
          @engine.resolve_all_peers_gone_if_empty
        end
        @engine.maybe_reconnect(@endpoint) if reconnect
        # Stop after cleanup: when tear_down! is called from a task on
        # this barrier (e.g. the read-loop's ensure), stopping earlier
        # would cancel the current fiber mid-cleanup.
        @barrier.stop
      end


      private def transition!(new_state)
        allowed = TRANSITIONS[@state]
        raise InvalidTransition, "#{@state} → #{new_state}" unless allowed&.include?(new_state)
        @state = new_state
      end
    end
  end
end

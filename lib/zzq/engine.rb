# frozen_string_literal: true

require "async"
require "async/clock"
require "set"
require_relative "error"
require_relative "engine/socket_lifecycle"
require_relative "engine/connection_lifecycle"

module ZZQ
  # Per-socket orchestrator. Owns listeners, the connection map, the
  # transport registry, and the socket-level state machine. Adapted
  # from NNQ::Engine minus reactor / inproc / SP-handshake concerns.
  class Engine
    # Scheme → transport module registry. Populated by each transport
    # file on require.
    @transports = {}


    class << self
      attr_reader :transports
    end


    attr_reader :options
    attr_reader :routing
    attr_reader :connections  # Hash{Protocol::MQTT::Connection => ConnectionLifecycle}
    attr_reader :lifecycle
    attr_reader :last_endpoint
    attr_reader :dialed
    attr_accessor :monitor_queue


    # @yieldparam engine [Engine] build_routing hook; returns the
    #   role-specific routing strategy.
    def initialize(options:)
      @options       = options
      @connections   = {}
      @listeners     = []
      @lifecycle     = SocketLifecycle.new
      @last_endpoint = nil
      @monitor_queue = nil
      @dialed        = Set.new
      @dial_opts     = {}
      @routing       = yield(self)
    end


    def emit_monitor_event(type, endpoint: nil, detail: nil)
      return unless @monitor_queue
      @monitor_queue.enqueue(MonitorEvent.new(type: type, endpoint: endpoint, detail: detail))
    rescue Async::Stop
    end


    def parent_task   = @lifecycle.parent_task
    def barrier       = @lifecycle.barrier
    def closed?       = @lifecycle.closed?
    def peer_connected = @lifecycle.peer_connected
    def all_peers_gone = @lifecycle.all_peers_gone


    def resolve_all_peers_gone_if_empty
      @lifecycle.resolve_all_peers_gone_if_empty(@connections)
    end


    # Must be called from inside an Async::Task.
    def capture_parent_task
      @lifecycle.capture_parent_task
    end


    # Bind (listener) — role-agnostic. The routing layer (on accept)
    # drives the MQTT handshake.
    def bind(endpoint, **opts)
      transport = transport_for(endpoint)
      listener  = transport.bind(endpoint, self, **opts)
      listener.start_accept_loop(@lifecycle.barrier) do |io|
        handle_accepted(io, endpoint: endpoint)
      end
      @listeners << listener
      @last_endpoint = listener.endpoint
      emit_monitor_event(:listening, endpoint: @last_endpoint)
    end


    # Connect (dialer).
    def connect(endpoint, **opts)
      @dialed << endpoint
      @dial_opts[endpoint] = opts unless opts.empty?
      @last_endpoint = endpoint
      transport_for(endpoint).connect(endpoint, self, **opts)
    end


    def dial_opts_for(endpoint)
      @dial_opts[endpoint] || {}
    end


    def maybe_reconnect(endpoint)
      # Reconnect loop not wired up yet (M4 job).
    end


    def transport_for(endpoint)
      scheme = endpoint[/\A([a-z+]+):\/\//i, 1] or raise Error, "no scheme: #{endpoint}"
      Engine.transports[scheme] or raise Error, "unsupported transport: #{scheme}"
    end


    # Called by transports for each accepted connection. Hands off to
    # routing so the broker can read CONNECT / write CONNACK. Returns
    # the +ConnectionLifecycle+ so frame-oriented transports (e.g.
    # WebSocket) can attach cancel-supervisors to its barrier.
    def handle_accepted(io, endpoint:)
      lifecycle = ConnectionLifecycle.new(self, endpoint: endpoint)
      mqtt = lifecycle.begin_handshake!(io)
      @routing.handle_accepted(mqtt, lifecycle) if @routing.respond_to?(:handle_accepted)
      lifecycle
    rescue => e
      lifecycle.handshake_failed!(error: e)
      raise unless e.is_a?(ConnectionRejected) || CONNECTION_LOST.any? { |c| e.is_a?(c) }
    end


    # Called by transports for each dialed connection. Hands off to
    # routing so the client can write CONNECT / read CONNACK. Returns
    # the +ConnectionLifecycle+ so frame-oriented transports (e.g.
    # WebSocket) can attach cancel-supervisors to its barrier.
    def handle_connected(io, endpoint:)
      lifecycle = ConnectionLifecycle.new(self, endpoint: endpoint)
      mqtt = lifecycle.begin_handshake!(io)
      @routing.handle_connected(mqtt, lifecycle) if @routing.respond_to?(:handle_connected)
      lifecycle
    rescue => e
      lifecycle.handshake_failed!(error: e)
      raise unless e.is_a?(ConnectionRejected) || CONNECTION_LOST.any? { |c| e.is_a?(c) }
    end


    def spawn_task(annotation:, parent: @lifecycle.barrier, &block)
      parent.async(annotation: annotation, &block)
    end


    def close
      return unless @lifecycle.alive?

      @lifecycle.start_closing!
      @listeners.each(&:stop)
      @routing.close if @routing.respond_to?(:close)
      @connections.values.each(&:close!)

      emit_monitor_event(:closed)
      @monitor_queue&.enqueue(nil)

      @lifecycle.barrier&.stop
      @lifecycle.finish_closing!
      @lifecycle.peer_connected.resolve(nil) unless @lifecycle.peer_connected.resolved?
    end


    def handle_connection_lost(conn)
      @connections[conn]&.lost!
    end
  end
end

# frozen_string_literal: true

require "async"
require "async/queue"
require_relative "error"
require_relative "monitor_event"
require_relative "options"
require_relative "engine"

module ZZQ
  # Abstract base class for {ZZQ::Client} and {ZZQ::Broker}. Owns the
  # Engine and exposes the infrastructure API (bind / connect / close /
  # monitor / peer_connected / all_peers_gone). The role-specific API
  # (subscribe / publish / ingest) lives on the subclasses.
  #
  # ZZQ is pure-Async: every public method that performs I/O must run
  # inside an Async::Task. Either wrap calls in `Async do … end`, or
  # use the block form of #connect / #bind which enters `Async{}` for
  # you and closes the socket on block exit.
  class Socket
    attr_reader :options


    def self.bind(endpoint, **opts, &block)
      sock = new(**opts)
      if block
        sock.bind(endpoint, &block)
      else
        sock.bind(endpoint)
        sock
      end
    end


    def self.connect(endpoint, **opts, &block)
      sock = new(**opts)
      if block
        sock.connect(endpoint, &block)
      else
        sock.connect(endpoint)
        sock
      end
    end


    def initialize(**opts)
      @options = Options.new(**opts)
      @engine  = Engine.new(options: @options) { |engine| build_routing(engine) }
    end


    # Binds to +endpoint+. If a block is given, wraps the call in
    # `Async do … end` (if not already inside one), yields the owning
    # Async::Task to the block, and closes the socket on block exit.
    def bind(endpoint, **opts, &block)
      block ? run_in_async(endpoint, :bind, opts, &block) : run_sync(:bind, endpoint, **opts)
    end


    # Connects to +endpoint+. Block form yields the owning Async::Task
    # and auto-closes on block exit, same as {#bind}.
    def connect(endpoint, **opts, &block)
      block ? run_in_async(endpoint, :connect, opts, &block) : run_sync(:connect, endpoint, **opts)
    end


    def close
      if Async::Task.current?
        @engine.close
      else
        Async { @engine.close }.wait
      end
      nil
    end


    def last_endpoint        = @engine.last_endpoint
    def connection_count     = @engine.connections.size
    def peer_connected       = @engine.peer_connected
    def all_peers_gone       = @engine.all_peers_gone


    # Yields lifecycle events for this socket until it's closed. Must
    # be called from within an Async::Task.
    def monitor(&block)
      raise NotInAsyncContext unless Async::Task.current?
      queue = Async::Queue.new
      @engine.monitor_queue = queue
      @engine.spawn_task(annotation: "zzq monitor") do
        while (event = queue.dequeue)
          block.call(event)
        end
      rescue Async::Stop
      ensure
        @engine.monitor_queue = nil
        block.call(MonitorEvent.new(type: :monitor_stopped))
      end
    end


    # Coerces +body+ to a frozen BINARY String. MQTT payloads are
    # opaque bytes — uniform frozen+BINARY contract mirrors NNQ.
    def coerce_binary(body)
      body = body.to_str unless body.is_a?(String)
      body.force_encoding(Encoding::BINARY) unless body.frozen? || body.encoding == Encoding::BINARY
      body.freeze
    end


    private


    def run_sync(op, endpoint, **opts)
      raise NotInAsyncContext unless Async::Task.current?
      @engine.capture_parent_task
      @engine.public_send(op, endpoint, **opts)
    end


    def run_in_async(endpoint, op, opts, &block)
      if (task = Async::Task.current?)
        run_sync(op, endpoint, **opts)
        begin
          block.call(task)
        ensure
          close
        end
      else
        Async do |task|
          run_sync(op, endpoint, **opts)
          begin
            block.call(task)
          ensure
            close
          end
        end.wait
      end
    end


    # Subclass hook — return a role-specific routing strategy instance.
    def build_routing(_engine)
      raise NotImplementedError, "#{self.class} must implement #build_routing"
    end
  end
end

# frozen_string_literal: true

require "socket"
require "uri"
require "io/stream"
require_relative "../engine"

module ZZQ
  module Transport
    # Plain MQTT-over-TCP transport. Endpoint scheme: `mqtt://host:port`
    # (default port 1883). One server per bind, blocking accept inside
    # an Async fiber.
    module MQTT
      Engine.transports["mqtt"] = self


      class << self
        def bind(endpoint, engine, **)
          host, port = parse_endpoint(endpoint)
          host       = "0.0.0.0" if host == "*"
          server     = TCPServer.new(host, port)
          actual     = server.local_address.ip_port
          host_part  = host.include?(":") ? "[#{host}]" : host
          Listener.new("mqtt://#{host_part}:#{actual}", server, actual, engine)
        end


        def connect(endpoint, engine, **)
          host, port = parse_endpoint(endpoint)
          sock = ::Socket.tcp(host, port, connect_timeout: connect_timeout(engine.options))
          engine.handle_connected(IO::Stream::Buffered.wrap(sock), endpoint: endpoint)
        end


        def connect_timeout(options)
          ri = options.reconnect_interval
          ri = ri.end if ri.is_a?(Range)
          [ri, 0.5].max
        end


        def parse_endpoint(endpoint)
          uri = URI.parse(endpoint.sub("mqtt://", "tcp://"))
          [uri.hostname, uri.port]
        end
      end


      class Listener
        attr_reader :endpoint, :port


        def initialize(endpoint, server, port, engine)
          @endpoint = endpoint
          @server   = server
          @port     = port
          @engine   = engine
          @task     = nil
        end


        def start_accept_loop(parent_task, &on_accepted)
          @task = parent_task.async(annotation: "zzq mqtt accept #{@endpoint}") do
            loop do
              client = @server.accept
              on_accepted.call(IO::Stream::Buffered.wrap(client))
            rescue Async::Stop, IOError
              break
            end
          ensure
            @server.close rescue nil
          end
        end


        def stop
          @task&.stop
          @server.close rescue nil
        end
      end
    end
  end
end

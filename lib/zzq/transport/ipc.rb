# frozen_string_literal: true

require "socket"
require "io/stream"
require_relative "../engine"

module ZZQ
  module Transport
    # IPC transport using Unix domain sockets.
    #
    # Endpoint forms:
    #   * `ipc:///var/run/zzq.sock` — filesystem path (portable).
    #   * `ipc://@zzq-local`        — Linux abstract namespace (the
    #     leading `@` becomes a NUL byte in sockaddr.sun_path).
    module IPC
      Engine.transports["ipc"] = self


      class << self
        def bind(endpoint, engine, **)
          path      = parse_path(endpoint)
          sock_path = to_socket_path(path)

          File.delete(sock_path) if !abstract?(path) && File.exist?(sock_path)

          server = UNIXServer.new(sock_path)
          Listener.new(endpoint, server, path, engine)
        end


        def connect(endpoint, engine, **)
          path      = parse_path(endpoint)
          sock_path = to_socket_path(path)
          sock      = UNIXSocket.new(sock_path)
          engine.handle_connected(IO::Stream::Buffered.wrap(sock), endpoint: endpoint)
        end


        def parse_path(endpoint)
          endpoint.sub(%r{\Aipc://}, "")
        end


        # Linux abstract-namespace Unix sockets: leading `@` becomes a
        # NUL byte in sun_path. Only supported on Linux.
        def to_socket_path(path)
          abstract?(path) ? "\0#{path[1..]}" : path
        end


        def abstract?(path)
          path.start_with?("@")
        end
      end


      class Listener
        attr_reader :endpoint


        def initialize(endpoint, server, path, engine)
          @endpoint = endpoint
          @server   = server
          @path     = path
          @engine   = engine
          @task     = nil
        end


        def start_accept_loop(parent_task, &on_accepted)
          @task = parent_task.async(annotation: "zzq ipc accept #{@endpoint}") do
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
          File.delete(@path) rescue nil unless IPC.abstract?(@path)
        end
      end
    end
  end
end

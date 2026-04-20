# frozen_string_literal: true

require "socket"
require "openssl"
require "uri"
require "io/stream"
require_relative "../engine"

module ZZQ
  module Transport
    # MQTT-over-TLS transport — endpoint scheme `mqtts://host:port`
    # (default port 8883).
    #
    # Both bind and connect accept a `tls_context:` option — any
    # +OpenSSL::SSL::SSLContext+. On bind the context must carry a
    # cert + key; on connect a default client context is used if
    # none is given. The transport does not constrain the context;
    # see the README for a recommended TLS 1.3 +
    # `TLS_CHACHA20_POLY1305_SHA256` configuration.
    #
    # Per-connection application data is capped at
    # {DATA_LIMIT} bytes, after which the socket is torn down and
    # the peer must reconnect. This keeps AEAD nonce counters well
    # below their unsafe bound (e.g. ChaCha20-Poly1305's ~256 GiB
    # per-key integrity bound) without relying on a specific cipher
    # choice.
    module MQTTS
      Engine.transports["mqtts"] = self

      # 128 GiB — conservative for every AEAD in TLS 1.3 (ChaCha20-
      # Poly1305 at ~256 GiB, AES-GCM limits are higher under TLS 1.3
      # record sizes). Reconnection negotiates fresh keys, so this
      # caps nonce reuse exposure regardless of cipher choice.
      DATA_LIMIT = 128 * 1024 * 1024 * 1024


      class << self
        def bind(endpoint, engine, tls_context: nil, **)
          raise Error, "mqtts:// bind requires tls_context:" unless tls_context

          host, port = parse_endpoint(endpoint)
          host       = "0.0.0.0" if host == "*"
          server     = TCPServer.new(host, port)
          actual     = server.local_address.ip_port
          host_part  = host.include?(":") ? "[#{host}]" : host
          Listener.new("mqtts://#{host_part}:#{actual}", server, tls_context, engine)
        end


        def connect(endpoint, engine, tls_context: nil, **)
          host, port = parse_endpoint(endpoint)
          ctx = tls_context || OpenSSL::SSL::SSLContext.new
          tcp = ::Socket.tcp(host, port, connect_timeout: connect_timeout(engine.options))
          ssl = OpenSSL::SSL::SSLSocket.new(tcp, ctx)
          ssl.hostname = host  # SNI
          ssl.sync_close = true
          ssl.connect
          engine.handle_connected(LimitedStream.wrap(ssl), endpoint: endpoint)
        end


        def connect_timeout(options)
          ri = options.reconnect_interval
          ri = ri.end if ri.is_a?(Range)
          [ri, 0.5].max
        end


        def parse_endpoint(endpoint)
          uri = URI.parse(endpoint.sub("mqtts://", "tcp://"))
          [uri.hostname, uri.port]
        end
      end


      class Listener
        attr_reader :endpoint


        def initialize(endpoint, server, tls_context, engine)
          @endpoint = endpoint
          @server   = server
          @ctx      = tls_context
          @engine   = engine
          @task     = nil
        end


        def start_accept_loop(parent_task, &on_accepted)
          @task = parent_task.async(annotation: "zzq mqtts accept #{@endpoint}") do |task|
            loop do
              client = @server.accept
              # TLS handshake per-connection, off the accept loop, so
              # one slow handshake can't block the next client.
              task.async(annotation: "zzq mqtts handshake") do
                ssl = OpenSSL::SSL::SSLSocket.new(client, @ctx)
                ssl.sync_close = true
                ssl.accept
                on_accepted.call(LimitedStream.wrap(ssl))
              rescue Async::Stop, IOError, OpenSSL::SSL::SSLError
                ssl&.close rescue nil
                client.close rescue nil
              end
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


      # Buffered stream with a per-connection byte cap. When the cap
      # is reached on either direction the underlying socket is
      # closed and the next I/O raises — forcing the peer to
      # reconnect and negotiate fresh keys before ChaCha20-Poly1305's
      # nonce counter approaches its unsafe range.
      class LimitedStream < IO::Stream::Buffered
        def self.wrap(io, limit: DATA_LIMIT)
          if io.respond_to?(:buffered=)
            io.buffered = false
          elsif io.respond_to?(:sync=)
            io.sync = true
          end
          new(io, limit)
        end


        def initialize(io, limit = DATA_LIMIT)
          super(io)
          @limit = limit
          @bytes = 0
        end


        protected


        def syswrite(buffer)
          enforce!(buffer.bytesize)
          super
        end


        def sysread(size, buffer)
          result = super
          enforce!(result.bytesize) if result.is_a?(String)
          result
        end


        private


        def enforce!(n)
          @bytes += n
          return if @bytes < @limit

          close rescue nil
          raise ZZQ::Error,
            "mqtts:// data limit reached (#{@limit} B): dropping connection to force fresh keys"
        end
      end
    end
  end
end

# frozen_string_literal: true

module ZZQ
  # An MQTT 3.1.1 and 5.0 broker with an MQTT-SN gateway on +udp://+. It
  # runs on the process's IO lanes; see {ZZQ.io_threads=}.
  #
  # @example
  #   broker = ZZQ::Broker.new
  #   broker.bind("mqtt://0.0.0.0:1883")
  class Broker
    OPTIONS = %i[
      max_packet_size max_inflight max_queued max_queued_bytes receive_maximum max_subscriptions
      max_retained max_topic_levels topic_alias_maximum max_session_expiry max_connections
      max_state_bytes max_keep_alive connect_timeout sys_interval persistence state_file tls
    ].freeze

    # Starts a broker and binds it. With a block, yields it and closes it
    # after.
    #
    # @param endpoint [String] see {#bind}
    # @param options [Hash] see {#initialize}
    # @return [Broker, Object] the broker, or the block's result
    def self.bind(endpoint, **options)
      broker = new(**options)
      broker.bind(endpoint)
      return broker unless block_given?

      begin
        yield broker
      ensure
        broker.close
      end
    end

    # @return [String, nil] the endpoint bound last
    attr_reader :last_endpoint

    # Starts a broker.
    #
    # @param options [Hash] limits as in zzq-broker's +BrokerOptions+, such
    #   as +max_connections:+ or +max_queued:+; +max_keep_alive:+ and
    #   +sys_interval:+ (seconds, nil for none); +connect_timeout:+
    #   (seconds); +persistence:+ (path of a file for retained messages);
    #   +state_file:+ (path of a file that keeps sessions over a restart:
    #   {#close} writes it, and a broker started with it loads it and renames
    #   it to +<path>.loaded+); +tls:+ (+cert:+, +key:+, +client_ca:+ as PEM
    #   strings)
    def initialize(**options)
      opts = Options.check(options, OPTIONS)
      @native = Waiting.await(Native::Broker.start(opts))
      @closed = false
    end

    # Listens on an endpoint.
    #
    # @param endpoint [String] +mqtt://host:port+, +mqtts://+, +tcp://+,
    #   +tls://+, +ws://+, +wss://+, +quic://+, +udp://+ (MQTT-SN),
    #   +ipc://+ or +inproc://+; port 0 picks a free port
    # @return [String] the bound endpoint, with the actual port
    def bind(endpoint)
      @last_endpoint = Waiting.await(native.bind(Options.endpoint(endpoint)))
    end

    # Publishes a message as the broker itself.
    #
    # @param topic [String]
    # @param payload [String]
    # @param qos [Integer]
    # @param retain [Boolean]
    # @return [nil]
    def publish(topic, payload, qos: 0, retain: false)
      payload = payload.to_s unless payload.is_a?(String)
      native.publish(topic, payload, qos, retain)
    end

    # @return [Hash{Symbol => Integer}] counters summed over lanes
    def stats
      Waiting.await(native.stats)
    end

    # Stops the broker, once calls on other threads such as {#stats} are
    # done, and waits until its connections are closed. With +state_file:+,
    # it first closes them as lost, so wills go out, and writes the
    # sessions.
    #
    # @return [nil]
    # @raise [Error] if the state file could not be written
    # @raise [ClosedError] if a lane stopped before it could close the
    #   connections; the broker stops anyway
    def close
      return if @closed

      @closed = true
      Waiting.await(@native.shutdown)
      nil
    end

    # @return [Boolean]
    def closed?
      @closed
    end

    private

    def native
      raise ClosedError, "broker closed" if @closed

      @native
    end
  end
end

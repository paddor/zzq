# frozen_string_literal: true

module ZZQ
  # An MQTT 3.1.1 or 5.0 client. Calls wait without blocking other fibers
  # under a fiber scheduler such as Async, and release the GVL in threads.
  #
  # Publishing and subscribing work from any fiber or thread. One fiber at
  # a time receives.
  #
  # @example
  #   ZZQ::Client.connect("mqtt://localhost", client_id: "reader") do |client|
  #     client.subscribe("sensors/#", qos: 1)
  #     client.each { |message| puts "#{message.topic}: #{message.payload}" }
  #   end
  class Client
    OPTIONS = %i[
      client_id version clean_start keep_alive session_expiry username password will inbox
      reconnect connect_timeout tls
    ].freeze

    # Messages a receive takes from the native inbox at once.
    BATCH = 64

    # Connects a new client. With a block, yields it and closes it after.
    #
    # @param endpoint [String] see {#connect}
    # @param options [Hash] see {#initialize}
    # @return [Client, Object] the client, or the block's result
    def self.connect(endpoint, **options, &block)
      new(**options).connect(endpoint, &block)
    end

    # @param client_id [String] empty lets a v5 broker assign one
    # @param version [Integer] 5 (default) or 3 for MQTT 3.1.1
    # @param clean_start [Boolean] start a new session (default true)
    # @param keep_alive [Integer] seconds, 60 by default
    # @param session_expiry [Integer] v5 seconds the broker keeps the session
    # @param username [String, nil]
    # @param password [String, nil]
    # @param will [Hash, nil] +topic:+, +payload:+, +qos:+, +retain:+, +delay:+
    # @param inbox [Integer] messages the client buffers before it stops
    #   reading, 4096 by default
    # @param reconnect [Boolean, Hash] reconnect after a loss, optionally
    #   with +min:+ and +max:+ seconds between attempts
    # @param connect_timeout [Numeric] seconds, 10 by default
    # @param tls [Hash, nil] +ca:+, +cert:+, +key:+ (PEM strings),
    #   +server_name:+, +insecure:+
    def initialize(**options)
      @options = Options.check(options, OPTIONS)
      @native = nil
      @inbox = []
      @receiving = false
      @closed = false
    end

    # Connects and waits for CONNACK. With a block, yields the client and
    # closes it after.
    #
    # @param endpoint [String] +mqtt://host[:port]+, +mqtts://+, +tcp://+,
    #   +tls://+, +ws://+, +wss://+, +quic://+, +ipc://+ or +inproc://+
    # @return [self, Object] the client, or the block's result
    # @raise [ConnectionRefused] if the broker refuses the connection
    # @raise [TransportError] if the connection fails
    def connect(endpoint)
      raise Error, "already connected" if @native

      @native = Waiting.await(Native::Client.connect(Options.endpoint(endpoint), @options))
      return self unless block_given?

      begin
        yield self
      ensure
        close
      end
    end

    # @return [Boolean] whether the broker resumed a session
    def session_present?
      native.session_present?
    end

    # @return [String, nil] the client ID a v5 broker assigned
    def assigned_client_id
      native.assigned_client_id
    end

    # Publishes a message. QoS 0 returns once queued, QoS 1 and 2 once the
    # broker acked it.
    #
    # @param topic [String]
    # @param payload [String]
    # @param qos [Integer] 0, 1 or 2
    # @param retain [Boolean] the broker keeps it for later subscribers;
    #   an empty payload clears the topic
    # @return [nil]
    def publish(topic, payload, qos: 0, retain: false)
      payload = payload.to_s unless payload.is_a?(String)
      op = native.publish(topic, payload, qos, retain)
      Waiting.await(op) if op
      nil
    end

    # Subscribes to a topic filter.
    #
    # @param filter [String]
    # @param qos [Integer] maximum QoS
    # @param no_local [Boolean] v5: skip this client's own publishes
    # @param retain_as_published [Boolean] v5: keep the publisher's retain
    #   flag
    # @param retain_handling [Integer] v5: 0 send retained messages, 1 only
    #   for a new subscription, 2 never
    # @param subscription_id [Integer] v5 Subscription Identifier, 0 for
    #   none
    # @return [Integer] the granted QoS
    # @raise [Rejected] if the broker refuses the filter
    def subscribe(filter, qos: 0, no_local: false, retain_as_published: false,
                  retain_handling: 0, subscription_id: 0)
      spec = [filter, qos, no_local, retain_as_published, retain_handling]
      code = Waiting.await(native.subscribe([spec], subscription_id)).first
      raise Rejected.new("subscribe to #{filter.inspect} refused", code) if code >= 0x80

      code
    end

    # Unsubscribes from topic filters.
    #
    # @param filters [Array<String>]
    # @return [nil]
    # @raise [Rejected] if a v5 broker refuses one
    def unsubscribe(*filters)
      codes = Waiting.await(native.unsubscribe(filters.flatten))
      refused = codes.find { |code| code >= 0x80 }
      raise Rejected.new("unsubscribe refused", refused) if refused

      nil
    end

    # Takes the next message, waiting for one.
    #
    # @param timeout [Numeric, nil] seconds
    # @return [Message, nil] nil once the client is closed
    # @raise [IO::TimeoutError] if no message came within +timeout+
    def receive(timeout: nil)
      return @inbox.shift unless @inbox.empty?
      return nil unless @native
      raise Error, "another fiber is receiving" if @receiving

      @receiving = true
      begin
        next_batch(timeout)
      ensure
        @receiving = false
      end
    end

    # Yields each message until the client is closed.
    #
    # @yieldparam message [Message]
    # @return [Enumerator, nil] an Enumerator without a block
    def each
      return enum_for(:each) unless block_given?

      while (message = receive)
        yield message
      end
    end

    # Sends DISCONNECT and closes. A fiber waiting in {#receive} gets nil.
    #
    # @param reason [Integer] v5 reason code; 0x04 asks for the will
    # @return [nil]
    def disconnect(reason: 0)
      return if @closed || @native.nil?

      @closed = true
      Waiting.await(@native.disconnect(reason))
      nil
    end

    # Closes with a normal DISCONNECT.
    #
    # @return [nil]
    def close
      disconnect
    end

    # @return [Boolean]
    def closed?
      @closed
    end

    private

    # Waits for messages, keeps them in the inbox and returns the first.
    def next_batch(timeout)
      Waiting.with_wake do |wake|
        limit = Waiting.deadline(timeout)
        loop do
          batch = @native.receive(wake, BATCH)
          return nil if batch.nil?

          if batch.is_a?(Array)
            @inbox = batch
            return @inbox.shift
          end
          wake.wait(limit)
        end
      end
    end

    def native
      raise ClosedError, "client closed" if @closed
      raise ClosedError, "client not connected" unless @native

      @native
    end
  end
end

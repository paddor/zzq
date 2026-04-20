# frozen_string_literal: true

require "async"
require "async/promise"
require "protocol/mqtt"
require_relative "../engine/keepalive"
require_relative "../message"
require_relative "../subscription"
require_relative "packet_id_allocator"
require_relative "qos_tracker"
require_relative "topic_match"

module ZZQ
  module Routing
    # Client-side routing.
    #
    # Responsibilities:
    #   * CONNECT / CONNACK handshake on +handle_connected+.
    #   * Per-connection read loop dispatching PINGRESP, DISCONNECT,
    #     PUBLISH (fan-out to matching subscriptions), SUBACK/UNSUBACK
    #     (resolve pending acks), and PUBACK/PUBREC/PUBREL/PUBCOMP
    #     (drive outbound QoS 1/2 state machines).
    #   * Keepalive fiber.
    #   * QoS 0/1/2 publish and subscribe.
    class Client
      HANDSHAKE_TIMEOUT = 10  # seconds to wait for CONNACK
      SUBSCRIBE_TIMEOUT = 30  # seconds to wait for SUBACK / UNSUBACK
      PUBLISH_TIMEOUT   = 30  # seconds to wait for terminal ack on qos>0


      # @return [Protocol::MQTT::Packet::Connack, nil] the most recent
      #   CONNACK received, or nil before the first connect.
      attr_reader :last_connack


      def initialize(engine)
        @engine        = engine
        @conn          = nil
        @keepalive     = nil
        @last_connack  = nil
        @subscriptions = []   # ZZQ::Subscription
        @packet_ids    = PacketIdAllocator.new(
          receive_maximum: @engine.options.receive_maximum,
        )
        @qos_tracker   = QosTracker.new
        @pending_ctl   = {}   # packet_id => Async::Promise (SUBACK / UNSUBACK)
        @inbound_qos2  = {}   # packet_id => Message awaiting PUBREL (manual-ack path; M9)
      end


      def session_present?
        @last_connack&.session_present || false
      end


      # Called by Engine#handle_connected. Runs inside the caller's
      # Async task (Socket#connect → Engine.connect → Transport.connect
      # → Engine.handle_connected), so we can synchronously drive the
      # CONNECT/CONNACK exchange and raise to the caller on refusal.
      def handle_connected(mqtt, lifecycle)
        opts = @engine.options
        mqtt.version = opts.version

        connect_packet = build_connect(opts)

        Async::Task.current.with_timeout(HANDSHAKE_TIMEOUT) do
          mqtt.write_packet(connect_packet)
          reply = mqtt.read_packet
          raise ConnectionRefused.new(0, "no CONNACK from broker") if reply.nil?
          raise ConnectionRefused.new(0, "expected CONNACK, got #{reply.class.name}") unless reply.is_a?(Protocol::MQTT::Packet::Connack)
          unless success_connack?(reply)
            raise ConnectionRefused.new(reply.reason_code)
          end
          @last_connack = reply
        end

        @conn = mqtt
        lifecycle.handshake_succeeded!

        start_read_loop(mqtt, lifecycle)
        start_keepalive(mqtt, lifecycle)
      end


      def connection_added(_conn); end


      def connection_removed(conn)
        return unless @conn.equal?(conn)
        @conn = nil
        @subscriptions.each(&:close)
        @subscriptions.clear
        @pending_ctl.each_value { |p| p.resolve(nil) unless p.resolved? }
        @pending_ctl.clear
        # Wake any fiber blocked on a publish ack. Don't clear the
        # tracker yet — M6 retransmit-on-reconnect will want to inspect
        # +inflight_outbound+. For now, just unblock with nil.
        @qos_tracker.inflight_outbound.each do |pending|
          pending.promise.resolve(nil) unless pending.promise.resolved?
        end
      end


      def close
        return unless @conn
        begin
          @conn.write_packet(Protocol::MQTT::Packet::Disconnect.new)
        rescue IOError, Errno::EPIPE, Errno::ECONNRESET, ClosedError
          # peer already gone; fine
        end
        @conn = nil
      end


      # Send a PUBLISH. For QoS 0 returns immediately after write. For
      # QoS 1 blocks the fiber until PUBACK. For QoS 2 blocks until
      # PUBCOMP.
      def publish(topic:, payload:, qos: 0, retain: false, properties: {})
        raise ClosedError, "client not connected" unless @conn
        if qos.zero?
          pkt = Protocol::MQTT::Packet::Publish.new(
            topic: topic, payload: payload, qos: 0, retain: retain,
            properties: properties,
          )
          @conn.write_packet(pkt)
          @keepalive&.note_sent
          return nil
        end

        packet_id = @packet_ids.acquire
        pkt = Protocol::MQTT::Packet::Publish.new(
          topic: topic, payload: payload, qos: qos, retain: retain,
          packet_id: packet_id, properties: properties,
        )
        pending = qos == 1 \
          ? @qos_tracker.track_qos1(packet_id, pkt) \
          : @qos_tracker.track_qos2(packet_id, pkt)
        begin
          @conn.write_packet(pkt)
          @keepalive&.note_sent
          result = Async::Task.current.with_timeout(PUBLISH_TIMEOUT) { pending.promise.wait }
          raise ClosedError, "connection lost before publish ack" if result.nil?
        ensure
          @packet_ids.release(packet_id)
        end
        nil
      end


      # Send a SUBSCRIBE, wait for SUBACK, return a Subscription.
      def subscribe(filter, qos: 0, no_local: false, retain_as_published: false,
                    retain_handling: 0)
        raise ClosedError, "client not connected" unless @conn
        packet_id = @packet_ids.acquire
        filter_entry = {
          filter: filter, qos: qos, no_local: no_local,
          retain_as_published: retain_as_published,
          retain_handling: retain_handling,
        }
        subscription = Subscription.new(
          filters: [filter],
          hwm: @engine.options.recv_hwm,
          on_close: ->(sub) { send_unsubscribe(sub) },
        )
        @subscriptions << subscription

        promise = Async::Promise.new
        @pending_ctl[packet_id] = promise
        begin
          @conn.write_packet(Protocol::MQTT::Packet::Subscribe.new(
            packet_id: packet_id, filters: [filter_entry],
          ))
          @keepalive&.note_sent

          suback = Async::Task.current.with_timeout(SUBSCRIBE_TIMEOUT) { promise.wait }
          raise ClosedError, "connection lost before SUBACK" if suback.nil?
          reason = suback.reason_codes.first
          if reason >= 0x80
            @subscriptions.delete(subscription)
            raise Error, "SUBSCRIBE refused: reason=0x#{reason.to_s(16)}"
          end
        ensure
          @packet_ids.release(packet_id)
        end
        subscription
      end


      private


      def build_connect(opts)
        Protocol::MQTT::Packet::Connect.new(
          client_id: opts.client_id.to_s,
          clean_start: opts.clean_start,
          keep_alive: opts.keep_alive,
          username: opts.username,
          password: opts.password,
          will: opts.will,
          properties: build_connect_properties(opts),
        )
      end


      def build_connect_properties(opts)
        return {} unless opts.version == 5
        props = {}
        props[:session_expiry_interval] = opts.session_expiry_interval if opts.session_expiry_interval > 0
        props[:receive_maximum]         = opts.receive_maximum         if opts.receive_maximum != 65_535
        props[:maximum_packet_size]     = opts.max_packet_size         if opts.max_packet_size
        props[:topic_alias_maximum]     = opts.topic_alias_maximum     if opts.topic_alias_maximum > 0
        props
      end


      def success_connack?(ack)
        # v3 accepted = 0x00; v5 success = 0x00. Both map to 0.
        ack.reason_code.zero?
      end


      def send_unsubscribe(subscription)
        return unless @conn
        packet_id = @packet_ids.acquire
        promise = Async::Promise.new
        @pending_ctl[packet_id] = promise
        begin
          @conn.write_packet(Protocol::MQTT::Packet::Unsubscribe.new(
            packet_id: packet_id, filters: subscription.filters.dup,
          ))
          @keepalive&.note_sent
        rescue *CONNECTION_LOST, ClosedError
          @pending_ctl.delete(packet_id)
        ensure
          @packet_ids.release(packet_id)
        end
        @subscriptions.delete(subscription)
      end


      def start_read_loop(mqtt, lifecycle)
        lifecycle.barrier.async(annotation: "zzq client read #{lifecycle.endpoint}") do
          loop do
            pkt = mqtt.read_packet
            break if pkt.nil?  # clean EOF
            @keepalive&.note_received
            dispatch(pkt, mqtt)
          rescue *CONNECTION_LOST, Protocol::MQTT::MalformedPacket
            break
          end
        ensure
          lifecycle.lost! unless lifecycle.state == :closed
        end
      end


      def start_keepalive(mqtt, lifecycle)
        ka = @engine.options.keep_alive
        return if ka.zero?
        @keepalive = Engine::Keepalive.new(
          mqtt, keep_alive: ka,
          on_timeout: -> { lifecycle.lost! },
        )
        lifecycle.barrier.async(annotation: "zzq client keepalive") do
          @keepalive.run
        end
      end


      def dispatch(pkt, mqtt)
        case pkt
        when Protocol::MQTT::Packet::Pingresp
          # liveness refreshed via @keepalive.note_received above
        when Protocol::MQTT::Packet::Disconnect
          raise EOFError  # cleanly unwind the read loop
        when Protocol::MQTT::Packet::Publish
          handle_inbound_publish(pkt, mqtt)
        when Protocol::MQTT::Packet::Suback, Protocol::MQTT::Packet::Unsuback
          promise = @pending_ctl.delete(pkt.packet_id)
          promise&.resolve(pkt)
        when Protocol::MQTT::Packet::Puback
          @qos_tracker.on_puback(pkt.packet_id)
        when Protocol::MQTT::Packet::Pubrec
          entry = @qos_tracker.on_pubrec(pkt.packet_id)
          if entry
            mqtt.write_packet(Protocol::MQTT::Packet::Pubrel.new(packet_id: pkt.packet_id))
            @keepalive&.note_sent
          end
        when Protocol::MQTT::Packet::Pubrel
          # Inbound half of QoS 2 (broker → client). Release the held
          # dedup entry and acknowledge with PUBCOMP.
          @qos_tracker.on_pubrel(pkt.packet_id)
          mqtt.write_packet(Protocol::MQTT::Packet::Pubcomp.new(packet_id: pkt.packet_id))
          @keepalive&.note_sent
        when Protocol::MQTT::Packet::Pubcomp
          @qos_tracker.on_pubcomp(pkt.packet_id)
        end
      end


      def handle_inbound_publish(pkt, mqtt)
        case pkt.qos
        when 0
          fan_out_publish(pkt)
        when 1
          fan_out_publish(pkt)
          mqtt.write_packet(Protocol::MQTT::Packet::Puback.new(packet_id: pkt.packet_id))
          @keepalive&.note_sent
        when 2
          # Only fan out the first receipt; duplicates (same packet id
          # before PUBREL) are dropped but still PUBRECed.
          fan_out_publish(pkt) if @qos_tracker.on_qos2_publish(pkt.packet_id)
          mqtt.write_packet(Protocol::MQTT::Packet::Pubrec.new(packet_id: pkt.packet_id))
          @keepalive&.note_sent
        end
      end


      def fan_out_publish(pkt)
        msg = Message.from_publish(pkt)
        @subscriptions.each do |sub|
          sub.deliver(msg) if sub.filters.any? { |f| TopicMatch.match?(f, pkt.topic) }
        end
      end
    end
  end
end

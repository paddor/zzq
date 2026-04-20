# frozen_string_literal: true

require "async"
require "securerandom"
require "protocol/mqtt"
require_relative "../engine/keepalive"
require_relative "../message"
require_relative "packet_id_allocator"
require_relative "qos_tracker"
require_relative "retained_store"
require_relative "topic_match"
require_relative "topic_trie"

module ZZQ
  module Routing
    # Broker-side routing.
    #
    # Current scope:
    #   * Read CONNECT, send CONNACK.
    #   * Per-connection read loop: PINGREQ/PINGRESP, DISCONNECT,
    #     PUBLISH fan-out to matching sessions (QoS 0/1/2), SUBSCRIBE /
    #     UNSUBSCRIBE with SUBACK / UNSUBACK, and PUBACK/PUBREC/PUBREL/
    #     PUBCOMP to drive outbound QoS state per session.
    #   * Simple linear session map; proper topic trie + retained store
    #     come in M7.
    class Broker
      CONNECT_TIMEOUT = 10  # seconds to wait for the initial CONNECT


      Session = Struct.new(
        :client_id, :conn, :subscriptions, :packet_ids, :qos_tracker,
        :will, :keepalive, :clean_disconnect,
        keyword_init: true,
      )


      def initialize(engine)
        @engine     = engine
        @sessions   = {}  # client_id  => Session
        @by_conn    = {}  # Connection => Session
        @trie       = TopicTrie.new
        @retained   = RetainedStore.new(persistence: engine.options.persistence)
      end


      def handle_accepted(mqtt, lifecycle)
        connect = nil
        Async::Task.current.with_timeout(CONNECT_TIMEOUT) do
          connect = mqtt.read_connect_packet
          raise ConnectionRejected, "no CONNECT received" if connect.nil?

          client_id = connect.client_id
          if client_id.empty? && mqtt.version == 3 && !connect.clean_start
            write_connack(mqtt, reason: Protocol::MQTT::ReasonCodes::V3Connack::IDENTIFIER_REJECTED)
            raise ConnectionRejected, "empty ClientId with clean_session=false is illegal in v3"
          end
          assigned = client_id.empty?
          client_id = generate_client_id if assigned

          # v5 Receive Maximum: client advertises the max number of
          # outstanding QoS > 0 publishes it is willing to accept. Sized
          # on the packet-id allocator used for fan-out to this session.
          client_rcv_max = connect.properties[:receive_maximum] || 65_535
          session = build_session(client_id, mqtt, will: connect.will,
                                   receive_maximum: client_rcv_max)
          @sessions[client_id] = session
          @by_conn[mqtt] = session

          write_connack(mqtt, reason: 0, assigned_client_id: assigned ? client_id : nil)
        end

        lifecycle.handshake_succeeded!
        start_read_loop(mqtt, lifecycle)
        start_keepalive(mqtt, lifecycle, connect.keep_alive)
      end


      def connection_added(_conn); end


      def connection_removed(conn)
        session = @by_conn.delete(conn)
        return unless session
        publish_will(session) unless session.clean_disconnect
        @sessions.delete(session.client_id)
        @trie.remove_session(session)
      end


      # Publish the client's Last Will and Testament via the normal
      # dispatch path. Called when the session is torn down without
      # having seen a clean DISCONNECT.
      def publish_will(session)
        will = session.will
        return unless will
        msg = Message.new(
          topic:      will.fetch(:topic),
          payload:    will.fetch(:payload),
          qos:        will.fetch(:qos, 0),
          retain:     will.fetch(:retain, false),
          properties: will.fetch(:properties, {}) || {},
        )
        # Also honor RETAIN on wills.
        apply_retain(msg) if msg.retain
        dispatch_message(msg)
      end


      def close; end


      # Used by tests.
      def session_count = @sessions.size


      # Fan out a Message to every session subscribed to a matching
      # filter. Used by both inbound PUBLISH handling and
      # {Broker#ingest} from a non-MQTT source.
      def dispatch_message(message, origin: nil)
        delivered = {}  # session.object_id => true  (one copy per session)
        @trie.match(message.topic).each do |hit|
          # v5 No Local (MQTT-3.8.3-3): when set on the subscription,
          # don't echo a publish back to the publishing client.
          next if hit.entry[:no_local] && origin && hit.session.conn.equal?(origin)
          next if delivered[hit.session.object_id]
          delivered[hit.session.object_id] = true
          forward_publish(hit.session, message, hit.entry)
        end
      end


      private


      def build_session(client_id, mqtt, will: nil, receive_maximum: 65_535)
        Session.new(
          client_id:        client_id,
          conn:             mqtt,
          subscriptions:    [],
          packet_ids:       PacketIdAllocator.new(receive_maximum: receive_maximum),
          qos_tracker:      QosTracker.new,
          will:             will,
          keepalive:        nil,
          clean_disconnect: false,
        )
      end


      def start_keepalive(mqtt, lifecycle, keep_alive)
        return if keep_alive.nil? || keep_alive.zero?
        session = @by_conn[mqtt] or return
        session.keepalive = Engine::Keepalive.new(
          mqtt, keep_alive: keep_alive, role: :watcher,
          on_timeout: -> { lifecycle.lost! },
        )
        lifecycle.barrier.async(annotation: "zzq broker keepalive #{lifecycle.endpoint}") do
          session.keepalive.run
        end
      end


      def write_connack(mqtt, reason:, assigned_client_id: nil, session_present: false)
        props = {}
        props[:assigned_client_identifier] = assigned_client_id if assigned_client_id && mqtt.version == 5
        mqtt.write_packet(
          Protocol::MQTT::Packet::Connack.new(
            session_present: session_present,
            reason_code: reason,
            properties: props,
          ),
        )
      end


      def generate_client_id
        "zzq-#{SecureRandom.hex(6)}"
      end


      def start_read_loop(mqtt, lifecycle)
        lifecycle.barrier.async(annotation: "zzq broker read #{lifecycle.endpoint}") do
          loop do
            pkt = mqtt.read_packet
            break if pkt.nil?
            @by_conn[mqtt]&.keepalive&.note_received
            dispatch(pkt, mqtt)
          rescue *CONNECTION_LOST, Protocol::MQTT::MalformedPacket
            break
          end
        ensure
          lifecycle.lost! unless lifecycle.state == :closed
        end
      end


      def dispatch(pkt, mqtt)
        case pkt
        when Protocol::MQTT::Packet::Pingreq
          mqtt.write_packet(Protocol::MQTT::Packet::Pingresp.new)
        when Protocol::MQTT::Packet::Disconnect
          # Clean DISCONNECT: v3 = any disconnect; v5 = reason 0x00
          # (NORMAL_DISCONNECTION). v5 reason 0x04 explicitly requests
          # that the will still be published. Others suppress (spec).
          session = @by_conn[mqtt]
          if session
            reason = mqtt.version == 5 ? pkt.reason_code : 0
            session.clean_disconnect = (reason != 0x04)
          end
          raise EOFError  # cleanly unwind the read loop
        when Protocol::MQTT::Packet::Publish
          handle_publish(pkt, mqtt)
        when Protocol::MQTT::Packet::Subscribe
          handle_subscribe(pkt, mqtt)
        when Protocol::MQTT::Packet::Unsubscribe
          handle_unsubscribe(pkt, mqtt)
        when Protocol::MQTT::Packet::Puback
          handle_puback(pkt, mqtt)
        when Protocol::MQTT::Packet::Pubrec
          handle_pubrec(pkt, mqtt)
        when Protocol::MQTT::Packet::Pubrel
          handle_pubrel(pkt, mqtt)
        when Protocol::MQTT::Packet::Pubcomp
          handle_pubcomp(pkt, mqtt)
        end
      end


      def handle_publish(pkt, mqtt)
        session = @by_conn[mqtt] or return
        message = Message.from_publish(pkt)
        case pkt.qos
        when 0
          apply_retain(message) if pkt.retain
          dispatch_message(message, origin: mqtt)
        when 1
          apply_retain(message) if pkt.retain
          dispatch_message(message, origin: mqtt)
          mqtt.write_packet(Protocol::MQTT::Packet::Puback.new(packet_id: pkt.packet_id))
        when 2
          # Fan out only on first receipt; duplicates are still PUBRECed.
          if session.qos_tracker.on_qos2_publish(pkt.packet_id)
            apply_retain(message) if pkt.retain
            dispatch_message(message, origin: mqtt)
          end
          mqtt.write_packet(Protocol::MQTT::Packet::Pubrec.new(packet_id: pkt.packet_id))
        end
      end


      def apply_retain(message)
        @retained.store(message)
      end


      def handle_subscribe(pkt, mqtt)
        session = @by_conn[mqtt] or return
        added = []
        reasons = pkt.filters.map do |entry|
          sub_entry = {
            filter: entry.fetch(:filter),
            qos: entry.fetch(:qos, 0),
            no_local: entry.fetch(:no_local, false),
            retain_as_published: entry.fetch(:retain_as_published, false),
            retain_handling: entry.fetch(:retain_handling, 0),
          }
          session.subscriptions << sub_entry
          @trie.add(sub_entry[:filter], session, sub_entry)
          added << sub_entry
          sub_entry[:qos]  # granted QoS == requested (MVP)
        end
        mqtt.write_packet(Protocol::MQTT::Packet::Suback.new(
          packet_id: pkt.packet_id, reason_codes: reasons,
        ))
        # Replay retained messages for each newly-added filter.
        added.each { |sub_entry| replay_retained(session, sub_entry) }
      end


      def handle_unsubscribe(pkt, mqtt)
        session = @by_conn[mqtt]
        reasons = pkt.filters.map do |filter|
          matched = session&.subscriptions&.reject! { |s| s[:filter] == filter }
          @trie.remove(filter, session) if matched && session
          matched && !matched.empty? ? 0x00 : 0x11  # 0x11 = No subscription existed
        end
        mqtt.write_packet(Protocol::MQTT::Packet::Unsuback.new(
          packet_id: pkt.packet_id, reason_codes: reasons,
        ))
      end


      # For each retained message matching the new filter, deliver a
      # PUBLISH copy with RETAIN=1 (MQTT-3.3.1-8). Retain handling per
      # v5: 0 = send retained on every SUBSCRIBE; 1 = only if the sub
      # didn't exist; 2 = never. MVP honours 0/2; 1 is a future polish.
      def replay_retained(session, sub_entry)
        return if sub_entry[:retain_handling] == 2
        @retained.each_matching(sub_entry[:filter]) do |msg|
          forward_publish(session, msg, sub_entry, retain: true)
        end
      end


      # --- outbound-QoS ack handlers -------------------------------------

      def handle_puback(pkt, mqtt)
        session = @by_conn[mqtt] or return
        pending = session.qos_tracker.on_puback(pkt.packet_id)
        session.packet_ids.release(pkt.packet_id) if pending
      end


      def handle_pubrec(pkt, mqtt)
        session = @by_conn[mqtt] or return
        entry = session.qos_tracker.on_pubrec(pkt.packet_id)
        return unless entry
        mqtt.write_packet(Protocol::MQTT::Packet::Pubrel.new(packet_id: pkt.packet_id))
      end


      # Inbound PUBREL: the client is telling us to release its own
      # inbound QoS 2 dedup entry (it published at QoS 2; we PUBRECed;
      # now it's clearing). Respond with PUBCOMP.
      def handle_pubrel(pkt, mqtt)
        session = @by_conn[mqtt] or return
        session.qos_tracker.on_pubrel(pkt.packet_id)
        mqtt.write_packet(Protocol::MQTT::Packet::Pubcomp.new(packet_id: pkt.packet_id))
      end


      def handle_pubcomp(pkt, mqtt)
        session = @by_conn[mqtt] or return
        pending = session.qos_tracker.on_pubcomp(pkt.packet_id)
        session.packet_ids.release(pkt.packet_id) if pending
      end


      def forward_publish(session, message, entry, retain: false)
        granted_qos = [message.qos, entry[:qos]].min
        # retain_as_published (v5): when true, preserve publisher's
        # RETAIN flag; otherwise spec forces RETAIN=0 on fan-out (and
        # =1 only on retained-replay, tracked via the +retain+ arg).
        out_retain =
          if retain
            true
          elsif entry[:retain_as_published]
            message.retain
          else
            false
          end

        if granted_qos.zero?
          out = Protocol::MQTT::Packet::Publish.new(
            topic: message.topic, payload: message.payload,
            qos: 0, retain: out_retain, dup: false, packet_id: nil,
            properties: message.properties,
          )
          session.conn.write_packet(out)
          return
        end

        packet_id = session.packet_ids.acquire
        out = Protocol::MQTT::Packet::Publish.new(
          topic: message.topic, payload: message.payload,
          qos: granted_qos, retain: out_retain, dup: false,
          packet_id: packet_id, properties: message.properties,
        )
        if granted_qos == 1
          session.qos_tracker.track_qos1(packet_id, out)
        else
          session.qos_tracker.track_qos2(packet_id, out)
        end
        session.conn.write_packet(out)
      rescue *CONNECTION_LOST, ClosedError
        # recipient is gone; tear-down handled by their read loop.
        session.packet_ids.release(packet_id) if packet_id
      end
    end
  end
end

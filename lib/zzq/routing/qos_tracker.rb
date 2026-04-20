# frozen_string_literal: true

require "async/promise"

module ZZQ
  module Routing
    # Outbound inflight tracking + inbound QoS 2 dedup for one
    # session.
    #
    # Each side of a QoS 1/2 exchange owns one tracker. The publisher
    # side records a {PendingPublish} per outbound packet id, awaiting
    # a PUBACK (QoS 1) or completing via PUBREC/PUBREL/PUBCOMP
    # (QoS 2). The subscriber side remembers which QoS 2 packet ids
    # have been received-but-not-yet-released so duplicate PUBLISH
    # packets with the same id are dropped.
    class QosTracker
      # One outbound publication awaiting ack.
      #
      # +stage+ is one of:
      #   * :waiting_puback  — QoS 1, awaiting PUBACK
      #   * :waiting_pubrec  — QoS 2, awaiting PUBREC
      #   * :waiting_pubcomp — QoS 2, PUBREC received, PUBREL sent
      PendingPublish = Struct.new(:packet_id, :packet, :stage, :promise, keyword_init: true)


      def initialize
        @outbound       = {}   # packet_id => PendingPublish
        @inbound_qos2   = {}   # packet_id => true (received, awaiting PUBREL)
      end


      # Register an outbound QoS 1 publish. Returns the PendingPublish.
      def track_qos1(packet_id, packet)
        promise = Async::Promise.new
        @outbound[packet_id] = PendingPublish.new(
          packet_id: packet_id, packet: packet, stage: :waiting_puback,
          promise: promise,
        )
      end


      # Register an outbound QoS 2 publish (awaiting PUBREC).
      def track_qos2(packet_id, packet)
        promise = Async::Promise.new
        @outbound[packet_id] = PendingPublish.new(
          packet_id: packet_id, packet: packet, stage: :waiting_pubrec,
          promise: promise,
        )
      end


      # Called on inbound PUBACK. Returns the PendingPublish that
      # completed, or nil if the id was unknown / stage mismatched.
      def on_puback(packet_id)
        pending = @outbound[packet_id]
        return nil unless pending && pending.stage == :waiting_puback
        @outbound.delete(packet_id)
        pending.promise.resolve(:acked) unless pending.promise.resolved?
        pending
      end


      # Called on inbound PUBREC. Transitions the entry to
      # +:waiting_pubcomp+ and returns it so the caller can send
      # PUBREL. Returns nil if unknown / stage mismatched.
      def on_pubrec(packet_id)
        pending = @outbound[packet_id]
        return nil unless pending && pending.stage == :waiting_pubrec
        pending.stage = :waiting_pubcomp
        pending
      end


      # Called on inbound PUBCOMP. Completes a QoS 2 exchange.
      def on_pubcomp(packet_id)
        pending = @outbound[packet_id]
        return nil unless pending && pending.stage == :waiting_pubcomp
        @outbound.delete(packet_id)
        pending.promise.resolve(:acked) unless pending.promise.resolved?
        pending
      end


      # Inbound QoS 2 PUBLISH dedup. Returns true if this id is a new
      # (non-duplicate) reception; false if it's a duplicate.
      def on_qos2_publish(packet_id)
        return false if @inbound_qos2.key?(packet_id)
        @inbound_qos2[packet_id] = true
        true
      end


      # Inbound PUBREL: peer tells us to release the held qos2 id.
      def on_pubrel(packet_id)
        @inbound_qos2.delete(packet_id)
      end


      # Snapshot of still-pending outbound publications, used on
      # reconnect for retransmission.
      def inflight_outbound = @outbound.values


      def outbound_size = @outbound.size
      def inbound_size  = @inbound_qos2.size
    end
  end
end

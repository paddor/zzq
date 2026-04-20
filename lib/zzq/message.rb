# frozen_string_literal: true

require "protocol/mqtt"

module ZZQ
  # One MQTT application message as delivered to a subscriber or
  # accepted by {Client#publish} / {Broker#ingest}.
  #
  # `Message` is independent of any particular connection: it carries
  # every wire-level field that a PUBLISH packet does, so it can be
  # shipped across non-MQTT transports (OMQ, NNQ, a file, a queue) via
  # {#to_wire} / {.from_wire} and re-injected at the other end.
  #
  # `#ack!` is only meaningful for QoS 1/2 messages received with
  # `auto_ack: false` — it sends the appropriate PUBACK/PUBCOMP.
  class Message
    attr_reader :topic, :payload, :qos, :retain, :dup, :packet_id, :properties


    def initialize(topic:, payload:, qos: 0, retain: false, dup: false,
                   packet_id: nil, properties: {}, on_ack: nil)
      @topic      = topic
      @payload    = payload.b.freeze
      @qos        = qos
      @retain     = retain
      @dup        = dup
      @packet_id  = packet_id
      @properties = properties
      @on_ack     = on_ack
      @acked      = false
    end


    def ack!
      return if @qos.zero? || @acked
      @acked = true
      @on_ack&.call(self)
    end


    def acked?
      @acked
    end


    # Encode this message as a self-describing byte string using the
    # PUBLISH packet codec. The output can be cross-shipped over a
    # non-MQTT transport and reconstructed via {.from_wire}. v5 wire
    # format is used so the full property block round-trips.
    def to_wire
      packet = to_publish
      packet.encode(version: 5)
    end


    # Inverse of {#to_wire}. Raises +Protocol::MQTT::MalformedPacket+
    # if the bytes don't decode as a PUBLISH packet.
    def self.from_wire(bytes)
      pkt, consumed = Protocol::MQTT::Packet.decode(bytes, version: 5)
      raise Protocol::MQTT::MalformedPacket, "expected PUBLISH, got #{pkt.class}" unless pkt.is_a?(Protocol::MQTT::Packet::Publish)
      raise Protocol::MQTT::MalformedPacket, "trailing bytes after PUBLISH" if consumed != bytes.bytesize
      from_publish(pkt)
    end


    # Build a PUBLISH packet that represents this message. Used both
    # internally (for over-the-wire transmission) and by {#to_wire}.
    def to_publish
      Protocol::MQTT::Packet::Publish.new(
        topic:      @topic,
        payload:    @payload,
        qos:        @qos,
        retain:     @retain,
        dup:        @dup,
        packet_id:  @packet_id,
        properties: @properties,
      )
    end


    # Build a Message from a decoded PUBLISH packet.
    def self.from_publish(pkt, on_ack: nil)
      new(
        topic:      pkt.topic,
        payload:    pkt.payload,
        qos:        pkt.qos,
        retain:     pkt.retain,
        dup:        pkt.dup,
        packet_id:  pkt.packet_id,
        properties: pkt.properties,
        on_ack:     on_ack,
      )
    end


    def ==(other)
      other.is_a?(Message) &&
        other.topic == @topic &&
        other.payload == @payload &&
        other.qos == @qos &&
        other.retain == @retain &&
        other.dup == @dup &&
        other.packet_id == @packet_id &&
        other.properties == @properties
    end
    alias eql? ==


    def hash
      [self.class, @topic, @payload, @qos, @retain, @dup, @packet_id, @properties].hash
    end
  end
end

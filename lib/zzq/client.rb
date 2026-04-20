# frozen_string_literal: true

require_relative "socket"

module ZZQ
  # MQTT client. Owns exactly one MQTT session at a time (possibly
  # persisted across reconnects), subscriptions, and a QoS tracker for
  # inflight PUB flows.
  #
  # M3 stub: only the constructor and {#build_routing} stand. The
  # CONNECT/CONNACK handshake and publish/subscribe plumbing come in
  # M4 / M5.
  class Client < Socket
    def build_routing(engine)
      Routing::Client.new(engine)
    end


    # Most recent CONNACK received from the broker, or nil before the
    # first successful connect.
    def last_connack     = @engine.routing.last_connack
    def session_present? = @engine.routing.session_present?


    # Publish a message. For QoS 0 returns once the packet is
    # written. For QoS 1 blocks until PUBACK; for QoS 2 blocks until
    # PUBCOMP.
    def publish(topic, payload, qos: 0, retain: false, properties: {})
      raise NotInAsyncContext unless Async::Task.current?
      @engine.routing.publish(
        topic: topic, payload: coerce_binary(payload),
        qos: qos, retain: retain, properties: properties,
      )
    end


    # Subscribe to a topic filter. Returns a {ZZQ::Subscription} whose
    # queue receives matching messages until it is closed.
    def subscribe(filter, qos: 0, no_local: false, retain_as_published: false,
                  retain_handling: 0)
      raise NotInAsyncContext unless Async::Task.current?
      @engine.routing.subscribe(
        filter, qos: qos, no_local: no_local,
        retain_as_published: retain_as_published,
        retain_handling: retain_handling,
      )
    end
  end
end

require_relative "routing/client"

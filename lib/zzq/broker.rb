# frozen_string_literal: true

require_relative "socket"

module ZZQ
  # MQTT broker. Owns the session map (by client id), retained-message
  # store, topic trie, and shared-subscription groups.
  #
  # M3 stub: constructor + {#build_routing}. CONNECT handling,
  # subscription matching, and retained store come in M7.
  class Broker < Socket
    def build_routing(engine)
      Routing::Broker.new(engine)
    end


    # Number of currently-connected MQTT sessions.
    def session_count = @engine.routing.session_count


    # Inject a {ZZQ::Message} into the broker's delivery path as if it
    # had arrived via a PUBLISH packet. Fans out to every matching
    # subscriber. Use this to bridge non-MQTT inputs (NNQ, OMQ, a
    # file) into broker topics.
    def ingest(message)
      raise NotInAsyncContext unless Async::Task.current?
      @engine.routing.dispatch_message(message)
      nil
    end
    alias_method :<<, :ingest
  end
end

require_relative "routing/broker"

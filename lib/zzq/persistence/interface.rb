# frozen_string_literal: true

module ZZQ
  module Persistence
    # Abstract base for persistence backends.
    #
    # Concrete backends (Memory, PStore, future Redis/SQLite) implement
    # the methods below. Broker state calls through this interface; the
    # in-memory runtime state in {Routing::Broker} is the source of
    # truth during a session, and the backend is the durable record
    # that survives restarts.
    #
    # v1 scope: retained messages only. Session persistence will land
    # alongside session-expiry (M8's deferred piece).
    class Interface
      # Yields each persisted {ZZQ::Message} (retained). Called once at
      # broker start to warm the in-memory RetainedStore.
      def load_retained
        raise NotImplementedError
      end


      # Persist a retained message under +message.topic+. Replaces any
      # existing retained message for that topic.
      def save_retained(_message)
        raise NotImplementedError
      end


      # Remove the retained message for +topic+ (zero-length-payload
      # rule, MQTT-3.3.1-6).
      def delete_retained(_topic)
        raise NotImplementedError
      end


      # Wipe all retained state. Mostly for tests and operator tooling.
      def clear_retained
        raise NotImplementedError
      end
    end
  end
end

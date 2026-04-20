# frozen_string_literal: true

require_relative "topic_match"

module ZZQ
  module Routing
    # Exact-topic map of retained application messages.
    #
    # MQTT spec: the last PUBLISH with RETAIN=1 for a given topic is
    # stored. A zero-length payload + RETAIN=1 deletes the retained
    # message for that topic (MQTT-3.3.1-6).
    #
    # On SUBSCRIBE, the broker walks this store and delivers every
    # retained message whose topic matches the new filter.
    class RetainedStore
      # +persistence+ is any {ZZQ::Persistence::Interface} subclass.
      # Default is in-memory (no durability). On construction the
      # in-process cache is warmed from the backend's load_retained.
      def initialize(persistence: nil)
        @store       = {}  # topic => ZZQ::Message
        @persistence = persistence
        @persistence&.load_retained { |msg| @store[msg.topic] = msg }
      end


      # Upsert or delete per RETAIN semantics. Returns the stored
      # message (nil if the empty-payload rule deleted it).
      def store(message)
        if message.payload.empty?
          @store.delete(message.topic)
          @persistence&.delete_retained(message.topic)
          nil
        else
          @store[message.topic] = message
          @persistence&.save_retained(message)
          message
        end
      end


      def delete(topic)
        @persistence&.delete_retained(topic)
        @store.delete(topic)
      end


      # Enumerate retained messages matching +filter+.
      def each_matching(filter)
        return enum_for(:each_matching, filter) unless block_given?
        @store.each_value do |msg|
          yield msg if TopicMatch.match?(filter, msg.topic)
        end
      end


      def size = @store.size
      def empty? = @store.empty?


      # Test helper: wipe everything.
      def clear
        @persistence&.clear_retained
        @store.clear
      end
    end
  end
end

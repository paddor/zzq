# frozen_string_literal: true

require_relative "interface"

module ZZQ
  module Persistence
    # In-process persistence backend. Default for ephemeral brokers and
    # tests. State is lost on process exit.
    class Memory < Interface
      def initialize
        super()
        @retained = {}  # topic => ZZQ::Message
      end


      def load_retained
        return enum_for(:load_retained) unless block_given?
        @retained.each_value { |msg| yield msg }
      end


      def save_retained(message)
        @retained[message.topic] = message
      end


      def delete_retained(topic)
        @retained.delete(topic)
      end


      def clear_retained
        @retained.clear
      end
    end
  end
end

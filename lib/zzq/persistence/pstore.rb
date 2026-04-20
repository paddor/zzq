# frozen_string_literal: true

require "fileutils"
require "pstore"
require "protocol/mqtt"
require_relative "interface"
require_relative "../message"

module ZZQ
  module Persistence
    # PStore-backed durable persistence. Single-process deployments
    # only — PStore uses file locking but doesn't coordinate well
    # across processes.
    #
    # `ultra_safe` is enabled by default: each commit does a full fsync
    # of the data file AND its parent directory, surviving power loss
    # (not just clean crashes). Fits a broker where "I told the
    # publisher QoS 1 ack" must survive a kernel panic.
    #
    # Messages are serialized via {ZZQ::Message#to_wire}, so the
    # on-disk format is the PUBLISH wire encoding — self-describing
    # and version-tagged.
    class PStore < Interface
      DEFAULT_FILENAME = "retained.pstore"


      def initialize(data_dir:, filename: DEFAULT_FILENAME, ultra_safe: true)
        super()
        FileUtils.mkdir_p(data_dir)
        @store = ::PStore.new(File.join(data_dir, filename))
        @store.ultra_safe = ultra_safe
      end


      def load_retained
        return enum_for(:load_retained) unless block_given?
        @store.transaction(true) do
          (@store[:retained] || {}).each_value do |bytes|
            yield ZZQ::Message.from_wire(bytes)
          end
        end
      end


      def save_retained(message)
        bytes = message.to_wire
        @store.transaction do
          @store[:retained] ||= {}
          @store[:retained][message.topic] = bytes
        end
      end


      def delete_retained(topic)
        @store.transaction do
          (@store[:retained] || {}).delete(topic)
        end
      end


      def clear_retained
        @store.transaction { @store[:retained] = {} }
      end
    end
  end
end

# frozen_string_literal: true

require "async/barrier"
require "async/promise"

module ZZQ
  class Engine
    # Owns the socket-level state: `:new → :open → :closing → :closed`.
    #
    # Adapted from NNQ::Engine::SocketLifecycle, minus the shared-IO-
    # thread fallback (ZZQ is pure-Async).
    class SocketLifecycle
      class InvalidTransition < RuntimeError; end

      STATES = %i[new open closing closed].freeze

      TRANSITIONS = {
        new:     %i[open closed].freeze,
        open:    %i[closing closed].freeze,
        closing: %i[closed].freeze,
        closed:  [].freeze,
      }.freeze


      attr_reader :state
      attr_reader :parent_task
      attr_reader :peer_connected
      attr_reader :all_peers_gone
      attr_reader :barrier
      attr_accessor :reconnect_enabled


      def initialize
        @state             = :new
        @parent_task       = nil
        @peer_connected    = Async::Promise.new
        @all_peers_gone    = Async::Promise.new
        @reconnect_enabled = true
        @barrier           = nil
      end


      def open?    = @state == :open
      def closing? = @state == :closing
      def closed?  = @state == :closed
      def alive?   = @state == :new || @state == :open


      # Must be called from within an Async::Task. Raises
      # NotInAsyncContext otherwise.
      def capture_parent_task
        return false if @parent_task
        task = Async::Task.current? or raise NotInAsyncContext
        @parent_task = task
        @barrier = Async::Barrier.new(parent: @parent_task)
        transition!(:open)
        true
      end


      def start_closing!  = transition!(:closing)
      def finish_closing! = transition!(:closed)


      def resolve_all_peers_gone_if_empty(connections)
        return unless @peer_connected.resolved? && connections.empty?
        return if @all_peers_gone.resolved?
        @all_peers_gone.resolve(true)
      end


      private def transition!(new_state)
        allowed = TRANSITIONS[@state]
        raise InvalidTransition, "#{@state} → #{new_state}" unless allowed&.include?(new_state)
        @state = new_state
      end
    end
  end
end

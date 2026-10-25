# frozen_string_literal: true

module ZZQ
  # A received message. The topic is a frozen UTF-8 String and the payload
  # a frozen binary String.
  #
  # @!attribute [r] topic
  #   @return [String]
  # @!attribute [r] payload
  #   @return [String]
  # @!attribute [r] qos
  #   @return [Integer] delivery QoS, 0 to 2
  # @!attribute [r] retain
  #   @return [Boolean] a retained message, or the publisher's flag under
  #     Retain As Published
  # @!attribute [r] subscription_ids
  #   @return [Array<Integer>] v5 Subscription Identifiers of the matching
  #     subscriptions
  Message = Data.define(:topic, :payload, :qos, :retain, :subscription_ids) do
    alias_method :retain?, :retain
  end
end

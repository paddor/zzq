# frozen_string_literal: true

module ZZQ
  # Base class of zzq errors.
  class Error < StandardError; end

  # The client or broker is closed, or the connection is gone.
  class ClosedError < Error; end

  # The endpoint, the connection or TLS failed.
  class TransportError < Error; end

  # The peer broke the MQTT protocol.
  class ProtocolError < Error; end

  # A topic name or filter is not valid.
  class InvalidTopic < Error; end

  # A message or field is larger than MQTT can carry.
  class TooLarge < Error; end

  # The broker rejected a request, such as a subscription.
  class Rejected < Error
    # @return [Integer, nil] MQTT reason code
    attr_reader :reason_code

    # @param message [String, nil]
    # @param reason_code [Integer, nil]
    def initialize(message = nil, reason_code = nil)
      super(message)
      @reason_code = reason_code
    end
  end

  # The broker refused the connection.
  class ConnectionRefused < Rejected; end
end

# frozen_string_literal: true

module ZZQ
  # A single event emitted by a Socket's #monitor task. Event types
  # cover both infrastructure lifecycle (:listening, :connected,
  # :disconnected, :closed) and MQTT semantics (:session_resumed,
  # :session_expired, :retained_updated, :publish_delivered,
  # :will_published).
  MonitorEvent = Data.define(:type, :endpoint, :detail) do
    def initialize(type:, endpoint: nil, detail: nil)
      super
    end
  end
end

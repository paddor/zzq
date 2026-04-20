# frozen_string_literal: true

module ZZQ
  # Per-socket configuration. Holds both infrastructure options (linger,
  # HWMs, reconnect) lifted from NNQ::Options, and MQTT-specific ones
  # (keepalive, session_expiry, receive_maximum, etc.) that shape the
  # wire behaviour.
  #
  # Clients and brokers share this class; role-irrelevant fields are
  # simply ignored by the other side.
  class Options
    DEFAULT_HWM = 1000

    # --- infrastructure -------------------------------------------------
    attr_accessor :linger
    attr_accessor :read_timeout
    attr_accessor :write_timeout
    attr_accessor :reconnect_interval
    attr_accessor :send_hwm
    attr_accessor :recv_hwm

    # --- MQTT wire ------------------------------------------------------
    # 3 = MQTT v3.1.1, 5 = MQTT v5.0
    attr_accessor :version

    # Client identifier. Client-only; required unless empty-id + v5 is
    # used (server assigns it).
    attr_accessor :client_id

    # Clean Start (v5) / Clean Session (v3.1.1). When true, any prior
    # session state for this client id is discarded on CONNECT.
    attr_accessor :clean_start

    # Keep-alive interval in seconds. The broker will close the
    # connection after 1.5× this period with no traffic. 0 disables.
    attr_accessor :keep_alive

    # Session expiry interval (v5 only), in seconds. Only meaningful
    # when clean_start is false.
    attr_accessor :session_expiry_interval

    # Maximum outstanding unacknowledged QoS 1/2 messages (v5).
    attr_accessor :receive_maximum

    # Maximum packet size this peer is willing to accept (v5). nil =
    # unlimited.
    attr_accessor :max_packet_size

    # Topic Alias Maximum we advertise (v5).
    attr_accessor :topic_alias_maximum

    # MQTT username / password (both optional).
    attr_accessor :username
    attr_accessor :password

    # Last Will and Testament, client-side. Nil or a Hash of
    # { topic:, payload:, qos:, retain:, properties: }.
    attr_accessor :will

    # Broker-side persistence backend. Nil = ephemeral (no durability).
    # Pass an instance of a {ZZQ::Persistence::Interface} subclass
    # (e.g. {ZZQ::Persistence::PStore}) to survive process restarts.
    attr_accessor :persistence


    def initialize(
      linger: Float::INFINITY, send_hwm: DEFAULT_HWM, recv_hwm: DEFAULT_HWM,
      version: 5, client_id: nil, clean_start: true, keep_alive: 60,
      session_expiry_interval: 0, receive_maximum: 65_535,
      max_packet_size: nil, topic_alias_maximum: 0,
      username: nil, password: nil, will: nil,
      persistence: nil
    )
      @linger                  = linger
      @read_timeout            = nil
      @write_timeout           = nil
      @reconnect_interval      = 0.1
      @send_hwm                = send_hwm
      @recv_hwm                = recv_hwm
      @version                 = version
      @client_id               = client_id
      @clean_start             = clean_start
      @keep_alive              = keep_alive
      @session_expiry_interval = session_expiry_interval
      @receive_maximum         = receive_maximum
      @max_packet_size         = max_packet_size
      @topic_alias_maximum     = topic_alias_maximum
      @username                = username
      @password                = password
      @will                    = will
      @persistence             = persistence
    end
  end
end

# frozen_string_literal: true

module ZZQ
  class Error < StandardError; end

  # Raised when sending on or interacting with a closed socket.
  class ClosedError < Error; end

  # Raised when the routing layer refuses to accept a peer (e.g. a
  # duplicate client id collision on the broker).
  class ConnectionRejected < Error; end

  # Raised when a socket method is called outside an Async::Task. ZZQ
  # is fiber-first: wrap calls in `Async do ... end` or use the block
  # form of #connect / #bind.
  class NotInAsyncContext < Error
    def initialize(msg = "ZZQ socket methods must run inside an Async task. " \
                         "Wrap in `Async do ... end`, or use the block form " \
                         "of #connect/#bind.")
      super
    end
  end

  # Server responded with a non-success CONNACK.
  class ConnectionRefused < Error
    attr_reader :reason_code

    def initialize(reason_code, message = nil)
      @reason_code = reason_code
      super(message || "broker refused connect: reason=0x#{reason_code.to_s(16)}")
    end
  end

  # Authentication failed.
  class AuthFailed < Error; end

  # Network errors that indicate the connection is gone.
  CONNECTION_LOST = [
    Errno::ECONNRESET, Errno::EPIPE, Errno::ENOTCONN, Errno::ECONNABORTED,
    Errno::EBADF, EOFError, IOError
  ].freeze
end

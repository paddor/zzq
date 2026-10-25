# frozen_string_literal: true

require "minitest/autorun"
require "zzq"

module ZZQTestHelpers
  # A broker on a free TCP port, closed after the test.
  def broker(**options)
    closing(ZZQ::Broker.new(sys_interval: nil, **options))
  end

  def tcp_endpoint(on = broker)
    on.bind("tcp://127.0.0.1:0")
  end

  # A connected client, closed after the test.
  def client(endpoint, **options)
    closing(ZZQ::Client.connect(endpoint, **options))
  end

  def closing(object)
    (@closing ||= []) << object
    object
  end

  def teardown
    @closing&.reverse_each(&:close)
  end
end

class Minitest::Spec
  include ZZQTestHelpers
end

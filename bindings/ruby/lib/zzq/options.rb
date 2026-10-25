# frozen_string_literal: true

module ZZQ
  # @api private
  module Options
    module_function

    # +given+ with String keys, after checking every key is +allowed+.
    def check(given, allowed)
      unknown = given.keys - allowed
      unless unknown.empty?
        raise ArgumentError, "unknown option#{"s" if unknown.size > 1}: #{unknown.join(", ")}"
      end

      stringify(given)
    end

    def stringify(hash)
      hash.to_h { |key, value| [key.to_s, value.is_a?(Hash) ? stringify(value) : value] }
    end

    # +mqtt://+ and +mqtts://+ as the +tcp://+ and +tls://+ endpoints they
    # name. Other schemes pass through.
    def endpoint(uri)
      uri = uri.to_s
      if uri.start_with?("mqtt://")
        "tcp://#{uri.delete_prefix("mqtt://")}"
      elsif uri.start_with?("mqtts://")
        "tls://#{uri.delete_prefix("mqtts://")}"
      else
        uri
      end
    end
  end
end

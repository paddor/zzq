# frozen_string_literal: true

module ZZQ
  module Routing
    # Tiny topic-filter matcher (MQTT §4.7). `+` matches exactly one
    # topic level; `#` must be the last segment and matches zero or
    # more remaining levels. Shared-subscription prefixes
    # (`$share/<group>/<filter>`) are stripped before matching.
    #
    # Superseded by a proper trie in M7; this linear matcher is fine
    # for MVP broker traffic.
    module TopicMatch
      module_function


      # @param filter [String] topic filter with `+` / `#` wildcards.
      # @param topic  [String] concrete topic name.
      # @return [Boolean]
      def match?(filter, topic)
        filter = strip_share(filter)
        return false if topic.start_with?("$") && filter.start_with?("+", "#")

        f_parts = filter.split("/", -1)
        t_parts = topic.split("/", -1)

        i = 0
        while i < f_parts.length
          f = f_parts[i]
          if f == "#"
            # Must be last; matches zero or more remaining levels.
            return i == f_parts.length - 1
          end
          return false if i >= t_parts.length
          return false unless f == "+" || f == t_parts[i]
          i += 1
        end
        i == t_parts.length
      end


      def strip_share(filter)
        return filter unless filter.start_with?("$share/")
        # $share/<group>/<filter> — drop first two segments.
        parts = filter.split("/", 3)
        parts.length == 3 ? parts[2] : filter
      end
    end
  end
end

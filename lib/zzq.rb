# frozen_string_literal: true

# ZZQ — async-native MQTT client and broker for Ruby.
#
# Built on `async`, `io-stream`, and `protocol-mqtt`. Applies the
# omq/nnq architecture (Socket base, Engine, pluggable transports,
# barrier-scoped lifecycle) to MQTT 3.1.1 and 5.0.
module ZZQ
end

require_relative "zzq/version"
require_relative "zzq/error"
require_relative "zzq/monitor_event"
require_relative "zzq/options"
require_relative "zzq/message"
require_relative "zzq/subscription"
require_relative "zzq/engine"
require_relative "zzq/socket"
require_relative "zzq/transport/mqtt"
require_relative "zzq/transport/ipc"
require_relative "zzq/transport/mqtts"
require_relative "zzq/persistence/interface"
require_relative "zzq/persistence/memory"
require_relative "zzq/persistence/pstore"
require_relative "zzq/client"
require_relative "zzq/broker"

module Beefcake
  module ClientBackends
    class DefaultBackend
      attr_accessor :host, :port

      def initialize(host, port)
        @host, @port = [host, port]
      end

      def send_request(*args)
        # Do nothing, just pass right through.
      end
    end
  end
end

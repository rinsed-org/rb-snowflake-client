module RubySnowflake
  class Client
    class HttpConnectionWrapper
      def initialize(hostname, port)
        @hostname = hostname
        @port = port
      end

      def start
        @connection = Net::HTTP.start(@hostname, @port, use_ssl: true)
        self
      rescue StandardError
        raise ConnectionError.new "Error connecting to server."
      end

      def request(request)
        # connections can timeout and close, re-open them
        # which is what the connection pool expects
        start unless connection.active?

        begin
          connection.request(request)
        rescue StandardError => error
          raise RequestError.new "HTTP error requesting data", cause: error
        end
      end

      private
        attr_accessor :connection
    end
  end
end

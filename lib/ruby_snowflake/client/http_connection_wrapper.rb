# frozen_string_literal: true

require "net/http"

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
      rescue OpenSSL::SSL::SSLError => e
        raise e # let open ssl errors propagate up to get retried
      rescue StandardError
        raise ConnectionError.new "Error connecting to server."
      end

      def request(request)
        # connections can timeout and close, re-open them
        # which is what the connection pool expects
        start unless connection.active?

        begin
          connection.request(request)
        rescue OpenSSL::SSL::SSLError => e
          raise e # let open ssl errors propagate up to get retried
        rescue StandardError
          raise RequestError, "HTTP error requesting data"
        end
      end

      private
        attr_accessor :connection
    end
  end
end

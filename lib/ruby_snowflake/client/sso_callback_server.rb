# frozen_string_literal: true

require "socket"
require "uri"
require "timeout"

module RubySnowflake
  class Client
    class SsoCallbackServer
      SUCCESS_HTML = <<~HTML
        <html>
        <head><title>Authentication Successful</title></head>
        <body>
        <h1>Authentication Successful</h1>
        <p>You can close this window and return to your application.</p>
        </body>
        </html>
      HTML

      attr_reader :port

      def initialize(port: 0, timeout: Client::DEFAULT_SSO_TIMEOUT)
        @port = port
        @timeout = timeout
        @server = nil
      end

      def start
        @server = TCPServer.new("127.0.0.1", @port)
        @port = @server.addr[1]
        self
      end

      def wait_for_token
        Timeout.timeout(@timeout) do
          loop do
            client = @server.accept
            begin
              request_line = client.gets
              while (line = client.gets) && line != "\r\n"; end

              token = extract_token(request_line)
              if token
                client.print("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: #{SUCCESS_HTML.bytesize}\r\n\r\n#{SUCCESS_HTML}")
                return token
              else
                client.print("HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n")
              end
            ensure
              client.close rescue nil
            end
          end
        end
      ensure
        shutdown
      end

      def shutdown
        @server&.close rescue nil
      end

      private

      def extract_token(request_line)
        return nil unless request_line

        match = request_line.match(%r{GET /\?token=([^\s&]+)})
        return nil unless match

        URI.decode_www_form_component(match[1])
      end
    end
  end
end

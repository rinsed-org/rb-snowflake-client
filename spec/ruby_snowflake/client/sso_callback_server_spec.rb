# frozen_string_literal: true

require "spec_helper"

RSpec.describe RubySnowflake::Client::SsoCallbackServer do
  describe "#start and #shutdown" do
    it "binds to localhost on a random port" do
      server = described_class.new(port: 0, timeout: 5)
      server.start

      expect(server.port).to be > 0
      expect(server.port).to be < 65536

      server.shutdown
    end
  end

  describe "#wait_for_token" do
    it "extracts token from a valid request" do
      server = described_class.new(port: 0, timeout: 5)
      server.start
      port = server.port

      token_thread = Thread.new { server.wait_for_token }
      sleep 0.1

      socket = TCPSocket.new("127.0.0.1", port)
      socket.print("GET /?token=test_token_123 HTTP/1.1\r\nHost: localhost\r\n\r\n")
      response = socket.read
      socket.close

      token = token_thread.value
      expect(token).to eq("test_token_123")
      expect(response).to include("200 OK")
    end

    it "handles favicon request gracefully without breaking token wait" do
      server = described_class.new(port: 0, timeout: 5)
      server.start
      port = server.port

      token_thread = Thread.new { server.wait_for_token }
      sleep 0.1

      # Send favicon request first (should get 404, server keeps waiting)
      favicon_socket = TCPSocket.new("127.0.0.1", port)
      favicon_socket.print("GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\n\r\n")
      favicon_response = favicon_socket.read
      favicon_socket.close

      expect(favicon_response).to include("404")

      # Now send real token request
      token_socket = TCPSocket.new("127.0.0.1", port)
      token_socket.print("GET /?token=real_token HTTP/1.1\r\nHost: localhost\r\n\r\n")
      token_response = token_socket.read
      token_socket.close

      token = token_thread.value
      expect(token).to eq("real_token")
      expect(token_response).to include("200 OK")
    end

    it "handles multiple spurious requests before receiving valid token" do
      server = described_class.new(port: 0, timeout: 5)
      server.start
      port = server.port

      token_thread = Thread.new { server.wait_for_token }
      sleep 0.1

      # Send multiple spurious requests
      3.times do |i|
        socket = TCPSocket.new("127.0.0.1", port)
        socket.print("GET /spurious#{i} HTTP/1.1\r\nHost: localhost\r\n\r\n")
        response = socket.read
        socket.close
        expect(response).to include("404")
      end

      # Finally send the real token
      socket = TCPSocket.new("127.0.0.1", port)
      socket.print("GET /?token=final_token HTTP/1.1\r\nHost: localhost\r\n\r\n")
      socket.read
      socket.close

      token = token_thread.value
      expect(token).to eq("final_token")
    end

    it "times out appropriately when no token is received" do
      server = described_class.new(port: 0, timeout: 1)
      server.start

      start_time = Time.now
      expect { server.wait_for_token }.to raise_error(Timeout::Error)

      elapsed = Time.now - start_time
      expect(elapsed).to be_within(0.5).of(1.0)
    end
  end
end

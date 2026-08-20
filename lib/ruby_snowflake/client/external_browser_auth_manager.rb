# frozen_string_literal: true

require "net/http"
require "json"
require "concurrent"
require "openssl"
require_relative "auth_manager"
require_relative "sso_callback_server"
require_relative "browser_launcher"

module RubySnowflake
  class Client
    class ExternalBrowserAuthManager
      include AuthManager

      DEFAULT_TOKEN_TTL = 3540 # 59 minutes, same as JWT

      def initialize(uri, account, user, options = {})
        @base_uri = uri
        @account = account
        @user = user
        @sso_timeout = options.fetch(:sso_timeout, Client::DEFAULT_SSO_TIMEOUT)
        @sso_port = options.fetch(:sso_port, 0) # 0 = random available port
        @token_ttl = options.fetch(:token_ttl, DEFAULT_TOKEN_TTL)
        @logger = options.fetch(:logger, Logger.new($stdout))

        @session_token = nil
        @token_expires_at = Time.now.to_i - 1 # Force initial auth
        @token_semaphore = Concurrent::Semaphore.new(1)
      end

      def apply_auth(request)
        request["Authorization"] = "Snowflake Token=\"#{token}\""
      end

      def uses_v1_api?
        true
      end

      def token
        return @session_token unless token_expired?

        @token_semaphore.acquire do
          return @session_token unless token_expired?

          perform_sso_authentication
          @session_token
        end
      end

      private

      def token_expired?
        Time.now.to_i > @token_expires_at
      end

      def perform_sso_authentication
        callback_server = SsoCallbackServer.new(port: @sso_port, timeout: @sso_timeout)
        callback_server.start

        sso_response = request_sso_url(callback_server.port)
        data = sso_response["data"]
        unless data && data["ssoUrl"] && data["proofKey"]
          raise AuthenticationError.new("Invalid SSO response: missing required fields")
        end
        sso_url = data["ssoUrl"]
        proof_key = data["proofKey"]

        @logger.info("Opening browser for SSO authentication...")

        BrowserLauncher.open(sso_url)

        @logger.info("Waiting for authentication callback on port #{callback_server.port}...")
        saml_token = callback_server.wait_for_token

        raise AuthenticationError.new("No SAML token received from SSO callback") unless saml_token

        @session_token = authenticate_with_saml_token(saml_token, proof_key)
        @token_expires_at = Time.now.to_i + @token_ttl

        @logger.info("SSO authentication successful")
      rescue Timeout::Error
        raise SsoTimeoutError.new("SSO authentication timed out after #{@sso_timeout} seconds")
      ensure
        callback_server&.shutdown
      end

      def request_sso_url(callback_port)
        uri = URI.parse("#{@base_uri}/session/authenticator-request")
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true

        request = Net::HTTP::Post.new(uri)
        request["Content-Type"] = "application/json"
        request["Accept"] = "application/json"

        request.body = {
          "data" => {
            "ACCOUNT_NAME" => @account,
            "LOGIN_NAME" => @user,
            "AUTHENTICATOR" => "externalbrowser",
            "BROWSER_MODE_REDIRECT_PORT" => callback_port.to_s
          }
        }.to_json

        response = http.request(request)

        unless response.code == "200"
          raise AuthenticationError.new("Failed to get SSO URL: #{response.code} - #{response.body[0..200]}")
        end

        JSON.parse(response.body)
      end

      def authenticate_with_saml_token(saml_token, proof_key)
        uri = URI.parse("#{@base_uri}/session/v1/login-request")
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = true

        request = Net::HTTP::Post.new(uri)
        request["Content-Type"] = "application/json"
        request["Accept"] = "application/json"

        request.body = {
          "data" => {
            "ACCOUNT_NAME" => @account,
            "LOGIN_NAME" => @user,
            "AUTHENTICATOR" => "externalbrowser",
            "TOKEN" => saml_token,
            "PROOF_KEY" => proof_key
          }
        }.to_json

        response = http.request(request)

        unless response.code == "200"
          raise AuthenticationError.new("Failed to authenticate with SAML token: #{response.code} - #{response.body[0..200]}")
        end

        json = JSON.parse(response.body)

        unless json["success"]
          raise AuthenticationError.new("Authentication failed: #{json["message"]}")
        end

        json["data"]["token"]
      end
    end
  end
end

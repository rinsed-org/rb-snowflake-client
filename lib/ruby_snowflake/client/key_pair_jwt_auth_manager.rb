# frozen_string_literal: true

require "jwt"
require "openssl"
require "concurrent"

module RubySnowflake
  class Client
    class KeyPairJwtAuthManager
      # requires text of a PEM formatted RSA private key
      def initialize(organization, account, user, private_key, jwt_token_ttl)
        @organization = organization
        @account = account
        @user = user
        @private_key_pem = private_key
        @jwt_token_ttl = jwt_token_ttl

        # start with an expired value to force creation
        @token_expires_at = Time.now.to_i - 1
        @token_semaphore = Concurrent::Semaphore.new(1)
      end

      def jwt_token
        return @token unless jwt_token_expired?

        @token_semaphore.acquire do
          now = Time.now.to_i
          @token_expires_at = now + @jwt_token_ttl

          private_key = OpenSSL::PKey.read(@private_key_pem)

          payload = {
            :iss => "#{@account.upcase}.#{@user.upcase}.#{public_key_fingerprint}",
            :sub => "#{@account.upcase}.#{@user.upcase}",
            :iat => now,
            :exp => @token_expires_at
          }

          @token = JWT.encode payload, private_key, "RS256"
        end
      end

      private
        def jwt_token_expired?
          Time.now.to_i > @token_expires_at
        end

        def public_key_fingerprint
          return @public_key_fingerprint unless @public_key_fingerprint.nil?

          public_key_der = OpenSSL::PKey::RSA.new(@private_key_pem).public_key.to_der
          digest = OpenSSL::Digest::SHA256.new.digest(public_key_der)
          fingerprint = Base64.strict_encode64(digest)

          @public_key_fingerprint = "SHA256:#{fingerprint}"
        end
    end
  end
end

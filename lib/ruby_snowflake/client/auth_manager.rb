# frozen_string_literal: true

module RubySnowflake
  class Client
    module AuthManager
      def apply_auth(request)
        raise NotImplementedError, "Subclasses must implement #apply_auth"
      end

      def token
        raise NotImplementedError, "Subclasses must implement #token"
      end
    end
  end
end

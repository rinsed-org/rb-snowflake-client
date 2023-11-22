# frozen_string_literal: true

module RubySnowflake
  class IndifferentCaseInsensitiveHash < Hash
    def [](key)
      super normalize_key(key)
    end

    def []=(key, value)
      super normalize_key(key), value
    end

    private
      def normalize_key(key)
        if key.is_a?(Symbol)
          key.to_s.downcase
        elsif key.respond_to?(:downcase)
          key.downcase
        else
          key
        end
      end
  end
end

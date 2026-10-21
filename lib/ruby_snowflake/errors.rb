# frozen_string_literal: true

module RubySnowflake
  class Error < StandardError
    def initialize(details)
      @details = details
    end

    def message
      @details.to_s
    end
  end

  class BadResponseError < Error ; end
  class ConnectionError < Error ; end
  class ConnectionStarvedError < Error ; end
  class MissingConfig < Error ; end
  class RetryableBadResponseError < Error ; end
  class RequestError < Error ; end
  class QueryTimeoutError < Error ;  end
end

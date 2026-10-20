# frozen_string_literal: true

module RubySnowflake
  module PartitionFuture
    # Waits for a partition fetch. RubySnowflake errors pass through; others (usually a pool
    # timeout) are wrapped in ConnectionStarvedError.
    def self.value!(future)
      future.value!
    rescue RubySnowflake::Error
      raise
    rescue StandardError => error
      raise ConnectionStarvedError.new(
              "A partition request timed out. This is usually due to using the client in " \
              "multiple threads. The client uses a connection thread pool and if too many " \
              "requests are all done in threads at the same time, threads can get starved " \
              "of access to connections. The solution for this is to either increase the " \
              "max_connections parameter on the client or create a new client instance " \
              "with its own connection pool to snowflake per thread. Rejection reason: #{error.message}"
            )
    end
  end
end

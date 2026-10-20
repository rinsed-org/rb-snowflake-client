# frozen_string_literal: true

module RubySnowflake
  module PartitionFuture
    # Waits for a partition fetch. RubySnowflake errors pass through, pool timeouts become
    # ConnectionStarvedError and anything else a RubySnowflake::Error, with the original as cause.
    # Only the fetch's own error is mapped, never an interrupt raised in the waiting thread.
    def self.value!(future)
      future.wait
      return future.value unless future.rejected?

      reason = future.reason
      case reason
      when RubySnowflake::Error
        raise reason
      when ConnectionPool::TimeoutError
        raise ConnectionStarvedError.new(
                "A partition request timed out. This is usually due to using the client in " \
                "multiple threads. The client uses a connection thread pool and if too many " \
                "requests are all done in threads at the same time, threads can get starved " \
                "of access to connections. The solution for this is to either increase the " \
                "max_connections parameter on the client or create a new client instance " \
                "with its own connection pool to snowflake per thread. Rejection reason: #{reason.message}"
              ), cause: reason
      else
        raise Error.new("Fetching a result partition failed: #{reason.class}: #{reason.message}"), cause: reason
      end
    end
  end
  private_constant :PartitionFuture
end

# frozen_string_literal: true

module RubySnowflake
  class Client
    class ThreadedInMemoryStrategy
      def self.result(statement_json_body, retreive_proc, num_threads)
        partitions = statement_json_body["resultSetMetaData"]["partitionInfo"]
        result = Result.new(partitions.size, statement_json_body["resultSetMetaData"]["rowType"])
        result[0] = statement_json_body["data"]

        thread_pool = Concurrent::FixedThreadPool.new(num_threads)
        partitions
          .each_with_index.map do |partition, index|
            next if index == 0 # already have the first partition
            [index, Concurrent::Future.execute(executor: thread_pool) { retreive_proc.call(index) }]
          end
          .each do |entry|
            next if entry.nil? # 0th index

            index, future = entry
            if future.rejected?
              if future.reason.is_a? RubySnowflake::Error
                raise future.reason
              else
                raise ConnectionStarvedError.new(
                        "A partition request timed out. This is usually do to using the client in" \
                        "multiple threads. The client uses a connection thread pool and if too many" \
                        "requests are all done in threads at the same time, threads can get starved" \
                        "of access to connections. The solution for this is to either increase the " \
                        "max_connections parameter on the client or create a new client instance" \
                        "with it's own connection pool to snowflake per thread. Rejection reason: #{future.reason.message}"
                      )
              end
            end
            result[index] = future.value
          end
        result
      end
    end
  end
end

# frozen_string_literal: true

module RubySnowflake
  class Client
    class ThreadedInMemoryStrategy
      def self.result(statement_json_body, retreive_proc, num_threads)
        partitions = statement_json_body["resultSetMetaData"]["partitionInfo"]
        result = Result.new(partitions.size, statement_json_body["resultSetMetaData"]["rowType"])
        result[0] = statement_json_body["data"]

        thread_pool = Concurrent::FixedThreadPool.new(num_threads)
        futures = []
        begin
          partitions.each_index do |index|
            next if index == 0 # already have the first partition
            futures << [index, Concurrent::Future.execute(executor: thread_pool) { retreive_proc.call(index) }]
          end

          futures.each do |index, future|
            result[index] = PartitionFuture.value!(future)
          end
        ensure
          # On a failure or interrupt, skip the fetches that haven't started
          futures.each { |_index, future| future.cancel }
          thread_pool.shutdown # no wait: in-flight fetches finish and return their connections
        end
        result
      end
    end
  end
end

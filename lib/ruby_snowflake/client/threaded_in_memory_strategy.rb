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
            result[index] = PartitionFuture.value!(future)
          end
        result
      end
    end
  end
end

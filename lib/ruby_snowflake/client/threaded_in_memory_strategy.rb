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
        partitions.each_with_index do |partition, index|
          next if index == 0 # already have the first partition
          futures << Concurrent::Future.execute(executor: thread_pool) do
            [index, retreive_proc.call(index)]
          end
        end
        futures.each do |future|
          # TODO: futures can get rejected, handle this error case
          index, partition_data = future.value
          result[index] = partition_data
        end
        result
      end
    end
  end
end

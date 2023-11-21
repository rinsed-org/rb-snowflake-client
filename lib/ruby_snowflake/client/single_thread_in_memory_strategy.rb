# frozen_string_literal: true

module RubySnowflake
  class Client
    class SingleThreadInMemoryStrategy
      def self.result(statement_json_body, retreive_proc)
        partitions = statement_json_body["resultSetMetaData"]["partitionInfo"]
        result = Result.new(partitions.size, statement_json_body["resultSetMetaData"]["rowType"])
        result[0] = statement_json_body["data"]

        partitions.each_with_index do |partition, index|
          next if index == 0 # already have the first partition
          result[index] = retreive_proc.call(index)
        end

        result
      end
    end
  end
end

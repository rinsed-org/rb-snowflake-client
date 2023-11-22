# frozen_string_literal: true

module RubySnowflake
  class Client
    class StreamingResultStrategy
      def self.result(statement_json_body, retreive_proc)
        partitions = statement_json_body["resultSetMetaData"]["partitionInfo"]

        result = StreamingResult.new(
          partitions.size,
          statement_json_body["resultSetMetaData"]["rowType"],
          retreive_proc
        )
        result[0] = statement_json_body["data"]

        result
      end
    end
  end
end

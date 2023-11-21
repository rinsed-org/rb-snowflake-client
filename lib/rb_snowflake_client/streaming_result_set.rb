# frozen_string_literal: true

require "concurrent"

require_relative "result_set"

class StreamingResultSet < ResultSet
  def initialize(partition_count, row_type_data, retreive_proc)
    super(partition_count, row_type_data)
    @retreive_proc = retreive_proc
  end

  def each
    return to_enum(:each) unless block_given?

    thread_pool = Concurrent::FixedThreadPool.new 1

    data.each_with_index do |_partition, index|
      next_index = [index+1, data.size-1].min
      if data[next_index].nil? # prefetch
        data[next_index] = Concurrent::Future.execute(executor: thread_pool) do
          @retreive_proc.call(next_index)
        end
      end

      if data[index].is_a? Concurrent::Future
        data[index] = data[index].value # wait for it to finish
      end
      data[index].each do |row|
        yield wrap_row(row)
      end
    end
  end


  def size
    not_implemented
  end

  def last
    not_implemented
  end

  private
    def not_implemented
      raise "not implemented on streaming result set"
    end
end

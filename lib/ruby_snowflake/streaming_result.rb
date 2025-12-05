# frozen_string_literal: true

require "concurrent"

require_relative "result"

module RubySnowflake
  class StreamingResult < Result
    def initialize(partition_count, row_type_data, retreive_proc, prefetch_threads: 1)
      super(partition_count, row_type_data)
      @retreive_proc = retreive_proc
      @prefetch_threads = prefetch_threads
    end

    def each
      return to_enum(:each) unless block_given?

      thread_pool = Concurrent::FixedThreadPool.new(@prefetch_threads)

      data.each_with_index do |_partition, index|
        # Prefetch the next N partitions (where N = prefetch_threads)
        # This allows parallel fetching while maintaining memory efficiency
        @prefetch_threads.times do |offset|
          next_index = index + offset + 1
          break if next_index >= data.size

          if data[next_index].nil? # not yet fetched or prefetched
            data[next_index] = Concurrent::Future.execute(executor: thread_pool) do
              @retreive_proc.call(next_index)
            end
          end
        end

        if data[index].is_a? Concurrent::Future
          data[index] = data[index].value # wait for it to finish
        end

        data[index].each do |row|
          yield wrap_row(row)
        end

        # After iterating over the current partition, clear the data to release memory
        data[index].clear

        # Reassign to a symbol so:
        # - When looking at the list of partitions in `data` it is easier to detect
        # - Will raise an exception if `data.each` is attempted to be called again
        # - It won't trigger prefetch detection as `next_index`
        data[index] = :finished
      end

      # Ensure thread pool is properly shut down
      thread_pool.shutdown
      thread_pool.wait_for_termination
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
end

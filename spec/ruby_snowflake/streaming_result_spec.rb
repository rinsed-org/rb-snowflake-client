# frozen_string_literal: true

require 'spec_helper'

RSpec.describe RubySnowflake::StreamingResult do
  let(:partition_count) { 5 }
  let(:row_type_data) { [{ "name" => "id", "type" => "fixed" }, { "name" => "value", "type" => "text" }] }
  let(:partitions_data) do
    [
      [[1, "first"], [2, "second"]],
      [[3, "third"], [4, "fourth"]],
      [[5, "fifth"], [6, "sixth"]],
      [[7, "seventh"], [8, "eighth"]],
      [[9, "ninth"], [10, "tenth"]]
    ]
  end
  let(:retrieve_proc) { ->(index) { partitions_data[index] } }

  describe '#initialize' do
    context 'with default prefetch_threads' do
      subject { described_class.new(partition_count, row_type_data, retrieve_proc) }

      it 'initializes with 1 prefetch thread by default' do
        expect(subject.instance_variable_get(:@prefetch_threads)).to eq(1)
      end
    end

    context 'with custom prefetch_threads' do
      subject { described_class.new(partition_count, row_type_data, retrieve_proc, prefetch_threads: 4) }

      it 'initializes with specified prefetch threads' do
        expect(subject.instance_variable_get(:@prefetch_threads)).to eq(4)
      end
    end

    context 'with invalid prefetch_threads' do
      it 'raises ArgumentError for zero' do
        expect { described_class.new(partition_count, row_type_data, retrieve_proc, prefetch_threads: 0) }
          .to raise_error(ArgumentError, /prefetch_threads must be a positive integer/)
      end

      it 'raises ArgumentError for negative values' do
        expect { described_class.new(partition_count, row_type_data, retrieve_proc, prefetch_threads: -1) }
          .to raise_error(ArgumentError, /prefetch_threads must be a positive integer/)
      end
    end
  end

  describe '#each' do
    subject { described_class.new(partition_count, row_type_data, retrieve_proc, prefetch_threads: prefetch_threads) }

    before do
      # Populate first partition (as done in StreamingResultStrategy)
      subject[0] = partitions_data[0]
    end

    context 'with single thread (backward compatible behavior)' do
      let(:prefetch_threads) { 1 }

      it 'iterates through all rows correctly' do
        rows = []
        subject.each { |row| rows << [row["id"], row["value"]] }

        expect(rows).to eq([
          [1, "first"], [2, "second"],
          [3, "third"], [4, "fourth"],
          [5, "fifth"], [6, "sixth"],
          [7, "seventh"], [8, "eighth"],
          [9, "ninth"], [10, "tenth"]
        ])
      end

      it 'clears processed partitions to save memory' do
        rows = []
        subject.each { |row| rows << row }

        # Check that partitions were cleared (marked as :finished)
        expect(subject.instance_variable_get(:@data)[0]).to eq(:finished)
        expect(subject.instance_variable_get(:@data)[1]).to eq(:finished)
      end

      it 'calls retrieve_proc for each partition' do
        call_count = 0
        instrumented_proc = lambda do |index|
          call_count += 1
          partitions_data[index]
        end

        result = described_class.new(partition_count, row_type_data, instrumented_proc, prefetch_threads: 1)
        result[0] = partitions_data[0]

        result.each { |row| row }

        # Should call for partitions 1-4 (partition 0 was pre-populated)
        expect(call_count).to eq(4)
      end
    end

    context 'with multiple threads' do
      let(:prefetch_threads) { 3 }

      it 'iterates through all rows correctly' do
        rows = []
        subject.each { |row| rows << [row["id"], row["value"]] }

        expect(rows).to eq([
          [1, "first"], [2, "second"],
          [3, "third"], [4, "fourth"],
          [5, "fifth"], [6, "sixth"],
          [7, "seventh"], [8, "eighth"],
          [9, "ninth"], [10, "tenth"]
        ])
      end

      it 'prefetches multiple partitions in parallel' do
        # Track concurrent fetches
        concurrent_fetches = []
        mutex = Mutex.new
        instrumented_proc = lambda do |index|
          mutex.synchronize { concurrent_fetches << index }
          sleep 0.01 # Simulate network latency
          partitions_data[index]
        end

        result = described_class.new(partition_count, row_type_data, instrumented_proc, prefetch_threads: 3)
        result[0] = partitions_data[0]

        result.each { |row| row }

        # With 3 threads, should prefetch indices 1, 2, 3 before processing them
        expect(concurrent_fetches).to include(1, 2, 3)
      end

      it 'properly shuts down thread pool' do
        thread_pool = nil
        allow(Concurrent::FixedThreadPool).to receive(:new).and_wrap_original do |method, *args|
          thread_pool = method.call(*args)
          thread_pool
        end

        subject.each { |row| row }

        expect(thread_pool).to be_shutdown
      end
    end

    context 'with more threads than partitions' do
      let(:prefetch_threads) { 10 }

      it 'handles gracefully without errors' do
        rows = []
        expect { subject.each { |row| rows << row } }.not_to raise_error

        expect(rows.length).to eq(10)
      end
    end

    context 'when returning an enumerator' do
      let(:prefetch_threads) { 2 }

      it 'returns an enumerator when no block given' do
        enumerator = subject.each

        expect(enumerator).to be_a(Enumerator)
        expect(enumerator.to_a.length).to eq(10)
      end
    end

    context 'when an exception occurs during iteration' do
      let(:prefetch_threads) { 3 }

      it 'properly shuts down thread pool even on exception' do
        thread_pool = nil
        allow(Concurrent::FixedThreadPool).to receive(:new).and_wrap_original do |method, *args|
          thread_pool = method.call(*args)
          thread_pool
        end

        # Raise exception after processing 2 rows
        count = 0
        expect do
          subject.each do |row|
            count += 1
            raise StandardError, "Test error" if count == 2
          end
        end.to raise_error(StandardError, "Test error")

        # Thread pool should still be shut down
        expect(thread_pool).to be_shutdown
      end
    end
  end
end

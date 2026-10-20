# frozen_string_literal: true

require "rb_snowflake_client"

RSpec.describe RubySnowflake::StreamingResult do
  let(:row_types) do
    [{"name" => "value", "type" => "text", "scale" => 0, "precision" => 0}]
  end
  let(:thread_pool) { Concurrent::FixedThreadPool.new(1) }

  before do
    allow(Concurrent::FixedThreadPool).to receive(:new).with(1).and_return(thread_pool)
    allow(thread_pool).to receive(:shutdown).and_call_original
  end

  def build_result(&retrieve_proc)
    result = described_class.new(2, row_types, retrieve_proc)
    result[0] = [["first"]]
    result
  end

  it "shuts down its prefetch pool after iteration" do
    values = build_result { [["second"]] }.map { |row| row["value"] }

    expect(values).to eq(%w[first second])
    expect(thread_pool).to have_received(:shutdown)
  end

  it "doesn't wait for an in-flight prefetch when the consumer stops early" do
    prefetch_started = Queue.new
    release_prefetch = Queue.new
    result = build_result do
      prefetch_started << true
      release_prefetch.pop
      [["second"]]
    end

    consumer = Thread.new do
      result.each do
        prefetch_started.pop # the prefetch is now blocked on release_prefetch
        break
      end
    end

    begin
      expect(consumer.join(5)).not_to be_nil
      expect(thread_pool).to be_shuttingdown
    ensure
      release_prefetch << true
    end

    expect(thread_pool.wait_for_termination(5)).to be(true)
  end

  it "shuts down its prefetch pool when the consumer raises" do
    expect do
      build_result { [["second"]] }.each { raise "consumer failed" }
    end.to raise_error("consumer failed")

    expect(thread_pool).to have_received(:shutdown)
  end

  it "propagates prefetch failures and shuts down the pool" do
    expect do
      build_result { raise "partition failed" }.to_a
    end.to raise_error("partition failed")

    expect(thread_pool).to have_received(:shutdown)
  end
end

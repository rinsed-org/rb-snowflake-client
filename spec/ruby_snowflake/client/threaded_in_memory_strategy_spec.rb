# frozen_string_literal: true

require "rb_snowflake_client"

RSpec.describe RubySnowflake::Client::ThreadedInMemoryStrategy do
  let(:statement_json_body) do
    {
      "resultSetMetaData" => {
        "partitionInfo" => [{}, {}, {}],
        "rowType" => [{"name" => "value", "type" => "text", "scale" => 0, "precision" => 0}]
      },
      "data" => [["first"]]
    }
  end

  def result(&retrieve_proc)
    described_class.result(statement_json_body, retrieve_proc, 2)
  end

  it "shuts down its thread pool" do
    thread_pool = Concurrent::FixedThreadPool.new(2)
    allow(Concurrent::FixedThreadPool).to receive(:new).with(2).and_return(thread_pool)

    result { |index| [["partition #{index}"]] }

    expect(thread_pool.shuttingdown? || thread_pool.shutdown?).to be(true)
  end

  it "cancels the fetches that haven't started when it's interrupted" do
    thread_pool = Concurrent::FixedThreadPool.new(1)
    allow(Concurrent::FixedThreadPool).to receive(:new).with(1).and_return(thread_pool)
    statement_json_body["resultSetMetaData"]["partitionInfo"] = [{}, {}, {}, {}]
    fetched = Queue.new
    release_fetch = Queue.new

    consumer = Thread.new do
      described_class.result(statement_json_body, ->(index) { fetched << index; release_fetch.pop }, 1)
    end
    consumer.report_on_exception = false

    expect(fetched.pop).to eq(1) # the only worker is now blocked in partition 1's fetch
    Thread.pass until consumer.status == "sleep" # every fetch is queued and the consumer is waiting
    consumer.raise("interrupted")
    expect { consumer.join }.to raise_error("interrupted")
    3.times { release_fetch << [["partition"]] } # enough for every fetch, should any run

    expect(thread_pool.wait_for_termination(5)).to be(true)
    expect(fetched.size).to eq(0)
  end

  it "raises a ConnectionStarvedError when a partition fetch fails after the strategy starts waiting for it" do
    expect do
      result do |index|
        next [["partition #{index}"]] if index == 1

        sleep 0.2 # still running when the strategy reaches this partition
        raise ConnectionPool::TimeoutError, "no connection"
      end
    end.to raise_error(RubySnowflake::ConnectionStarvedError, /Rejection reason: no connection/) { |error|
      expect(error.cause).to be_a(ConnectionPool::TimeoutError)
    }
  end

  it "wraps other errors in a plain RubySnowflake::Error" do
    expect do
      result { raise JSON::ParserError, "unexpected token" }
    end.to raise_error(RubySnowflake::Error, /JSON::ParserError: unexpected token/) { |error|
      expect(error).not_to be_a(RubySnowflake::ConnectionStarvedError)
      expect(error.cause).to be_a(JSON::ParserError)
    }
  end

  it "raises RubySnowflake errors as is" do
    failure = RubySnowflake::BadResponseError.new("Bad response!")

    expect { result { raise failure } }.to raise_error(failure)
  end
end

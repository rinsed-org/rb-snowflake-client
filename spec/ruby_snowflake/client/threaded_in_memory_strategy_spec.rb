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

  it "raises RubySnowflake errors as is" do
    failure = RubySnowflake::BadResponseError.new("Bad response!")

    expect { result { raise failure } }.to raise_error(failure)
  end
end

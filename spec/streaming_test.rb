require "benchmark"
require "rb_snowflake_client"
require 'dotenv/load'

def new_client
  client = RubySnowflake::Client.from_env # set SNOWFLAKE_* in your .env file
  client.logger.level = Logger::DEBUG
  client
end

size = 1_000
11.times do
  count = 0
  data = nil
  bm =
  Benchmark.measure do
    data = new_client.query(
      "SELECT * FROM RUBY_SNOWFLAKE_CLIENT_TESTING.PUBLIC.LARGE_TABLE limit #{size};",
      streaming: true
    )

    data.each {|row| row; count += 1 } # access each row, causing type conversion to happen
  end

  # you can now data.first or data.each and get rows that act like hashes
  # Row does the parsing at access time right now
  # data.first.tap do |row|
  #   puts row
  #   puts "#{row[:id]}, #{row[:code]}, #{row[:payload]}, #{row[:updated_at]}"
  # end

  puts "Querying with #{size}; took #{bm.real} actual size #{count}"
  puts
  puts
  size = size * 2
end

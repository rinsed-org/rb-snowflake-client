require "benchmark"
require "logger"
require "rb_snowflake_client"

def new_client
  client = RubySnowflake::Client.new(
    "https://oza47907.us-east-1.snowflakecomputing.com",
    ENV["SNOWFLAKE_PRIVATE_KEY"], # set this in your .env file
    "GBLARLO",
    "OZA47907",
    "SNOWFLAKE_CLIENT_TEST",
    "WEB_TEST_WH",
    ""
  )
  client.logger.level = Logger::DEBUG
  client
end

size = 1_000
11.times do
  data = nil
  type_conversion_time = 0
  bm =
    Benchmark.measure do
    data = new_client.query <<-SQL
  SELECT * FROM FIVETRAN_DATABASE.RINSED_WEB_PRODUCTION_MAMMOTH.EVENTS limit #{size};
  SQL

    # access each column on each row, causing type conversion to happen
    keys = data.columns
    data.each do |row|
      type_conversion_time += Benchmark.measure do
        keys.each { |key| row[key] }
      end.utime
    end
  end

  # you can now data.first or data.each and get rows that act like hashes
  # Row does the parsing at access time right now
  # data.first.tap do |row|
  #   puts row
  #   puts "#{row[:id]}, #{row[:code]}, #{row[:payload]}, #{row[:updated_at]}"
  # end

  puts "Querying with #{size}; took #{bm.utime} actual size #{data.size} type conversion: #{type_conversion_time}"
  puts
  puts
  size = size * 2
end

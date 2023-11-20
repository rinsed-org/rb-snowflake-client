require "benchmark"
require_relative "snowflake_client"


client = SnowflakeClient.new("https://oza47907.us-east-1.snowflakecomputing.com",
                             "private_key.pem",
                             "GBLARLO",
                             "OZA47907",
                             "SNOWFLAKE_CLIENT_TEST",
                             "SHA256:pbfmeTQ2+MestU2J9dXjGXTjtvZprYfHxzZzqqcIhFc=")

size = 1_000
10.times do
  bm =
  Benchmark.measure do
      data = client.query <<-SQL
  SELECT * FROM FIVETRAN_DATABASE.RINSED_WEB_PRODUCTION_MAMMOTH.EVENTS limit #{size};
  SQL
  end
  puts "Querying with #{size}; took #{bm.real}"
  puts
  puts
  size = size * 2
end

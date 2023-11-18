require_relative "client"

client = SnowflakeClient.new("https://oza47907.us-east-1.snowflakecomputing.com",
                             "private_key.pem",
                             "GBLARLO",
                             "OZA47907",
                             "SNOWFLAKE_CLIENT_TEST",
                             "SHA256:pbfmeTQ2+MestU2J9dXjGXTjtvZprYfHxzZzqqcIhFc=")

data = client.query <<-SQL
  SELECT * FROM FIVETRAN_DATABASE.RINSED_WEB_PRODUCTION_MAMMOTH.EVENTS limit 12000;
SQL

puts data.size

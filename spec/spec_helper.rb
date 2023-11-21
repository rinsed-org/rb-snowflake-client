require "rb_snowflake_client"
require "rspec"
require "pry"
require "dotenv/load"

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run :focus
  config.order = "random"
  config.mock_with( :rspec ) do |mock|
    mock.syntax = :expect
  end
end


# frozen_string_literal: true

require_relative "lib/ruby_snowflake/version"

Gem::Specification.new do |s|
  s.name    = "rb_snowflake_client"
  s.version = RubySnowflake::VERSION
  s.summary = "Snowflake connector for Ruby"
  s.author  = "Rinsed"
  s.email   = ["reid@rinsed.co", "alex@rinsed.co"]
  s.description = <<~DESC
  Using the HTTP V2 Api for Snowflake runs queries & creates native Ruby objects.
  DESC
  s.homepage = "https://github.com/rinsed-org/rb-snowflake-client"
  s.license = "MIT"

  s.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features|vendor)/}) }
  end

  s.require_paths = ["lib"]
  s.add_dependency "concurrent-ruby", ">= 1.0"
  s.add_dependency "connection_pool", ">= 2.3"
  s.add_dependency "dotenv", ">= 2.8"
  s.add_dependency "jwt", ">= 1.5"
  s.add_dependency "oj", ">= 3.16"
  s.add_dependency "retryable", ">= 3.0"
end

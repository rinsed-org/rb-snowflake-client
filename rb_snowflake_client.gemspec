# frozen_string_literal: true

require_relative "lib/rb_snowflake_client/version"

Gem::Specification.new do |s|
  s.name    = "rb_snowflake_client"
  s.version = RbSnowflakeClient::VERSION
  s.summary = "Snowflake connector for Ruby"
  s.author  = "Rinsed"
  s.email   = ["reid@rinsed.co", "alex@rinsed.co"]
  s.description = <<~DESC
  Using the HTTP V2 Api for Snowflake runs queries & creates native Ruby objects.
  DESC
  s.license = "MIT" # TODO: double check

  s.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features|vendor)/}) }
  end

  s.require_paths = ["lib"]
end

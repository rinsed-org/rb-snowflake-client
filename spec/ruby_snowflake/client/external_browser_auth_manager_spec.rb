# frozen_string_literal: true

require "spec_helper"

RSpec.describe RubySnowflake::Client::ExternalBrowserAuthManager do
  let(:base_uri) { "https://test.snowflakecomputing.com" }
  let(:account_identifier) { "TEST_ACCOUNT" }
  let(:user) { "test_user" }
  let(:sso_timeout) { 5 }

  subject { described_class.new(base_uri, account_identifier, user, sso_timeout: sso_timeout) }

  describe "#uses_v1_api?" do
    it "returns true" do
      expect(subject.uses_v1_api?).to eq(true)
    end
  end

  describe "#apply_auth" do
    it "uses correct Snowflake Token authorization header format" do
      # Mock the token without triggering browser auth
      subject.instance_variable_set(:@session_token, "mock_token_123")
      subject.instance_variable_set(:@token_expires_at, Time.now.to_i + 3600)

      request = Net::HTTP::Get.new(URI("https://test.com"))
      subject.apply_auth(request)

      auth_header = request["Authorization"]
      expect(auth_header).to eq('Snowflake Token="mock_token_123"')
    end
  end
end

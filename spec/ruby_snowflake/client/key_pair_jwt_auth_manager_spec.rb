require "spec_helper"

RSpec.describe RubySnowflake::Client::KeyPairJwtAuthManager do
  let(:organization) { nil }
  let(:account) { "account" }
  let(:user) { "user" }
  let(:private_key) { OpenSSL::PKey::RSA.new(2048).to_pem }
  let(:jwt_token_ttl) { 3600 }
  
  subject { described_class.new(organization, account, user, private_key, jwt_token_ttl) }

  describe "#jwt_token" do
    context "when creating a JWT token" do
      it "generates a valid token" do
        expect(subject.jwt_token).to be_a(String)
      end
      
      it "generates a token with the correct claims" do
        # Use the JWT gem to decode the token
        token = subject.jwt_token
        decoded_token = JWT.decode(token, OpenSSL::PKey::RSA.new(private_key).public_key, true, { algorithm: 'RS256' })[0]
        
        expect(decoded_token["iss"]).to include(account.upcase)
        expect(decoded_token["iss"]).to include(user.upcase)
        expect(decoded_token["sub"]).to eq("#{account.upcase}.#{user.upcase}")
        expect(decoded_token["iat"]).to be_a(Integer)
        expect(decoded_token["exp"]).to be_a(Integer)
      end
      
      it "creates token with proper expiration time" do
        now = Time.now.to_i
        token = subject.jwt_token
        decoded_token = JWT.decode(token, OpenSSL::PKey::RSA.new(private_key).public_key, true, { algorithm: 'RS256' })[0]
        
        # Expect the token to expire in approximately jwt_token_ttl seconds
        expect(decoded_token["exp"] - now).to be_within(5).of(jwt_token_ttl)
      end
    end
  end
  
  describe "account_name handling" do
    context "when organization is nil" do
      let(:organization) { nil }
      
      it "uses only the account in the token" do
        token = subject.jwt_token
        decoded_token = JWT.decode(token, OpenSSL::PKey::RSA.new(private_key).public_key, true, { algorithm: 'RS256' })[0]
        
        expect(decoded_token["iss"]).to start_with("#{account.upcase}.")
        expect(decoded_token["sub"]).to eq("#{account.upcase}.#{user.upcase}")
      end
    end
    
    context "when organization is empty string" do
      let(:organization) { "" }
      
      it "uses only the account in the token" do
        token = subject.jwt_token
        decoded_token = JWT.decode(token, OpenSSL::PKey::RSA.new(private_key).public_key, true, { algorithm: 'RS256' })[0]
        
        expect(decoded_token["iss"]).to start_with("#{account.upcase}.")
        expect(decoded_token["sub"]).to eq("#{account.upcase}.#{user.upcase}")
      end
    end
    
    context "when organization is provided" do
      let(:organization) { "org" }
      
      it "uses org-account format in the token" do
        token = subject.jwt_token
        decoded_token = JWT.decode(token, OpenSSL::PKey::RSA.new(private_key).public_key, true, { algorithm: 'RS256' })[0]
        
        expect(decoded_token["iss"]).to start_with("#{organization.upcase}-#{account.upcase}.")
        expect(decoded_token["sub"]).to eq("#{organization.upcase}-#{account.upcase}.#{user.upcase}")
      end
    end
  end
end
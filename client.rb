require "fast_jsonparser" # claims to be 150% faster than ruby JSON and 40% faster than OJ
require "json"
require "jwt"
require "net/http"
require "uri"
require 'securerandom'

# TODO: double check that net/http is actually using compression like it should be
# TODO: investigate if streaming the result would be faster, especially if it can be streamed to the parser and yielded to the caller

class SnowflakeClient
  JWT_TOKEN_TTL = 3600 # seconds, this is the max supported by snowflake

  def initialize(uri, private_key_path, organization, account, user, public_key_fingerprint)
    @base_uri = uri
    @private_key_path = private_key_path
    @organization = organization
    @account = account
    @user = user
    @public_key_fingerprint = public_key_fingerprint # should be able to generate this from key pair, but haven't figured out right openssl options yet

    # start with an expired value to force creation
    @token_expires_at = Time.now.to_i - 1
  end

  def query(query)

    # use a persistent connection
    Net::HTTP.start(hostname, port, :use_ssl => uri.scheme == "https") do |http|

      response = request_with_auth_and_headers(http,
                                               Net::HTTP::Post,
                                               "/api/v2/statements?requestId=#{SecureRandom.uuid}",
                                               { "statement" => query }.to_json)
      get_all_response_data(http, response)
    end
  end

  private
    def hostname
      URI.parse(@base_uri).hostname
    end

    def port
      URI.parse(@base_uri).port
    end

    def jwt_token
      return @token unless jwt_token_expired?

      now = Time.now.to_i
      @token_expires_at = now + JWT_TOKEN_TTL

      private_key = OpenSSL::PKey.read(File.read(@private_key_path))

      payload = {
        :iss => "#{@organization.upcase}-#{@account.upcase}.#{@user}.#{@public_key_fingerprint}",
        :sub => "#{@organization.upcase}-#{@account.upcase}.#{@user}",
        :iat => now,
        :exp => @token_expires_at
      }

      @token = JWT.encode payload, private_key, "RS256"
    end

    def jwt_token_expired?
      Time.now.to_i > @token_expires_at
    end

    def handle_errors(response)
      raise "Bad response! Got code: #{response.code}, w/ message #{response.body}" unless response.code == "200"
    end

    def request_with_auth_and_headers(http, request_class, path, body=nil)
      uri = URI.parse("#{@base_uri}#{path}")
      request = request_class.new(uri)
      request["Content-Type"] = "application/json"
      request["Accept"] = "application/json"
      request["Authorization"] = "Bearer #{jwt_token}"
      request["X-Snowflake-Authorization-Token-Type"] = "KEYPAIR_JWT"
      request.body = body unless body.nil?

      response = http.request(request)
      handle_errors(response)
      response
    end

    def get_all_response_data(http, response)
      json_body = FastJsonparser.parse(response.body)
      statementHandle = json_body[:statementHandle]
      partitions = json_body[:resultSetMetaData][:partitionInfo]
      data = json_body[:data]
      partitions.each_with_index do |partition, index|
        next if index == 0 # already have the first partition

        expected_rows = partition[:rowCount]
        partition_response = request_with_auth_and_headers(
          http,
          Net::HTTP::Get,
          "/api/v2/statements/#{statementHandle}?partition=#{index}&requestId=#{SecureRandom.uuid}",
        )

        parition_json = FastJsonparser.parse(partition_response.body)
        partition_data = parition_json[:data]


        raise "mismatched data size!" if expected_rows != partition_data.size
        data.concat partition_data
      end
      data
    end
end

# frozen_string_literal: true

require "benchmark"
require "concurrent"
require "connection_pool"
require "json"
require "jwt"
require "net/http"
require "oj"
require "securerandom"
require "uri"

require_relative "result_set"

# TODO: double check that net/http is actually using compression like it should be
class SnowflakeClient
  JWT_TOKEN_TTL = 3600 # seconds, this is the max supported by snowflake
  CONNECTION_TIMEOUT = 5 # seconds
  MAX_CONNECTIONS = 8
  MAX_THREADS = 8
  THREAD_SCALE_FACTOR = 4 # parition count factor for number of threads (i.e. 2 == once we have 4 partitions, spin up a second thread)

  # TODO: parameterize warehouse
  def initialize(uri, private_key_path, organization, account, user, public_key_fingerprint)
    @base_uri = uri
    @private_key_path = private_key_path
    @organization = organization
    @account = account
    @user = user
    @public_key_fingerprint = public_key_fingerprint # should be able to generate this from key pair, but haven't figured out right openssl options yet

    # start with an expired value to force creation
    @token_expires_at = Time.now.to_i - 1
    @token_semaphore = Concurrent::Semaphore.new(1)
  end

  def query(query)
    response = nil
    connection_pool.with do |connection|
      response = request_with_auth_and_headers(
        connection,
        Net::HTTP::Post,
        "/api/v2/statements?requestId=#{SecureRandom.uuid}",
        { "statement" => query, "warehouse" => "WEB_TEST_WH" }.to_json
      )
    end
    handle_errors(response)
    get_all_response_data(response)
  end

  private
    def connection_pool
      @connection_pool ||= ConnectionPool.new(size: MAX_CONNECTIONS, timeout: CONNECTION_TIMEOUT) do
        # TODO: the connection pool lib expects these connections to be "Self healing", they're obviously not
        #       so we'll need to write a wrapper that is
        puts "OPENING CONNECTION"
        Net::HTTP.start(hostname, port, :use_ssl => true)
      end
    end

    def hostname
      @hostname ||= URI.parse(@base_uri).hostname
    end

    def port
      @port ||= URI.parse(@base_uri).port
    end

    def jwt_token
      return @token unless jwt_token_expired?

      @token_semaphore.acquire do
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
    end

    def jwt_token_expired?
      Time.now.to_i > @token_expires_at
    end

    def handle_errors(response)
      raise "Bad response! Got code: #{response.code}, w/ message #{response.body}" unless response.code == "200"
    end

    def request_with_auth_and_headers(connection, request_class, path, body=nil)
      uri = URI.parse("#{@base_uri}#{path}")
      request = request_class.new(uri)
      request["Content-Type"] = "application/json"
      request["Accept"] = "application/json"
      request["Authorization"] = "Bearer #{jwt_token}"
      request["X-Snowflake-Authorization-Token-Type"] = "KEYPAIR_JWT"
      request.body = body unless body.nil?

      response = nil
      bm = Benchmark.measure { response = connection.request(request) }
      puts "HTTP Request time: #{bm.real}"
      handle_errors(response)
      response
    end

    def get_all_response_data(response)
      json_body = Oj.load(response.body, oj_options)
      statement_handle = json_body["statementHandle"]
      partitions = json_body["resultSetMetaData"]["partitionInfo"]
      data = ResultSet.new(partitions.size, json_body["resultSetMetaData"]["rowType"])
      #data = Concurrent::Array.new(partitions.size)
      data[0] = json_body["data"]

      num_threads = number_of_threads_to_use(partitions.size)
      puts "PARTITION COUNT: #{partitions.size} THREADS: #{num_threads}"

      if num_threads == 1
        # execute on this thread and avoid overhead of a thread pool
        partitions.each_with_index do |partition, index|
          next if index == 0 # already have the first partition
          data[index] = retreive_partition_data(statement_handle, index)
        end
      else
        thread_pool = Concurrent::FixedThreadPool.new(num_threads)
        futures = []
        partitions.each_with_index do |partition, index|
          next if index == 0 # already have the first partition
          futures << Concurrent::Future.execute(executor: thread_pool) do
            [index, retreive_partition_data(statement_handle, index)]
          end
        end
        futures.each do |future|
          index, partition_data = future.value
          data[index] = partition_data
        end
      end
      data
    end

    def retreive_partition_data(statement_handle, partition_index)
      partition_response = nil
      connection_pool.with do |connection|
        partition_response = request_with_auth_and_headers(
          connection,
          Net::HTTP::Get,
          "/api/v2/statements/#{statement_handle}?partition=#{partition_index}&requestId=#{SecureRandom.uuid}",
        )
      end

      partition_json = nil
      bm = Benchmark.measure { partition_json = Oj.load(partition_response.body, oj_options) }
      puts "JSON parsing took: #{bm.real}"
      partition_data = partition_json["data"]

      partition_data
    end

    def number_of_threads_to_use(partition_count)
      [[1, (partition_count / THREAD_SCALE_FACTOR.to_f).ceil].max, MAX_THREADS].min
    end

    def oj_options
      { :bigdecimal_load => :bigdecimal }
    end
end

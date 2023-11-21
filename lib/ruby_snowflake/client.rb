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

require_relative "result"
require_relative "streaming_result"
require_relative "client/streaming_result_strategy"
require_relative "client/threaded_in_memory_strategy"
require_relative "client/single_thread_in_memory_strategy"

module RubySnowflake
  class Error < StandardError
    # This will get pulled through to Sentry, see:
    # https://github.com/getsentry/sentry-ruby/blob/11ecd254c0d2cae2b327f0348074e849095aa32d/sentry-ruby/lib/sentry/error_event.rb#L31-L33
    attr_reader :sentry_context

    def initialize(details)
      @sentry_context = details
    end
  end

  # TODO: double check that net/http is actually using compression like it should be
  class Client
    JWT_TOKEN_TTL = 3600 # seconds, this is the max supported by snowflake
    CONNECTION_TIMEOUT = 30 # seconds
    MAX_CONNECTIONS = 8
    MAX_THREADS = 8
    THREAD_SCALE_FACTOR = 4 # parition count factor for number of threads (i.e. 2 == once we have 4 partitions, spin up a second thread)

    def self.connect
      new(
        ENV["SNOWFLAKE_URI"],
        ENV["SNOWFLAKE_PRIVATE_KEY_PATH"],
        ENV["SNOWFLAKE_ORGANIZATION"],
        ENV["SNOWFLAKE_ACCOUNT"],
        ENV["SNOWFLAKE_USER"],
        ENV["SNOWFLAKE_PUBLIC_KEY_FINGERPRINT"],
      )
    end

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

    def query(query, options = {})
      statement_count = count_statements(query) if options[:statement_count] == nil

      response = nil
      connection_pool.with do |connection|
        request_body = { "statement" => query, "warehouse" => "WEB_TEST_WH" }
        request_body["MULTI_STATEMENT_COUNT"] = statement_count if statement_count > 1

        response = request_with_auth_and_headers(
          connection,
          Net::HTTP::Post,
          "/api/v2/statements?requestId=#{SecureRandom.uuid}",
          Oj.dump(request_body)
        )
      end
      handle_errors(response)
      retreive_result_set(response, options[:streaming])
    end

    private
      def connection_pool
        @connection_pool ||= ConnectionPool.new(size: MAX_CONNECTIONS, timeout: CONNECTION_TIMEOUT) do
          # TODO: the connection pool lib expects these connections to be "Self healing", they're obviously not
          #       so we'll need to write a wrapper that is
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

      def count_statements(query)
        query.split(";").select {|part| part.strip.length > 0 }.size
      end

      def handle_errors(response)
        if response.code != "200"
          raise Error.new({}),
            "Bad response! Got code: #{response.code}, w/ message #{response.body}"
        end
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

      def retreive_result_set(response, streaming)
        json_body = Oj.load(response.body, oj_options)
        statement_handle = json_body["statementHandle"]
        num_threads = number_of_threads_to_use(json_body["resultSetMetaData"]["partitionInfo"].size)
        retreive_proc = ->(index) { retreive_partition_data(statement_handle, index) }

        if streaming
          StreamingResultStrategy.result(json_body, retreive_proc)
        elsif num_threads == 1
          SingleThreadInMemoryStrategy.result(json_body, retreive_proc)
        else
          ThreadedInMemoryStrategy.result(json_body, retreive_proc, num_threads)
        end
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

      def count_statements(query)
        query.split(";").select {|part| part.strip.length > 0 }.size
      end

      def number_of_threads_to_use(partition_count)
        [[1, (partition_count / THREAD_SCALE_FACTOR.to_f).ceil].max, MAX_THREADS].min
      end

      def oj_options
        { :bigdecimal_load => :bigdecimal }
      end
  end
end

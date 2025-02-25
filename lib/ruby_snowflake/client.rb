# frozen_string_literal: true

require "base64"
require "benchmark"
require "bigdecimal"
require "concurrent"
require "connection_pool"
require "json"
require "logger"
require "net/http"
require "retryable"
require "securerandom"
require "uri"

require_relative "client/http_connection_wrapper"
require_relative "client/key_pair_jwt_auth_manager"
require_relative "client/single_thread_in_memory_strategy"
require_relative "client/streaming_result_strategy"
require_relative "client/threaded_in_memory_strategy"
require_relative "result"
require_relative "streaming_result"

module RubySnowflake
  class Error < StandardError
    def initialize(details)
      @details = details
    end

    def message
      @details.to_s
    end
  end

  class BadResponseError < Error ; end
  class ConnectionError < Error ; end
  class ConnectionStarvedError < Error ; end
  class MissingConfig < Error ; end
  class RetryableBadResponseError < Error ; end
  class RequestError < Error ; end
  class QueryTimeoutError < Error ;  end

  class Client
    DEFAULT_LOGGER = Logger.new(STDOUT)
    DEFAULT_LOG_LEVEL = Logger::INFO
    # seconds (59 min), this is the max supported by snowflake - 1 minute
    DEFAULT_JWT_TOKEN_TTL = 3540
    # seconds, how long for a thread to wait for a connection before erroring
    DEFAULT_CONNECTION_TIMEOUT = 60
    # default maximum size of the http connection pool
    DEFAULT_MAX_CONNECTIONS = 16
    # default maximum size of the thread pool on a single query
    DEFAULT_MAX_THREADS_PER_QUERY = 8
    # partition count factor for number of threads
    # (i.e. 2 == once we have 4 partitions, spin up a second thread)
    DEFAULT_THREAD_SCALE_FACTOR = 4
    # how many times to retry common retryable HTTP responses (i.e. 429, 504)
    DEFAULT_HTTP_RETRIES = 2
    # how long to wait to allow a query to complete, in seconds
    DEFAULT_QUERY_TIMEOUT = 600 # 10 minutes

    JSON_PARSE_OPTIONS = { decimal_class: BigDecimal }.freeze
    VALID_RESPONSE_CODES = %w(200 202).freeze
    POLLING_RESPONSE_CODE = "202"
    POLLING_INTERVAL = 2 # seconds

    # can't be set after initialization
    attr_reader :connection_timeout, :max_connections, :logger, :max_threads_per_query, :thread_scale_factor, :http_retries, :query_timeout

    def self.from_env(logger: DEFAULT_LOGGER,
                      log_level: DEFAULT_LOG_LEVEL,
                      jwt_token_ttl: env_option("SNOWFLAKE_JWT_TOKEN_TTL", DEFAULT_JWT_TOKEN_TTL),
                      connection_timeout: env_option("SNOWFLAKE_CONNECTION_TIMEOUT", DEFAULT_CONNECTION_TIMEOUT ),
                      max_connections: env_option("SNOWFLAKE_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS ),
                      max_threads_per_query: env_option("SNOWFLAKE_MAX_THREADS_PER_QUERY", DEFAULT_MAX_THREADS_PER_QUERY),
                      thread_scale_factor: env_option("SNOWFLAKE_THREAD_SCALE_FACTOR", DEFAULT_THREAD_SCALE_FACTOR),
                      http_retries: env_option("SNOWFLAKE_HTTP_RETRIES", DEFAULT_HTTP_RETRIES),
                      query_timeout: env_option("SNOWFLAKE_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT))
      private_key =
        if key = ENV["SNOWFLAKE_PRIVATE_KEY"]
          key
        elsif path = ENV["SNOWFLAKE_PRIVATE_KEY_PATH"]
          File.read(path)
        else
          raise MissingConfig, "Either ENV['SNOWFLAKE_PRIVATE_KEY'] or ENV['SNOWFLAKE_PRIVATE_KEY_PATH'] must be set"
        end

      new(
        ENV.fetch("SNOWFLAKE_URI"),
        private_key,
        ENV.fetch("SNOWFLAKE_ORGANIZATION"),
        ENV.fetch("SNOWFLAKE_ACCOUNT"),
        ENV.fetch("SNOWFLAKE_USER"),
        ENV["SNOWFLAKE_DEFAULT_WAREHOUSE"],
        ENV["SNOWFLAKE_DEFAULT_DATABASE"],
        logger: logger,
        log_level: log_level,
        jwt_token_ttl: jwt_token_ttl,
        connection_timeout: connection_timeout,
        max_connections: max_connections,
        max_threads_per_query: max_threads_per_query,
        thread_scale_factor: thread_scale_factor,
        http_retries: http_retries,
        query_timeout: query_timeout,
      )
    end

    def initialize(
      uri, private_key, organization, account, user, default_warehouse, default_database,
      logger: DEFAULT_LOGGER,
      log_level: DEFAULT_LOG_LEVEL,
      jwt_token_ttl: DEFAULT_JWT_TOKEN_TTL,
      connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
      max_connections: DEFAULT_MAX_CONNECTIONS,
      max_threads_per_query: DEFAULT_MAX_THREADS_PER_QUERY,
      thread_scale_factor: DEFAULT_THREAD_SCALE_FACTOR,
      http_retries: DEFAULT_HTTP_RETRIES,
      query_timeout: DEFAULT_QUERY_TIMEOUT
    )
      @base_uri = uri
      @key_pair_jwt_auth_manager =
        KeyPairJwtAuthManager.new(organization, account, user, private_key, jwt_token_ttl)
      @default_warehouse = default_warehouse
      @default_database = default_database

      # set defaults for config settings
      @logger = logger
      @logger.level = log_level
      @connection_timeout = connection_timeout
      @max_connections = max_connections
      @max_threads_per_query = max_threads_per_query
      @thread_scale_factor = thread_scale_factor
      @http_retries = http_retries
      @query_timeout = query_timeout

      # Do NOT use normally, this exists for tests so we can reliably trigger the polling
      # response workflow from snowflake in tests
      @_enable_polling_queries = false
    end

    def query(query, warehouse: nil, streaming: false, database: nil, schema: nil, bindings: nil)
      warehouse ||= @default_warehouse
      database ||= @default_database

      query_start_time = Time.now.to_i
      response = nil
      connection_pool.with do |connection|
        request_body = {
          "warehouse" => warehouse&.upcase,
          "schema" => schema&.upcase,
          "database" =>  database&.upcase,
          "statement" => query,
          "bindings" => bindings
        }

        response = request_with_auth_and_headers(
          connection,
          Net::HTTP::Post,
          "/api/v2/statements?requestId=#{SecureRandom.uuid}&async=#{@_enable_polling_queries}",
          request_body.to_json
        )
      end
      retrieve_result_set(query_start_time, query, response, streaming)
    end

    alias fetch query

    def self.env_option(env_var_name, default_value)
      value = ENV[env_var_name]
      value.nil? || value.empty? ? default_value : ENV[env_var_name].to_i
    end

    # This method can be used to populate the JWT token used for authentication
    # in tests that require time travel.
    def create_jwt_token
      @key_pair_jwt_auth_manager.jwt_token
    end

    private_class_method :env_option

    private
      def connection_pool
        @connection_pool ||= ConnectionPool.new(size: @max_connections, timeout: @connection_timeout) do
          HttpConnectionWrapper.new(hostname, port).start
        end
      end

      def hostname
        @hostname ||= URI.parse(@base_uri).hostname
      end

      def port
        @port ||= URI.parse(@base_uri).port
      end

      def request_with_auth_and_headers(connection, request_class, path, body=nil)
        uri = URI.parse("#{@base_uri}#{path}")
        request = request_class.new(uri)
        request["Content-Type"] = "application/json"
        request["Accept"] = "application/json"
        request["Authorization"] = "Bearer #{@key_pair_jwt_auth_manager.jwt_token}"
        request["X-Snowflake-Authorization-Token-Type"] = "KEYPAIR_JWT"
        request.body = body unless body.nil?

        Retryable.retryable(tries: @http_retries + 1,
                            sleep: lambda {|n| 2**n }, # 1, 2, 4, 8, etc
                            on: [RetryableBadResponseError, OpenSSL::SSL::SSLError],
                            log_method: retryable_log_method) do
          response = nil
          bm = Benchmark.measure { response = connection.request(request) }
          logger.debug { "HTTP Request time: #{bm.real}" }
          raise_on_bad_response(response)
          response
        end
      end

      def raise_on_bad_response(response)
        return if VALID_RESPONSE_CODES.include? response.code

        # there are a class of errors we want to retry rather than just giving up
        if retryable_http_response_code?(response.code)
          raise RetryableBadResponseError,
                "Retryable bad response! Got code: #{response.code}, w/ message #{response.body}"

        else # not one we should retry
          raise BadResponseError,
            "Bad response! Got code: #{response.code}, w/ message #{response.body}"
        end
      end

      # shamelessly stolen from the battle tested python client
      # https://github.com/snowflakedb/snowflake-connector-python/blob/eceed981f93e29d2f4663241253b48340389f4ef/src/snowflake/connector/network.py#L191
      def retryable_http_response_code?(code)
        # retry (in order): bad request, forbidden (token expired in flight), method not allowed,
        # request timeout, too many requests, anything in the 500 range (504 is fairly common),
        # anything in the 3xx range as those are mostly "redirect" responses
        [400, 403, 405, 408, 429].include?(code.to_i) || (500..599).include?(code.to_i) ||
         (300..399).include?(code.to_i)
      end

      def retryable_log_method
        @retryable_log_method ||= proc do |retries, error|
          logger.info("Retry attempt #{retries} because #{error.message}")
        end
      end

      def poll_for_completion_or_timeout(query_start_time, query, statement_handle)
        first_data_json_body = nil

        connection_pool.with do |connection|
          loop do
            sleep POLLING_INTERVAL

            elapsed_time = Time.now.to_i - query_start_time
            if elapsed_time > @query_timeout
              cancelled = attempt_to_cancel_and_silence_errors(connection, statement_handle)
              raise QueryTimeoutError.new("Query timed out. Query cancelled? #{cancelled}; Duration: #{elapsed_time}; Query: '#{query}'")
            end

            poll_response = request_with_auth_and_headers(connection, Net::HTTP::Get,
                                                          "/api/v2/statements/#{statement_handle}")
            if poll_response.code == POLLING_RESPONSE_CODE
              next
            else
              return poll_response
            end
          end
        end
      end

      def attempt_to_cancel_and_silence_errors(connection, statement_handle)
        cancel_response = request_with_auth_and_headers(connection, Net::HTTP::Post,
                                                        "/api/v2/#{statement_handle}/cancel")
        true
      rescue Error => error
        if error.is_a?(BadResponseError) && error.message.include?("404")
          return true # snowflake cancelled it before we did
        end
        @logger.error("Error on attempting to cancel query #{statement_handle}, will raise a QueryTimeoutError")
        false
      end

      def retrieve_result_set(query_start_time, query, response, streaming)
        json_body = JSON.parse(response.body, JSON_PARSE_OPTIONS)
        statement_handle = json_body["statementHandle"]

        if response.code == POLLING_RESPONSE_CODE
          result_response = poll_for_completion_or_timeout(query_start_time, query, statement_handle)
          json_body = JSON.parse(result_response.body, JSON_PARSE_OPTIONS)
        end

        num_threads = number_of_threads_to_use(json_body["resultSetMetaData"]["partitionInfo"].size)
        retrieve_proc = ->(index) { retrieve_partition_data(statement_handle, index) }

        if streaming
          StreamingResultStrategy.result(json_body, retrieve_proc)
        elsif num_threads == 1
          SingleThreadInMemoryStrategy.result(json_body, retrieve_proc)
        else
          ThreadedInMemoryStrategy.result(json_body, retrieve_proc, num_threads)
        end
      end

      def retrieve_partition_data(statement_handle, partition_index)
        partition_response = nil
        connection_pool.with do |connection|
          partition_response = request_with_auth_and_headers(
            connection,
            Net::HTTP::Get,
            "/api/v2/statements/#{statement_handle}?partition=#{partition_index}&requestId=#{SecureRandom.uuid}",
          )
        end

        partition_json = {}
        bm = Benchmark.measure { partition_json = JSON.parse(partition_response.body, JSON_PARSE_OPTIONS) }
        logger.debug { "JSON parsing took: #{bm.real}" }
        partition_data = partition_json["data"]

        partition_data
      end

      def number_of_threads_to_use(partition_count)
        [[1, (partition_count / @thread_scale_factor.to_f).ceil].max, @max_threads_per_query].min
      end
  end
end

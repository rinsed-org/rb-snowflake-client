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
require "stringio"
require "uri"
require "zlib"

begin
  require "active_support"
  require "active_support/notifications"
rescue LoadError
  # This isn't required
end

require_relative "client/auth_manager"
require_relative "client/http_connection_wrapper"
require_relative "client/key_pair_jwt_auth_manager"
require_relative "client/browser_launcher"
require_relative "client/sso_callback_server"
require_relative "client/external_browser_auth_manager"
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
  class QueryTimeoutError < Error ; end
  class AuthenticationError < Error ; end
  class SsoTimeoutError < Error ; end
  class BrowserLaunchError < Error ; end

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
    # default role to use
    DEFAULT_ROLE = nil
    # seconds to wait for SSO callback
    DEFAULT_SSO_TIMEOUT = 120
    # SSO callback server port (0 = random available port)
    DEFAULT_SSO_PORT = 0

    JSON_PARSE_OPTIONS = { decimal_class: BigDecimal }.freeze
    VALID_RESPONSE_CODES = %w(200 202).freeze
    POLLING_RESPONSE_CODE = "202"
    POLLING_INTERVAL = 2 # seconds

    # can't be set after initialization
    attr_reader :connection_timeout, :max_connections, :logger, :max_threads_per_query, :thread_scale_factor, :http_retries, :query_timeout, :default_role

    def self.from_env(logger: DEFAULT_LOGGER,
                      log_level: DEFAULT_LOG_LEVEL,
                      jwt_token_ttl: env_option("SNOWFLAKE_JWT_TOKEN_TTL", DEFAULT_JWT_TOKEN_TTL),
                      connection_timeout: env_option("SNOWFLAKE_CONNECTION_TIMEOUT", DEFAULT_CONNECTION_TIMEOUT ),
                      max_connections: env_option("SNOWFLAKE_MAX_CONNECTIONS", DEFAULT_MAX_CONNECTIONS ),
                      max_threads_per_query: env_option("SNOWFLAKE_MAX_THREADS_PER_QUERY", DEFAULT_MAX_THREADS_PER_QUERY),
                      thread_scale_factor: env_option("SNOWFLAKE_THREAD_SCALE_FACTOR", DEFAULT_THREAD_SCALE_FACTOR),
                      http_retries: env_option("SNOWFLAKE_HTTP_RETRIES", DEFAULT_HTTP_RETRIES),
                      query_timeout: env_option("SNOWFLAKE_QUERY_TIMEOUT", DEFAULT_QUERY_TIMEOUT),
                      default_role: env_option("SNOWFLAKE_DEFAULT_ROLE", DEFAULT_ROLE),
                      sso_timeout: env_option("SNOWFLAKE_SSO_TIMEOUT", DEFAULT_SSO_TIMEOUT),
                      sso_port: env_option("SNOWFLAKE_SSO_PORT", DEFAULT_SSO_PORT))
      authenticator = ENV.fetch("SNOWFLAKE_AUTHENTICATOR", "keypair_jwt")

      private_key = nil
      if authenticator.downcase == "keypair_jwt"
        private_key =
          if key = ENV["SNOWFLAKE_PRIVATE_KEY"]
            key
          elsif path = ENV["SNOWFLAKE_PRIVATE_KEY_PATH"]
            File.read(path)
          else
            raise MissingConfig, "For keypair_jwt auth, either ENV['SNOWFLAKE_PRIVATE_KEY'] or ENV['SNOWFLAKE_PRIVATE_KEY_PATH'] must be set"
          end
      end

      new(
        ENV.fetch("SNOWFLAKE_URI"),
        private_key,
        ENV.fetch("SNOWFLAKE_ORGANIZATION", nil),
        ENV.fetch("SNOWFLAKE_ACCOUNT"),
        ENV.fetch("SNOWFLAKE_USER"),
        ENV["SNOWFLAKE_DEFAULT_WAREHOUSE"],
        ENV["SNOWFLAKE_DEFAULT_DATABASE"],
        default_role: ENV.fetch("SNOWFLAKE_DEFAULT_ROLE", nil),
        logger: logger,
        log_level: log_level,
        jwt_token_ttl: jwt_token_ttl,
        connection_timeout: connection_timeout,
        max_connections: max_connections,
        max_threads_per_query: max_threads_per_query,
        thread_scale_factor: thread_scale_factor,
        http_retries: http_retries,
        query_timeout: query_timeout,
        authenticator: authenticator,
        sso_timeout: sso_timeout,
        sso_port: sso_port
      )
    end

    def initialize(
      uri, private_key, organization, account, user, default_warehouse, default_database,
      default_role: nil,
      logger: DEFAULT_LOGGER,
      log_level: DEFAULT_LOG_LEVEL,
      jwt_token_ttl: DEFAULT_JWT_TOKEN_TTL,
      connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
      max_connections: DEFAULT_MAX_CONNECTIONS,
      max_threads_per_query: DEFAULT_MAX_THREADS_PER_QUERY,
      thread_scale_factor: DEFAULT_THREAD_SCALE_FACTOR,
      http_retries: DEFAULT_HTTP_RETRIES,
      query_timeout: DEFAULT_QUERY_TIMEOUT,
      authenticator: "keypair_jwt",
      sso_timeout: DEFAULT_SSO_TIMEOUT,
      sso_port: DEFAULT_SSO_PORT
    )
      @base_uri = uri
      @default_warehouse = default_warehouse
      @default_database = default_database
      @default_role = default_role

      # set defaults for config settings
      @logger = logger
      @logger.level = log_level
      @connection_timeout = connection_timeout
      @max_connections = max_connections
      @max_threads_per_query = max_threads_per_query
      @thread_scale_factor = thread_scale_factor
      @http_retries = http_retries
      @query_timeout = query_timeout

      @auth_manager = if private_key.respond_to?(:apply_auth)
        private_key
      else
        case authenticator.to_s.downcase
        when "keypair_jwt", "snowflake_jwt"
          KeyPairJwtAuthManager.new(organization, account, user, private_key, jwt_token_ttl)
        when "externalbrowser"
          account_identifier = Client.build_account_identifier(organization, account)
          @externalbrowser_base_uri = "https://#{account_identifier.downcase}.snowflakecomputing.com"
          ExternalBrowserAuthManager.new(
            @externalbrowser_base_uri,
            account_identifier,
            user,
            sso_timeout: sso_timeout,
            sso_port: sso_port,
            logger: @logger
          )
        else
          raise MissingConfig.new("Unsupported authenticator: #{authenticator}. Supported: keypair_jwt, externalbrowser")
        end
      end

      @key_pair_jwt_auth_manager = @auth_manager if @auth_manager.is_a?(KeyPairJwtAuthManager)

      # Do NOT use normally, this exists for tests so we can reliably trigger the polling
      # response workflow from snowflake in tests
      @_enable_polling_queries = false
    end

    def query(query, warehouse: nil, streaming: false, database: nil, schema: nil, bindings: nil, role: nil, query_name: nil, query_timeout: nil)
      warehouse ||= @default_warehouse
      database ||= @default_database
      role ||= @default_role
      query_timeout ||= @query_timeout

      if @auth_manager.respond_to?(:uses_v1_api?) && @auth_manager.uses_v1_api?
        if bindings && !bindings.empty?
          raise ArgumentError, "Bindings are not supported with externalbrowser authentication. Use string interpolation or switch to keypair auth."
        end
        with_instrumentation({ database:, schema:, warehouse:, query_name: }) do
          query_v1(query, warehouse: warehouse, database: database, schema: schema, role: role, streaming: streaming, query_timeout: query_timeout)
        end
      else
        with_instrumentation({ database:, schema:, warehouse:, query_name: }) do
          query_start_time = Time.now.to_i
          response = nil
          connection_pool.with do |connection|
            request_body = {
              "warehouse" => warehouse&.upcase,
              "schema" => schema&.upcase,
              "database" =>  database&.upcase,
              "statement" => query,
              "bindings" => bindings,
              "role" => role,
              "timeout" => query_timeout
            }

            response = request_with_auth_and_headers(
              connection,
              Net::HTTP::Post,
              "/api/v2/statements?requestId=#{SecureRandom.uuid}&async=#{@_enable_polling_queries}",
              request_body.to_json
            )
          end
          retrieve_result_set(query_start_time, query, response, streaming, query_timeout)
        end
      end
    end

    alias fetch query

    def self.env_option(env_var_name, default_value)
      value = ENV[env_var_name]
      value.nil? || value.empty? ? default_value : ENV[env_var_name].to_i
    end

    # This method can be used to populate the JWT token used for authentication
    # in tests that require time travel.
    def create_jwt_token
      if @auth_manager.respond_to?(:jwt_token)
        @auth_manager.jwt_token
      else
        @auth_manager.token
      end
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
        @auth_manager.apply_auth(request)
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

      def poll_for_completion_or_timeout(query_start_time, query, statement_handle, query_timeout)
        first_data_json_body = nil

        connection_pool.with do |connection|
          loop do
            sleep POLLING_INTERVAL

            elapsed_time = Time.now.to_i - query_start_time
            if elapsed_time > query_timeout
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

      def retrieve_result_set(query_start_time, query, response, streaming, query_timeout)
        json_body = JSON.parse(response.body, JSON_PARSE_OPTIONS)
        statement_handle = json_body["statementHandle"]

        if response.code == POLLING_RESPONSE_CODE
          result_response = poll_for_completion_or_timeout(query_start_time, query, statement_handle, query_timeout)
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

      def query_v1(query, warehouse:, database:, schema:, role:, streaming:, query_timeout:)
        response = nil
        connection_pool.with do |connection|
          uri = URI.parse("#{@externalbrowser_base_uri}/queries/v1/query-request?requestId=#{SecureRandom.uuid}")
          request = Net::HTTP::Post.new(uri)
          request["Content-Type"] = "application/json"
          request["Accept"] = "application/snowflake"
          @auth_manager.apply_auth(request)

          request_body = {
            "sqlText" => query,
            "sequenceId" => 1,
            "querySubmissionTime" => (Time.now.to_f * 1000).to_i
          }
          request_body["warehouse"] = warehouse.upcase if warehouse
          request_body["database"] = database.upcase if database
          request_body["schema"] = schema.upcase if schema
          request_body["role"] = role if role
          request_body["timeout"] = query_timeout if query_timeout

          request.body = request_body.to_json

          Retryable.retryable(tries: @http_retries + 1,
                              sleep: lambda {|n| 2**n },
                              on: [RetryableBadResponseError, OpenSSL::SSL::SSLError],
                              log_method: retryable_log_method) do
            bm = Benchmark.measure { response = connection.request(request) }
            logger.debug { "HTTP Request time: #{bm.real}" }
            raise_on_bad_response(response)
          end
        end

        json_body = JSON.parse(response.body, JSON_PARSE_OPTIONS)

        unless json_body["success"]
          error_msg = json_body["message"] || "Unknown error"
          error_code = json_body["code"]
          raise BadResponseError.new("Snowflake query failed (#{error_code}): #{error_msg}")
        end

        data = json_body["data"]
        chunks = data["chunks"] || []
        chunk_headers = data["chunkHeaders"] || {}

        logger.debug { "v1 API response - rowset: #{data["rowset"]&.size || 0} rows, chunks: #{chunks.size}" }

        partition_info = [{ "rowCount" => data["rowset"]&.size || 0 }] +
                         chunks.map { |c| { "rowCount" => c["rowCount"] } }
        statement_json = {
          "resultSetMetaData" => { "rowType" => data["rowtype"], "partitionInfo" => partition_info },
          "data" => data["rowset"] || []
        }

        retrieve_proc = ->(index) {
          chunk = chunks[index - 1]
          uri = URI.parse(chunk["url"])
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = (uri.scheme == "https")

          request = Net::HTTP::Get.new(uri)
          chunk_headers.each { |k, v| request[k] = v }

          begin
            response = http.request(request)
            unless response.code.start_with?("2")
              raise BadResponseError.new("S3 chunk fetch failed (#{response.code}): #{response.body[0..500]}")
            end

            body = response.body
            is_gzip = response["Content-Encoding"]&.downcase == "gzip" ||
                      (body.bytesize >= 2 && body.byteslice(0, 2) == "\x1f\x8b".b)
            if is_gzip
              body = Zlib::GzipReader.new(StringIO.new(body)).read
            end

            parsed = JSON.parse("[#{body}]")
            logger.debug { "Chunk #{index} fetched: #{parsed.size} rows" }
            parsed
          rescue => e
            logger.error("Failed to fetch chunk #{index}: #{e.class} - #{e.message}")
            raise
          end
        }

        if streaming
          StreamingResultStrategy.result(statement_json, retrieve_proc)
        elsif chunks.empty?
          result = Result.new(1, data["rowtype"])
          result[0] = data["rowset"]
          result
        else
          num_threads = number_of_threads_to_use(partition_info.size)
          if num_threads == 1
            SingleThreadInMemoryStrategy.result(statement_json, retrieve_proc)
          else
            ThreadedInMemoryStrategy.result(statement_json, retrieve_proc, num_threads)
          end
        end
      end

      def self.build_account_identifier(organization, account)
        if organization.nil? || organization.empty?
          account.upcase
        else
          "#{organization.upcase}-#{account.upcase}"
        end
      end

      def with_instrumentation(tags, &block)
        return block.call unless defined?(::ActiveSupport) && ::ActiveSupport

        ::ActiveSupport::Notifications.instrument(
            "rb_snowflake_client.snowflake_query.finish",
            tags.merge(query_id: SecureRandom.uuid)) do
          block.call
        end
      end
  end
end

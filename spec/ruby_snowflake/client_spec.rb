require "spec_helper"

RSpec.describe RubySnowflake::Client do
  let(:client) { described_class.from_env }

  describe "initialization" do
    context "when the environment variables are not set" do
      around do |example|
        old_env = ENV.to_h

        begin
          ENV.clear
          example.run
        ensure
          ENV.replace(old_env)
        end
      end

      it "should raise an error" do
        expect { client }.to raise_error(RubySnowflake::MissingConfig)
      end
    end
  end

  describe "querying" do
    let(:query) { "" }
    let(:result) { client.query(query) }

    context "with the alias name `fetch`" do
      let(:query) { "SELECT 1;" }
      it "should work" do
        result = client.fetch(query)

        expect(result).to be_a(RubySnowflake::Result)
        expect(result.length).to eq(1)
        rows = result.get_all_rows
        expect(rows).to eq(
          [{"1" => 1}]
        )
      end
    end

    context "with lower case database name" do
      let(:query) { "SELECT * from public.test_datatypes;" }

      it "should work" do
        result = client.fetch(query, database: "ruby_snowflake_client_testing")

        expect(result).to be_a(RubySnowflake::Result)
        expect(result.length).to eq(2)
      end
    end

    context "with lower case schema name" do
      let(:query) { "SELECT * from test_datatypes;" }

      it "should work" do
        result = client.fetch(query, database: "ruby_snowflake_client_testing", schema: "public")

        expect(result).to be_a(RubySnowflake::Result)
        expect(result.length).to eq(2)
      end
    end

    context "with lower case warehouse name" do
      let(:query) { "SELECT * from ruby_snowflake_client_testing.public.test_datatypes;" }

      it "should work" do
        result = client.fetch(query, warehouse: "web_data_load_wh")

        expect(result).to be_a(RubySnowflake::Result)
        expect(result.length).to eq(2)
      end
    end

    context "when we can't connect" do
      before do
        allow(Net::HTTP).to receive(:start).and_raise("Some connection error")
      end

      it "raises a ConnectionError" do
        expect { result }.to raise_error do |error|
          expect(error).to be_a RubySnowflake::ConnectionError
          expect(error.cause.message).to eq "Some connection error"
        end
      end
    end

    context "when the query times out" do
      before do
        ENV["SNOWFLAKE_QUERY_TIMEOUT"] = "1"
        client.instance_variable_set(:@_enable_polling_queries, true)
      end
      after do
        ENV["SNOWFLAKE_QUERY_TIMEOUT"] = nil
      end
      let(:query) { "SELECT SYSTEM$WAIT(10)" }

      it "attempts to cancel the query" do
        allow(client.logger).to receive(:error)

        start_time = Time.now.to_i
        expect { result }.to raise_error do |error|
          expect(error).to be_a RubySnowflake::QueryTimeoutError
        end
        # We are not receiving this error because we cancel it before Snowflake can
        expect(client.logger).not_to have_received(:error).with(a_string_including("cancel query"))
        expect(Time.now.to_i - start_time).to be >= 1 #query timeout
      end
    end

    context "when the query errors" do
      let(:query) { "INVALID QUERY;" }
      it "should raise an exception" do
        expect { result }.to raise_error do |error|
          expect(error).to be_a RubySnowflake::Error
          # TODO: make sure to include query in context
          #expect(error.sentry_context).to include(
            #sql: query
          #)
        end
      end

      context "for unauthorized database" do
        let(:query) { "SELECT * FROM TEST_DATABASE.RINSED_WEB_APP.EMAILS LIMIT 1;" }
        it "should raise an exception" do
          expect { result }.to raise_error do |error|
            expect(error).to be_a RubySnowflake::Error
            expect(error.message).to include "'TEST_DATABASE' does not exist or not authorized"
            # TODO: make sure to include query in context
            #expect(error.sentry_context).to include(
              #sql: query
            #)
          end
        end

        it "should raise the correct exception for threaded work" do
          require "parallel"

          Parallel.map((1..3).collect { _1 }, in_threads: 2) do |idx|
            c = described_class.from_env
            query = "SELECT * FROM TEST_DATABASE#{idx}.RINSED_WEB_APP.EMAILS LIMIT 1;"

            expect { c.query(query) }.to raise_error do |error|
              expect(error).to be_a RubySnowflake::Error
              # TODO: make sure to include query in context
              #expect(error.sentry_context).to include(
                #sql: query
              #)
              expect(error.message).to include "TEST_DATABASE#{idx}"
            end
          end
        end
      end
    end

    context "with a simple query returning string" do
      let(:query) { "SELECT 1;" }

      it "should return a RubySnowflake::Result" do
        expect(result).to be_a(RubySnowflake::Result)
      end

      it "should respond to get_all_rows" do
        expect(result.length).to eq(1)
        rows = result.get_all_rows
        expect(rows).to eq(
          [{"1" => 1}]
        )
      end

      it "should respond to each with a block" do
        expect { |b| result.each(&b) }.to yield_with_args(an_instance_of(RubySnowflake::Row))
      end
    end

    context "with a more complex query" do
      # We have setup a simple table in our Snowflake account with the below structure:
      # CREATE TABLE ruby_snowflake_client_testing.public.test_datatypes
      #   (ID int, NAME string, DOB date, CREATED_AT timestamp, COFFES_PER_WEEK float);
      # And inserted some test data:
      # INSERT INTO test_datatypes
      #    VALUES (1, 'John Smith', '1990-10-17', current_timestamp(), 3.41),
      #    (2, 'Jane Smith', '1990-01-09', current_timestamp(), 3.525);
      let(:query) { "SELECT * from ruby_snowflake_client_testing.public.test_datatypes;" }
      let(:expected_john) do
        {
          "coffes_per_week" => 3.41,
          "id" => 1,
          "dob" => Date.new(1990, 10, 17),
          "created_at" => be_within(0.01).of(Time.new(2023,5,12,4,22,8.63,0)),
          "name" => "John Smith",
        }
      end
      let(:expected_jane) do
        {
          "coffes_per_week" => 3.525,
          "id" => 2,
          "dob" => Date.new(1990, 1, 9),
          "created_at" => be_within(0.01).of(Time.new(2023,5,12,4,22,8.63,0)),
          "name" => "Jane Smith",
        }
      end

      it "should return 2 rows with the right data types" do
        rows = result.get_all_rows
        expect(rows.length).to eq(2)
        john = rows[0]
        jane = rows[1]
        expect(john).to match(expected_john)
        expect(jane).to match(expected_jane)
      end
    end

    context "with NUMBER and HighPrecision" do
      # We have setup a simple table in our Snowflake account with the below structure:
      # CREATE TABLE ruby_snowflake_client_testing.public.test_big_datatypes
      #   (ID NUMBER(38,0), BIGFLOAT NUMBER(8,2));
      # And inserted some test data:
      # INSERT INTO test_big_datatypes VALUES (1, 8.2549);
      let(:query) { "SELECT * from ruby_snowflake_client_testing.public.test_big_datatypes;" }
      it "should return 1 row with correct data types" do
        rows = result.get_all_rows
        expect(rows.length).to eq(1)
        expect(rows[0]).to eq({
          "id" => 1,
          "bigfloat" => BigDecimal("8.25"), #precision of only 2 decimals
        })
      end
    end

    context "with all the time types" do
      # We have setup a simple table for testing these, created with:
      # CREATE TABLE ruby_snowflake_client_testing.public.time_test
      #  (ID int PRIMARY KEY, time_value TIME, datetime_value DATETIME, timestamp_value TIMESTAMP,
      #   timestamp_ltz_value TIMESTAMP_LTZ, timestamp_ntz_value TIMESTAMP_NTZ,
      #   timestamp_tz_value TIMESTAMP_TZ);
      # And then ran an insert:
      # INSERT INTO ruby_snowflake_client_testing.public.time_test
      #   (ID, time_value, datetime_value, timestamp_value, timestamp_ltz_value,
      #    timestamp_ntz_value, timestamp_tz_value)
      # values
      #  (1,
      #   '12:34:56',                      -- time_value
      #   '2022-01-01 12:34:56',           -- datetime_value
      #   '2022-01-01 12:34:56.123',       -- timestamp_value
      #   '2022-01-01 12:34:56.123 -7:00', -- timestamp_ltz_value
      #   '2022-01-01 12:34:56.123',       -- timestamp_ntz_value
      #   '2022-01-01 12:34:56.123 +9:00') -- timestamp_tz_value
      it "converts them into the correct ruby value" do
        row = client.query("SELECT * FROM ruby_snowflake_client_testing.public.time_test").first
        expect(row["time_value"].utc.iso8601).to eq "1970-01-01T12:34:56Z"
        expect(row["datetime_value"].utc.iso8601).to eq "2022-01-01T12:34:56Z"
        expect(row["timestamp_value"].utc.iso8601).to eq "2022-01-01T12:34:56Z"
        expect(row["timestamp_ntz_value"].utc.iso8601).to eq "2022-01-01T12:34:56Z"
        expect(row["timestamp_ltz_value"].utc.iso8601).to eq "2022-01-01T19:34:56Z"
        expect(row["timestamp_tz_value"].utc.iso8601).to eq "2022-01-01T03:34:56Z"
      end
    end

    context "with a large amount of data" do
      # We have setup a very simple table with the below statement:
      # CREATE TABLE ruby_snowflake_client_testing.public.large_table (ID int PRIMARY KEY, random_text string);
      # We than ran a couple of inserts with large number of rows:
      # INSERT INTO ruby_snowflake_client_testing.public.large_table
      #   SELECT random()%50000, randstr(64, random()) FROM table(generator(rowCount => 50000));

      let(:limit) { 0 }
      let(:query) { "SELECT * FROM ruby_snowflake_client_testing.public.large_table LIMIT #{limit}" }

      context "fetching 50k rows" do
        let(:limit) { 50_000 }
        it "should work" do
          rows = result.get_all_rows
          expect(rows.length).to eq 50000
          expect((-50000...50000)).to include(rows[0]["id"].to_i)
        end
      end

      context "with async (polling) responses" do
        before { client.instance_variable_set(:@_enable_polling_queries, true) }

        let(:limit) { 1_000 }
        it "should work" do
          rows = result.get_all_rows
          expect(rows.length).to eq 1000
          expect((-50000...50000)).to include(rows[0]["id"].to_i)
        end
      end

      context "fetching 150k rows x 20 times" do
        let(:limit) { 150_000 }
        it "should work" do
          20.times do |idx|
            client = described_class.from_env
            result = client.query(query)
            rows = result.get_all_rows
            expect(rows.length).to eq 150000
            expect((-50000...50000)).to include(rows[0]["id"].to_i)
          end
        end
      end

      context "fetching 50k rows x 5 times - with threads" do
        let(:limit) { 50_000 }

        before do
          ENV["SNOWFLAKE_MAX_CONNECTIONS"] = "12"
          ENV["SNOWFLAKE_MAX_THREADS_PER_QUERY"] = "12"
        end

        after do
          ENV["SNOWFLAKE_MAX_CONNECTIONS"] = nil
          ENV["SNOWFLAKE_MAX_THREADS_PER_QUERY"] = nil
        end
        it "should work" do
          t = []
          5.times do |idx|
            t << Thread.new do
              client = described_class.from_env
              result = client.query(query)
              rows = result.get_all_rows
              expect(rows.length).to eq 50_000
              expect((-50000...50000)).to include(rows[0]["id"].to_i)
            end
          end

          t.map(&:join)
        end
      end

      context "fetching 150k rows x 5 times - with threads & shared client" do
        let(:limit) { 150_000 }

        before { ENV["SNOWFLAKE_MAX_CONNECTIONS"] = "40" }
        after { ENV["SNOWFLAKE_MAX_CONNECTIONS"] = nil }

        it "should work" do
          t = []
          client = described_class.from_env
          5.times do |idx|
            t << Thread.new do
              result = client.query(query)
              rows = result.get_all_rows
              expect(rows.length).to eq 150000
              expect((-50000...50000)).to include(rows[0]["id"].to_i)
            end
          end

          t.map(&:join)
        end
      end

      context "with async (polling) responses" do
        before { client.instance_variable_set(:@_enable_polling_queries, true) }

        context "fetching 50k rows x 5 times - with threads & shared client" do
          let(:limit) { 50_000 }

          before { ENV["SNOWFLAKE_MAX_CONNECTIONS"] = "40" }
          after { ENV["SNOWFLAKE_MAX_CONNECTIONS"] = nil }

          it "should work" do
            t = []
            client = described_class.from_env
            5.times do |idx|
              t << Thread.new do
                result = client.query(query)
                rows = result.get_all_rows
                expect(rows.length).to eq 50_000
                expect((-50000...50000)).to include(rows[0]["id"].to_i)
              end
            end

            t.map(&:join)
          end
        end
      end

      context "fetching 150k rows x 10 times - with streaming" do
        let(:limit) { 150_000 }
        it "should work" do
          t = []
          10.times do |idx|
            t << Thread.new do
              client = described_class.from_env
              result = client.query(query)
              count = 0
              first_row = nil
              result.each do |row|
                first_row = row if first_row.nil?
                count += 1
              end
              expect(count).to eq 150000
              expect((-50000...50000)).to include(first_row["id"].to_i)
            end
          end

          t.map(&:join)
        end
      end
    end
  end

  shared_examples "a configuration setting" do |attribute, value, attr_reader_available|
    let!(:args) do
      { attribute => value}
    end

    it "supports configuring #{attribute} via from_env" do
      expect do
        new_client = described_class.from_env(**args)
        expect(new_client.send(attribute)).to eq(value) if attr_reader_available
      end.not_to raise_error
    end

    it "supports configuring #{attribute} via new" do
      expect do
        new_client = described_class.new("https://blah.snowflake",
                                         "MYPEMKEY",
                                         "MYORG",
                                         "ACCOUNT",
                                         "USER",
                                         "MYWAREHOUSE",
                                         "MYDB",
                                         **args)
        expect(new_client.send(attribute)).to eq(value) if attr_reader_available
      end.not_to raise_error
    end
  end

  describe "configuration" do
    it_behaves_like "a configuration setting", :logger, Logger.new(STDOUT)
    it_behaves_like "a configuration setting", :log_level, Logger::WARN, false
    it_behaves_like "a configuration setting", :jwt_token_ttl, 44
    it_behaves_like "a configuration setting", :connection_timeout, 42
    it_behaves_like "a configuration setting", :max_threads_per_query, 6
    it_behaves_like "a configuration setting", :thread_scale_factor, 5
    it_behaves_like "a configuration setting", :http_retries, 2
    it_behaves_like "a configuration setting", :query_timeout, 2000
    it_behaves_like "a configuration setting", :default_role, "OTHER_ROLE"


    context "with optional settings set through env variables" do
      before do
        ENV["SNOWFLAKE_JWT_TOKEN_TTL"] = "3333"
        ENV["SNOWFLAKE_CONNECTION_TIMEOUT"] = "33"
        ENV["SNOWFLAKE_MAX_CONNECTIONS"] = "33"
        ENV["SNOWFLAKE_MAX_THREADS_PER_QUERY"] = "33"
        ENV["SNOWFLAKE_THREAD_SCALE_FACTOR"] = "3"
        ENV["SNOWFLAKE_HTTP_RETRIES"] = "33"
        ENV["SNOWFLAKE_QUERY_TIMEOUT"] = "3333"
        ENV["SNOWFLAKE_DEFAULT_ROLE"] = "OTHER_ROLE"
      end

      after do
        ENV["SNOWFLAKE_JWT_TOKEN_TTL"] = nil
        ENV["SNOWFLAKE_CONNECTION_TIMEOUT"] = nil
        ENV["SNOWFLAKE_MAX_CONNECTIONS"] = nil
        ENV["SNOWFLAKE_MAX_THREADS_PER_QUERY"] = nil
        ENV["SNOWFLAKE_THREAD_SCALE_FACTOR"] = nil
        ENV["SNOWFLAKE_HTTP_RETRIES"] = nil
        ENV["SNOWFLAKE_QUERY_TIMEOUT"] = nil
        ENV["SNOWFLAKE_DEFAULT_ROLE"] = nil
      end

      it "sets the settings" do
        expect(client.instance_variable_get(:@key_pair_jwt_auth_manager).
                 instance_variable_get(:@jwt_token_ttl)).to eq 3333
        expect(client.connection_timeout).to eq 33
        expect(client.max_connections).to eq 33
        expect(client.max_threads_per_query).to eq 33
        expect(client.thread_scale_factor).to eq 3
        expect(client.http_retries).to eq 33
        expect(client.query_timeout).to eq 3333
        expect(client.default_role).to eq "OTHER_ROLE"
      end
    end

    context "no extra env settings are set" do
      it "sets the settings to defaults" do
        expect(client.instance_variable_get(:@key_pair_jwt_auth_manager).
                 instance_variable_get(:@jwt_token_ttl)
              ).to eq RubySnowflake::Client::DEFAULT_JWT_TOKEN_TTL
        expect(client.connection_timeout).to eq RubySnowflake::Client::DEFAULT_CONNECTION_TIMEOUT
        expect(client.max_connections).to eq RubySnowflake::Client::DEFAULT_MAX_CONNECTIONS
        expect(client.max_threads_per_query).to eq RubySnowflake::Client::DEFAULT_MAX_THREADS_PER_QUERY
        expect(client.thread_scale_factor).to eq RubySnowflake::Client::DEFAULT_THREAD_SCALE_FACTOR
        expect(client.http_retries).to eq RubySnowflake::Client::DEFAULT_HTTP_RETRIES
        expect(client.default_role).to be_nil
      end
    end
  end
end

require "spec_helper"

RSpec.describe RubySnowflake::Client do
  describe "#fetch" do
    let(:client) { described_class.connect }

    let(:query) { "" }
    let(:result) { client.query(query) }

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
            #binding.pry
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
            c = described_class.connect
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
        expect(result).to be_a(Result)
      end

      it "should respond to get_all_rows" do
        #rows = result.get_all_rows
        expect(result.length).to eq(1)
        rows = result.get_all_rows
        expect(rows).to eq(
          # The value should be an integer. TODO: figure out why it's not
          [{"1" => "1"}]
        )
      end

      # TODO: if we want this semantics. W/o streaming doesn't feel too great.
      #it "should respond to get_all_rows with a block" do
        #expect { |b| result.get_all_rows(&b) }.to yield_with_args({"1" => 1})
      #end
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
          "dob" => be_within(0.01).of(Time.new(1990, 10, 17,0,0,0, 0)),
          "created_at" => be_within(0.01).of(Time.new(2023,5,12,4,22,8,0)),
          "name" => "John Smith",
        }
      end
      let(:expected_jane) do
        {
          "coffes_per_week" => 3.525,
          "id" => 2,
          "dob" => be_within(0.01).of(Time.new(1990,1,9,0,0,0, 0)),
          "created_at" => be_within(0.01).of(Time.new(2023,5,12,4,22,8,0)),
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
          "bigfloat" => 8.25, #precision of only 2 decimals
        })
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

      context "fetching 150k rows x 100 times" do
        let(:limit) { 150_000 }
        it "should work" do
          100.times do |idx|
            client = described_class.connect
            result = client.query(query)
            rows = result.get_all_rows
            expect(rows.length).to eq 150000
            expect((-50000...50000)).to include(rows[0]["id"].to_i)
          end
        end
      end

      context "fetching 150k rows x 10 times - with threads" do
        let(:limit) { 150_000 }
        it "should work" do
          t = []
          10.times do |idx|
            t << Thread.new do
              client = described_class.connect
              result = client.query(query)
              rows = result.get_all_rows
              expect(rows.length).to eq 150000
              expect((-50000...50000)).to include(rows[0]["id"].to_i)
            end
          end

          t.map(&:join)
        end
      end

      context "fetching 150k rows x 10 times - with threads & shared client" do
        let(:limit) { 150_000 }
        it "should work" do
          t = []
          client = described_class.connect
          10.times do |idx|
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
    end
  end
end

# Ruby snowflake client using the v2 HTTP API

# Why this library?

The available options for connecting from Ruby to Snowflake include:
* ODBC - which works, but can be very slow, especially for a lot of data, which is probably why you're using Snowflake
* The [ruby snowflake client](https://github.com/rinsed-org/ruby-snowflake-client) that wraps the go client. This is probably the fastest single threaded option, which we also created. However, that library takes the ruby GVL and so stops all other processing in your ruby process (threads).

This library is implemented in ruby and while it leverages some libraries that have native extensions, doesn't currently include any native extensions itself. Depending on network latency and the shape of the data this library can be faster or slower than the go wrapper. The big advantages are:
* It uses about half the memory when you pull a full result set into memory
* It does not hold onto the [ruby GVL](https://www.speedshop.co/2020/05/11/the-ruby-gvl-and-scaling.html) and so does not block other threads while waiting on IO like the go wrapper client.
* It will consume more resources for the same data, because it's using the HTTP v2 API and getting JSON back, there is just more work to as compared to the go or python clients that use Apache Arrow under the covers.

# Usage

## Create a client

Add to your Gemfile or use `gem install rb_snowflake_client`
```ruby
  gem "rb_snowflake_client"
```

Then require, create a client
```ruby
require "rb_snowflake_client"


# uses env variables, you can also new one up
# see: https://github.com/rinsed-org/pure-ruby-snowflake-client/blob/master/lib/ruby_snowflake/client.rb#L43
client = RubySnowflake::Client.new(
  "https://yourinstance.region.snowflakecomputing.com", # insert your URL here
  File.read("secrets/my_key.pem"),                      # your private key in PEM format (scroll down for instructions)
  "snowflake-organization",                             # your account name (doesn't match your URL)
  "snowflake-account",                                  # typically your subdomain
  "snowflake-user",                                     # Your snowflake user
  "some_warehouse",                                     # The name of your warehouse to use by default
  "some_database",                                      # The name of the database in the context of which the queries will run
  default_role: "some_role",                            # The name of the role with which the queries will run. A `nil` value uses the primary role of the user.
  max_connections: 12,                                  # Config options can be passed in
  connection_timeout: 45,                               # See below for the full set of options
  query_timeout: 1200,                                  # how long to wait for queries, in seconds
)

# alternatively you can use the `from_env` method, which will pull these values from the following environment variables. You can either provide the path to the PEM file, or it's contents in an ENV variable.
RubySnowflake::Client.from_env
```
Available ENV variables (see below in the config section for details)
- `SNOWFLAKE_URI`
- `SNOWFLAKE_PRIVATE_KEY_PATH` or `SNOWFLAKE_PRIVATE_KEY`
  - Use either the key or the path. Key takes precedence if both are provided.
- `SNOWFLAKE_ORGANIZATION`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_DEFAULT_WAREHOUSE`
- `SNOWFLAKE_DEFAULT_DATABASE`
- `SNOWFLAKE_DEFAULT_ROLE`
- `SNOWFLAKE_JWT_TOKEN_TTL`
- `SNOWFLAKE_CONNECTION_TIMEOUT`
- `SNOWFLAKE_MAX_CONNECTIONS`
- `SNOWFLAKE_MAX_THREADS_PER_QUERY`
- `SNOWFLAKE_THREAD_SCALE_FACTOR`
- `SNOWFLAKE_HTTP_RETRIES`
- `SNOWFLAKE_QUERY_TIMEOUT`

## Make queries

Once you have a client, make queries
```ruby
# will get all data in memory
result = client.query("SELECT ID, NAME FROM SOMETABLE")

# result is Enumerable
result.each do |row|
  puts row[:id]    # row supports access with symbols
  puts row["name"] # or case insensitive strings
  puts row.to_h    # and can produce a hash with keys/values
end
```

## Stream results

You can also stream results and not hold them all in memory. The client will prefetch the next data partition only. If you have some IO in your processing there should usually be data available for you.

```ruby
result = client.query("SELECT * FROM HUGETABLE", streaming: true)
result.each do |row|
  puts row
end
```

## Switching databases

You can also overwrite the database specified in the initializer, and run your query with a different context.

```ruby
result = client.query("SELECT * FROM SECRET_TABLE", database: "OTHER_DB")
result.each do |row|
  puts row
end
```

## Switching warehouses

Clients are not warehouse specific, you can override the default warehouse per query

```ruby
client.query("SELECT * FROM BIGTABLE", warehouse: "FAST_WH")
```

## Specifying a schema

```ruby
client.query("SELECT * FROM BIGTABLE", schema: "MY_SCHEMA")
```

## Specifying role

Queries by default use the primary role assigned to the account. If there are multiple roles you can switch between them on a per query basis.

```ruby
client.query("SELECT * FROM BIGTABLE", role: "MY_ROLE")
```

## Binding parameters

Say we have `BIGTABLE` with a `data` column of a type `VARIANT`.

```ruby
json_string = '{"valid": "json"}'
query = "insert into BIGTABLE(data) select parse_json(?)"
bindings = {
  "1": {
    "type": "TEXT",
    "value": json_string
  }
}
client.query(query, bindings: bindings)
```

For additional information about binding parameters refer to snowflake documentation: https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#using-bind-variables-in-a-statement

# Configuration Options

The client supports the following configuration options, each with their own getter/setter except connection pool options which must be set at construction. Additionally, all except logger can be configured with environment variables (see above, but the pattern is like: "SNOWFLAKE_HTTP_RETRIES". Configuration options can only be set on initialization through `new` or `from_env`.

- `logger` - takes any ruby logger (by default it's a std lib Logger.new(STDOUT), set at DEBUG level. Not available as an ENV variable config option
- `log_level` - takes a log level, type is dependent on logger, for the default ruby Logger, use a level like `Logger::WARN`. Not available as an ENV variable config option.
- `jwt_token_ttl` - The time to live set on JWT token in seconds, defaults to 3540 (59 minutes, the longest Snowflake supports is 60).
- `connection_timeout` - The amount of time in seconds that the client's connection pool will wait before erroring in handing out a valid connection, defaults to 60 seconds
- `max_connections` - The maximum number of http connections to hold open in the connection pool. If you use the client in a threaded context, you may need to increase this to be threads * client.max_threads_per_query, defaults to 16.
- `max_threads_per_query` - The maximum number of threads the client should use to retrieve data, per query, defaults to 8. If you want the client to act in a single threaded way, set this to 1
- `thread_scale_factor` - When downloading a result set into memory, thread count is calculated by dividing a query's partition count by this number. For details on implementation see the code in `client.rb`.
- `http_retries` - By default the client will retry common typically transient errors (http responses) twice, you can change the number of retries with this.
- `query_timeout` - By default the client will wait 10 minutes (600s) for a query to finish, you can change this default, will also set this limit in the query for snowflake to obey. Set in seconds.

Example configuration:
```ruby
  client = RubySnowflake::Client.from_env(
    logger: Rails.logger
    max_connections: 24
    http_retries 1
  )
end
```

# Gotchas

1. Does not yet support multiple statements (work around is to wrap in `BEGIN ... END`)
2. Only supports key pair authentication
3. Its faster to work directly with the row value and not call to_h if you don't need to

# Setting up a user for key pair authentication

This library uses JWT to authenticate with the API which relies on key-pair authentication to connect to Snowflake.

1. Generate a private/public key pair for your user. Your private key will now be in a file `private_key.pem`. Keep this safe! Don't check it in to source control.
```bash
openssl genpkey -algorithm RSA -out private_key.pem -pkeyopt rsa_keygen_bits:2048
```
2. Generate a public key in the format that Snowflake likes (will produce `public_key.pem`)
```bash
openssl rsa -pubout -in private_key.pem -out public_key.pem
```
3. Your public_key.pem file should look something like this
```text
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAx8FaPusz9X9MCvv0h3N3
v1QaruyU1ivHs8jLjo6idzLSHJPGk7n3LSXerIw5/LkhfA27ibJj225/fKFnPy+X
gidbhE4BlvSdoVgdMH7WB1ZC3PpAwwqHeMisIzarwOwUu6mLyG9VY55ciKJY8CwA
5xt19pgVsXg/lcOa72jDjK+ExdSAN6K2TqSKqq77yzeI5creslny5VuAGTbZy3Bt
Wk0zg1xz8+C4regIOlSoFrzn1e4wHqbFv2zFFvORC2LV3HXFRaHYClB7jWRN1bFj
om6gRpiTO8bsCSPKi0anxMN8qt1Lw2d/+cwezxCwI6xPLC7JhZYdx6u+hC0g3PVK
PQIDAQAB
-----END PUBLIC KEY-----
```
Snowflake doesn't like it in that format, but openssl can remove the newlines and begining and ending for you:
```bash
openssl rsa -pubin -in public_key.pem -outform DER | openssl base64 -A
```
(if it spits out a % at the end, remove that).
```text
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArOL5WQYaXSnVhQXQZQHVIzrNt08A+bnGjBb6DWFVRao3dlPG+HOf9Nv0nGlk8m5AMvvETUnN3tihuRHOJ9MOUzDp58IYIr5xvOENSunbRVyJL7DuCGwZz8z1pEnlBjZPONzEX8dCKxCU0neJrksFgwdhfhIUs7GnbTuJjYP9EqXPlbsYNYTVVnFNZ9DHFur9PggPJpPHTfFDz8MEB3Xb3AWV3pE752ed/PtRcTODvgoQSpP80cTgsKjsG009NY2ulEtV3r7yNJgawxmcMTNLhFlSS7Wm2NSEIS0aNo+DgSZI72MnAOw2klUzvdBl0i43gI+aX0Y6y/y18VL1o9KMQwIDAQAB
```
4. Now, in the snowflake web console or through your favorite client, log in as a user with permissions to edit users. For your particular user (`EXAMPLE_USER` below) update the user with the modified public key from above:
```sql
ALTER USER EXAMPLE_USER SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArOL5WQYaXSnVhQXQZQHVIzrNt08A+bnGjBb6DWFVRao3dlPG+HOf9Nv0nGlk8m5AMvvETUnN3tihuRHOJ9MOUzDp58IYIr5xvOENSunbRVyJL7DuCGwZz8z1pEnlBjZPONzEX8dCKxCU0neJrksFgwdhfhIUs7GnbTuJjYP9EqXPlbsYNYTVVnFNZ9DHFur9PggPJpPHTfFDz8MEB3Xb3AWV3pE752ed/PtRcTODvgoQSpP80cTgsKjsG009NY2ulEtV3r7yNJgawxmcMTNLhFlSS7Wm2NSEIS0aNo+DgSZI72MnAOw2klUzvdBl0i43gI+aX0Y6y/y18VL1o9KMQwIDAQAB'
```
5. Verify your auth setup. If you have `snowsql` installed, that has an easy method (CTRL-d to exit)
```bash
# example: snowsql -a AAAAAAA.BBBBBBBB.us-east-1 -u john --private-key-path private_key.pem
snowsql -a <account_identifier>.<region> -u <user> --private-key-path private_key.pem
```
or alternatively, use the client to verify:
```ruby
client = RubySnowflake::Client.new(
  "https://yourinstance.region.snowflakecomputing.com", # insert your URL here
  File.read("secrets/my_key.pem"),                      # path to your private key
  "snowflake-organization",                             # your account name (doesn't match your URL)
  "snowflake-account",                                  # typically your subdomain
  "snowflake-user",                                     # Your snowflake user
  "some_warehouse",                                     # The name of your warehouse to use by default
  "some_database",                                      # The name of the database in the context of which the queries will run
)
```

# Code of conduct

See [Code of Coduct](CODE_OF_CONDUCT.md)

# Contributing

Please fork and create a pull request. Getting tests to run will be the most labor intensive part. You'll want to have an active snowflake account, and then configure your `.env` for the tests to be able to connect to your instance. Inside of `client_spec.rb` there are SQL statements to create the required tables. A pull request to make this less manual is also welcome :-). We'll do our best to help you along. Also, feel free to use Issues to report issues. [We](https://rinsed.co) use this client in production today, so we're happy to look at issues, especially where performance or correctness is involved.

# Links:
- snowflake API reference https://docs.snowflake.com/en/developer-guide/sql-api/reference
- snowflake authentication docs: https://docs.snowflake.com/en/developer-guide/sql-api/authenticating

# Ruby snowflake client using the v2 HTTP API

# Usage

Add to your Gemfile or use `gem install rb-snowflake-client`
```ruby
  gem "rb-snowflake-client"
```

Then require, create a client and query
```ruby
require "rb_snowflake_client"

client = RubySnowflake::Client.connect # uses env variables, you can also new one up

# will get all data in memory
result = client.query("SELECT ID, NAME FROM SOMETABLE")

# result is Enumerable
result.each do |row|
  puts row[:id]    # row supports access with symbols
  puts row["name"] # or case insensitive strings
  puts row.to_h    # and can produce a hash with keys/values
end

# query supports multiple statements
result = client.query("SELECT 1; SELECT ID FROM MYTABLE")

# odds are you have alot of data in snowflake, you can also stream results
# and avoid pulling them all into memory. The client will prefetch the next
# data partition for you. If you have some IO in your processing there should
# always be data available for you.
result = client.query("SELECT * FROM HUGETABLE", streaming: true)
result.each do |row|
  puts row
end
```

# Keypair auth info

1. I've checked in a key pair that we'll need to delete soon, shouldn't ever really do that.
2. To generate a working pair:
   `openssl genpkey -algorithm RSA -out private_key.pem -pkeyopt rsa_keygen_bits:2048`
   JWT works with pem format:
   `openssl rsa -pubout -in private_key.pem -out public_key.pem`
3. Then you need to alter the user you want to auth with by setting their public key through an ALTER command (PEM format)
   Then, take the SHA256 fingerprint it generates on the user and use that as a parameter for the client.
4. All of this is hardcoded for a user with too many permissions right now. Will fix ASAP next week.

# Links:
- snowflake API reference https://docs.snowflake.com/en/developer-guide/sql-api/reference
- snowflake authentication docs: https://docs.snowflake.com/en/developer-guide/sql-api/authenticating

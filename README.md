# Experimental pure ruby snowflake client using the v2 HTTP API

# Links:
- snowflake API reference https://docs.snowflake.com/en/developer-guide/sql-api/reference
- snowflake authentication docs: https://docs.snowflake.com/en/developer-guide/sql-api/authenticating


# Usage
```bash
bundle install
bundle exec ruby test.rb
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

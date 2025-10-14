# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [1.5.0] - 2025-10-14
### Added
- Instrumentation feature added for Active Support users
- Added `query_timeout` as a per-query parameter, allowing timeout override on individual queries
### Fixed
- `query_timeout` now properly sends timeout parameter to Snowflake API for server-side enforcement
- Streaming mode now releases consumed records, fixing memory leak. Note: if you were iterating over streaming results more than once, this is a breaking change (though that was not its intended usage).

## [1.4.0] - 2025-05-01
### Added
- Enhanced Row API to implement Enumerable interface
- Added case-insensitive access to Row columns via both symbol and string keys
- Added numeric column access to Row (e.g., `row[0]`)
- Support setting organization or ENV["SNOWFLAKE_ORGANIZATION"] to nil or "" in JWT authentication
- Added default_role parameter and SNOWFLAKE_DEFAULT_ROLE env variable

## [1.3.0] - 2025-01-03
### Changed
- Bumped gem dependencies to newer versions
- Added support for role parameter in Client and query method

## [1.2.0] - 2025-01-03
### Changed
- Switched from Oj to JSON gem for parsing
- Improved performance by utilizing the optimized JSON gem

## [1.1.5] - 2024-12-19
### Fixed
- Parse exception detail OR message for better error handling

## [1.1.4] - 2024-11-05
### Fixed
- Fixed ENV variable issue

## [1.1.3] - 2024-08-09
### Added
- Retry HTTP codes in the 3xx range

## [1.1.2] - 2024-08-06
### Fixed
- CI error fixes

## [1.1.1] - 2024-07-12
### Fixed
- Added 502 to specific list of retryable HTTP error codes
- Fixed issue with checking string code presence in an array of integer values

## [1.1.0] - 2024-06-05
### Added
- Support for specifying a schema in query method
- Merged multiple community contributions

## [1.0.6] - 2024-06-05
### Added
- Allow specifying schema in query method

## [1.0.5] - 2024-03-20
### Added
- Added exponential backoff to retryable calls
- Improved handling of rate limiting (429 responses)

## [1.0.4] - 2024-01-30
### Fixed
- Fixed raise arguments
- Now properly raising OpenSSL errors to retry them

## [1.0.3] - 2024-01-17
### Fixed
- Now upcasing database and warehouse fields in requests
- Fixed error where lowercase field names would result in "Unable to run command without specifying database/warehouse"

## [1.0.2] - 2024-01-16
### Fixed
- Fixed typo in key pair memoization

## [1.0.1] - 2024-01-09
### Added
- Added `create_jwt_token` helper method for testing
- Support for time travel in tests

## [1.0.0] - 2023-12-11
### Changed
- First stable release
- Fixed markdown links in documentation

## [0.3.0] - 2023-12-08
### Added
- Support for Snowflake polling responses
- Handle async query execution

## [0.2.0] - 2023-12-07
### Added
- Extracted authentication logic into its own class
- Improved time handling for various Snowflake date/time types
- Support for TIME, DATETIME, TIMESTAMP, TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ

## [0.1.2] - 2023-12-04
### Added
- Support for database parameter in requests
- Added missing dependencies to gemspec

## [0.1.1] - 2023-12-01
### Added
- Added `fetch` as an alias for `query` for compatibility with other clients

## [0.1.0] - 2023-11-28
### Added
- First minor version release with basic functionality
- Support for querying Snowflake with the HTTP API
- Support for streaming results

## [0.0.6] - 2023-11-27
### Changed
- Cleaned up key pair authentication
- Improved documentation with better setup instructions

## [0.0.5] - 2023-11-27
### Fixed
- Various bug fixes and improvements

## [0.0.4] - 2023-11-22
### Changed
- Fixed type handling for query results
- All specs now pass

## [0.0.3] - 2023-11-21
### Changed
- Renamed to RubySnowflake namespace
- Initial gem structure

# frozen_string_literal: true

require "date"
require "time"

module RubySnowflake
  class Row
    include Enumerable

    EPOCH_JULIAN_DAY_NUMBER = Date.new(1970,1,1).jd
    TIME_FORMAT = "%s.%N".freeze

    def initialize(row_types, column_to_index, data)
      @row_types = row_types
      @data = data
      @column_to_index = column_to_index
    end

    # see: https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses#getting-the-data-from-the-results
    def [](column)
      index = column.is_a?(Numeric) ? Integer(column) : @column_to_index[column.to_sym.downcase]

      return nil if index.nil?
      return nil if @data[index].nil?

      case @row_types[index][:type]
      when :boolean
        @data[index] == "true"
      when :date
        Date.jd(Integer(@data[index]) + EPOCH_JULIAN_DAY_NUMBER)
      when :fixed
        if @row_types[index][:scale] == 0
          Integer(@data[index])
        else
          BigDecimal(@data[index]).round(@row_types[index][:scale])
        end

      # snowflake treats these all as 64 bit IEEE 754 floating point numbers, and will we too
      when :float, :double, :"double precision", :real
        Float(@data[index])

      # Despite snowflake indicating that it sends the offset in minutes, the actual time in UTC
      # is always sent in the first half of the data. If an offset is sent it looks like:
      #   "1641008096.123000000 1980"
      # If there isn't one, it's just like this:
      #   "1641065696.123000000"
      # in all cases, the actual time, in UTC is the float value, and the offset is ignorable
      when :time, :datetime, :timestamp, :timestamp_ntz, :timestamp_ltz, :timestamp_tz
        Time.strptime(@data[index], TIME_FORMAT).utc
      else
        @data[index]
      end
    end

    def each
      return to_enum __method__ unless block_given?

      @column_to_index.each_pair do |name, index|
        yield(output[name], self[index])
      end

      self
    end

    def keys
      map { |k, _| k }
    end

    alias columns keys

    def values
      map { |_, v| v }
    end

    def to_s
      to_h.to_s
    end
  end
end

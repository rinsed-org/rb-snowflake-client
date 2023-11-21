# frozen_string_literal: true

require "date"
require "time"

module RubySnowflake
  class Row
    EPOC_JULIAN_DAY_NUMBER = Date.new(1970,1,1).jd

    def initialize(row_types, column_to_index, data)
      @row_types = row_types
      @data = data
      @column_to_index = column_to_index
    end

    # see: https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses#getting-the-data-from-the-results
    def [](column)
      index = column.is_a?(Numeric) ? column.to_i : @column_to_index[column]
      return nil if index.nil?

      # TODO: double check these timestamp conversions, I'm pretty sure they're wrong.
      case @row_types[index]
      when :time, :timestamp_ltz, :timestamp_ntz
        Time.at(@data[index].to_f)
      when :timestamp_tz
        timestamp, offset_minutes = @data[index].split(" ")
        Time.at(timestamp.to_f - offset_minutes.to_i * 60)
      when :boolean
        @data[index] == "true"
      when :date # TODO looks like we might be returning a Time in the go version
        @data[index] = Date.jd(@data[index].to_i + EPOC_JULIAN_DAY_NUMBER)
      when :real
        @data[index] = @data[index].to_f
      else
        @data[index]
      end
    end

    def to_h
      output = IndifferentCaseInsensitiveHash.new
      @column_to_index.each_pair do |name, index|
        output[name] = self[index]
      end
      output
    end

    def to_s
      to_h.to_s
    end
  end
end

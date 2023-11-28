# frozen_string_literal: true

require "date"
require "time"

module RubySnowflake
  class Row
    EPOCH_JULIAN_DAY_NUMBER = Date.new(1970,1,1).jd

    def initialize(row_types, column_to_index, data)
      @row_types = row_types
      @data = data
      @column_to_index = column_to_index
    end

    # see: https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses#getting-the-data-from-the-results
    def [](column)
      index = column.is_a?(Numeric) ? Integer(column) : @column_to_index[column]
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
      when :float, :double, :"double precision", :real
        # snowflake treats these all as 64 bit IEEE 754 floating point numbers, and will we too
        Float(@data[index])
      when :time, :datetime, :timestamp, :timestamp_ltz, :timestamp_ntz
        Time.at(BigDecimal(@data[index])).utc
      when :timestamp_tz
        timestamp, offset_minutes = @data[index].split(" ")
        Time.at(BigDecimal(timestamp) - Integer(offset_minutes) * 60)
      else
        @data[index]
      end
    end

    def to_h
      output = {}
      @column_to_index.each_pair do |name, index|
        output[name.downcase] = self[index]
      end
      output
    end

    def to_s
      to_h.to_s
    end
  end
end

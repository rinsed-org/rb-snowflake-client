# frozen_string_literal: true

require "concurrent"

require_relative "indifferent_case_insensitive_hash"
require_relative "row"

module RubySnowflake
  class Result
    include Enumerable

    attr_reader :data

    def initialize(partition_count, row_type_data)
      @data = Concurrent::Array.new(partition_count)
      extract_row_metadata(row_type_data)
    end

    def []=(index, value)
      data[index] = value
    end

    def get_all_rows
      map(&:to_h)
    end

    def each
      return to_enum(:each) unless block_given?

      data.each do |partition|
        partition.each do |row|
          yield wrap_row(row)
        end
      end
    end

    def size
      data.map(&:size).sum
    end

    alias length size

    def first
      wrap_row(data.first.first)
    end

    def last
      wrap_row(data.last.last)
    end

    private
      def wrap_row(row)
        Row.new(@row_types, @column_to_index, row)
      end

      def extract_row_metadata(row_type_data)
        @row_types = []
        @column_to_index = IndifferentCaseInsensitiveHash.new

        row_type_data.each_with_index do |type_data, index|
          @row_types[index] = type_data["type"].downcase.to_sym
          @column_to_index[type_data["name"]] = index
        end
      end
  end
end

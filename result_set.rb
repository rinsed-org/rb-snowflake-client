# frozen_string_literal: true

require "concurrent"

class ResultSet
  include Enumerable

  attr_reader :data

  def initialize(partition_count)
    @data = Concurrent::Array.new(partition_count)
  end

  def []=(index, value)
    data[index] = value
  end

  def each
    return to_enum(:each) unless block_given?

    data.each do |partition|
      partition.each do |row|
        yield row
      end
    end
  end

  def size
    data.map(&:size).sum
  end
end

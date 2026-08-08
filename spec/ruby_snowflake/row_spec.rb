require "spec_helper"

RSpec.describe RubySnowflake::Row do
  describe "#dig" do
    it "digs into nested hashes" do
      nested_row_types = [{type: :text, scale: 0, precision: 0, name: :data}]
      nested_column_to_index = {"data" => 0}
      nested_data = [{"user" => {"name" => "Bob", "age" => 25}}]
      nested_row = described_class.new(nested_row_types, nested_column_to_index, nested_data)

      expect(nested_row.dig("data", "user", "name")).to eq("Bob")
      expect(nested_row.dig("data", "user", "age")).to eq(25)
      expect(nested_row.dig("data", "missing", "key")).to be_nil
    end

    it "digs into nested arrays" do
      array_row_types = [{type: :text, scale: 0, precision: 0, name: :items}]
      array_column_to_index = {"items" => 0}
      array_data = [[{"id" => 1}, {"id" => 2}]]
      array_row = described_class.new(array_row_types, array_column_to_index, array_data)

      expect(array_row.dig("items", 0, "id")).to eq(1)
      expect(array_row.dig("items", 1, "id")).to eq(2)
    end
  end

  describe "#fetch" do
    let(:row_types) do
      [
        {type: :text, scale: 0, precision: 0, name: :name},
        {type: :fixed, scale: 0, precision: 0, name: :age}
      ]
    end
    let(:column_to_index) { {"name" => 0, "age" => 1} }
    let(:data) { ["Alice", "30"] }
    let(:row) { described_class.new(row_types, column_to_index, data) }

    it "returns value for existing key" do
      expect(row.fetch("name")).to eq("Alice")
      expect(row.fetch(:age)).to eq(30)
    end

    it "returns default value for missing key" do
      expect(row.fetch("missing", "default")).to eq("default")
      expect(row.fetch(:nonexistent, 0)).to eq(0)
    end

    it "yields to block for missing key" do
      expect(row.fetch("missing") { |k| "no #{k}" }).to eq("no missing")
      expect(row.fetch(:missing) { |k| "key: #{k}" }).to eq("key: missing")
    end

    it "raises KeyError for missing key without default or block" do
      expect { row.fetch("missing") }.to raise_error(KeyError, /key not found/)
      expect { row.fetch(:nonexistent) }.to raise_error(KeyError, /key not found/)
    end
  end

  describe "#key?" do
    let(:row_types) do
      [
        {type: :text, scale: 0, precision: 0, name: :name},
        {type: :fixed, scale: 0, precision: 0, name: :age}
      ]
    end
    let(:column_to_index) { {"name" => 0, "age" => 1} }
    let(:data) { ["Alice", "30"] }
    let(:row) { described_class.new(row_types, column_to_index, data) }

    it "returns true for existing column" do
      expect(row.key?("name")).to be true
      expect(row.key?(:age)).to be true
    end

    it "returns false for non-existing column" do
      expect(row.key?("missing")).to be false
      expect(row.key?(:nonexistent)).to be false
    end

    it "is case-insensitive" do
      expect(row.key?("NAME")).to be true
      expect(row.key?("Name")).to be true
    end

    it "works with has_key? alias" do
      expect(row.has_key?("name")).to be true
      expect(row.has_key?("missing")).to be false
    end
  end
end

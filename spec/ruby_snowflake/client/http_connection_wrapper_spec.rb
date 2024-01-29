require "spec_helper"

RSpec.describe RubySnowflake::Client::HttpConnectionWrapper do
  let(:hostname) { "example.com" }
  let(:port) { 443 }
  let(:wrapper) { described_class.new(hostname, port) }

  describe "#start" do
    context "when the connection is successful" do
      it "returns itself" do
        allow(Net::HTTP).to receive(:start).with(hostname, port, use_ssl: true).and_return(double("HTTP Connection"))
        expect(wrapper.start).to eq(wrapper)
      end
    end

    context "when the connection fails with OpenSSL::SSL::SSLError" do
      it "propagates OpenSSL::SSL::SSLError" do
        allow(Net::HTTP).to receive(:start).with(hostname, port, use_ssl: true).and_raise(OpenSSL::SSL::SSLError)
        expect { wrapper.start }.to raise_error(OpenSSL::SSL::SSLError)
      end
    end

    context "when the connection fails with another StandardError" do
      it "raises a ConnectionError" do
        allow(Net::HTTP).to receive(:start).with(hostname, port, use_ssl: true).and_raise(StandardError)
        expect { wrapper.start }.to raise_error(RubySnowflake::ConnectionError)
      end
    end
  end

  describe "#request" do
    let(:request) { double("HTTP Request") }
    let(:response) { double("HTTP Response") }
    let(:connection_double) { double("HTTP Connection", active?: true, request: response) }

    before do
      allow(wrapper).to receive(:connection).and_return(connection_double)
    end

    context "when the request is successful" do
      it "returns the response" do
        allow(connection_double).to receive(:request).with(request).and_return(response)
        expect(wrapper.request(request)).to eq(response)
      end
    end

    context "when there is an OpenSSL::SSL::SSLError" do
      it "propagates the OpenSSL::SSL::SSLError" do
        allow(connection_double).to receive(:request).with(request).and_raise(OpenSSL::SSL::SSLError)
        expect { wrapper.request(request) }.to raise_error(OpenSSL::SSL::SSLError)
      end
    end

    context "when there is another StandardError" do
      it "raises a RequestError" do
        allow(connection_double).to receive(:request).with(request).and_raise(StandardError)
        expect { wrapper.request(request) }.to raise_error(RubySnowflake::RequestError)
      end
    end
  end
end

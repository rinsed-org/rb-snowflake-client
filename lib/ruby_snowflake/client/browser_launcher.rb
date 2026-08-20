# frozen_string_literal: true

require "rbconfig"

module RubySnowflake
  class Client
    class BrowserLauncher
      def self.open(url)
        new.open(url)
      end

      def open(url)
        case RbConfig::CONFIG["host_os"]
        when /darwin|mac os/i
          success = system("open", url, err: File::NULL)
        when /linux|bsd/i
          success = system("xdg-open", url, err: File::NULL)
        when /mswin|mingw|cygwin/i
          success = system("start", '""', url, err: File::NULL)
        else
          raise BrowserLaunchError.new("Unsupported platform for browser launch: #{RbConfig::CONFIG["host_os"]}")
        end

        raise BrowserLaunchError.new("Failed to open browser with URL: #{url}") unless success
        true
      end
    end
  end
end

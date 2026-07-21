# frozen_string_literal: true

require 'simplecov'

SimpleCov.start do
  enable_coverage :branch
  primary_coverage :line
  skip '/spec/'
  skip '/benchmark/'
  group 'Library', 'lib'
  group 'CLI', 'bin'
  minimum_coverage line: 100, branch: 100
end

require 'bundler/setup'
require 'purplelight'
require 'logger'

# Silence noisy Mongo driver warnings during tests
Mongo::Logger.logger.level = Logger::FATAL if defined?(Mongo::Logger) && Mongo::Logger.respond_to?(:logger) && Mongo::Logger.logger

# Compression backends: prefer zstd-ruby, then zstds; require at most once
begin
  require 'zstd-ruby'
rescue LoadError
  begin
    require 'zstds'
  rescue LoadError
    # Neither zstd backend available; tests will fall back to gzip where needed
  end
end

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

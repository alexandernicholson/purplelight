# frozen_string_literal: true

source 'https://rubygems.org'

git_source(:github) { |repo| "https://github.com/#{repo}.git" }

gemspec

group :compression do
  # Uncomment one of the following for zstd compression support
  # gem "ruby-zstds", "~> 1.3.1" # Provides ZSTDS namespace
  gem 'zstd-ruby', '~> 2.0'
end

group :parquet do
  # Optional: Apache Arrow + Parquet support
  gem 'red-arrow', '>= 25.0'
  gem 'red-parquet', '>= 25.0'
end

# Linting (development)

# Test/dev tools
group :development, :test do
  gem 'benchmark', '>= 0.5'
  gem 'parallel', '< 2' # parallel 2.x requires Ruby 3.3; purplelight supports Ruby 3.2
  gem 'rake', '>= 13.0'
  gem 'rspec', '>= 3.12'
  gem 'rubocop', require: false
  gem 'simplecov', '>= 0.22', require: false
  gem 'stackprof', '~> 0.2.28', require: false
end

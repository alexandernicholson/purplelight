# frozen_string_literal: true

source 'https://rubygems.org'

git_source(:github) { |repo| "https://github.com/#{repo}.git" }

gemspec

group :compression do
  # Uncomment one of the following for zstd compression support
  # gem "ruby-zstds", "~> 1.3.1" # Provides ZSTDS namespace
  gem 'zstd-ruby', '~> 1.5'
end

group :parquet do
  # Optional: Apache Arrow + Parquet support
  gem 'red-arrow', '>= 21.0'
  gem 'red-parquet', '>= 21.0'
end

# Linting (development)

# Test/dev tools
group :development, :test do
  gem 'rake', '>= 13.0'
  gem 'rspec', '>= 3.12'
  gem 'rubocop', require: false
end

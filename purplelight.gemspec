# frozen_string_literal: true

require_relative 'lib/purplelight/version'

Gem::Specification.new do |spec|
  spec.name          = 'purplelight'
  spec.version       = Purplelight::VERSION
  spec.authors       = ['Alexander Nicholson']
  spec.email         = ['rubygems-maint@ctrl.tokyo']

  spec.summary       = 'Snapshot MongoDB collections efficiently to JSONL/CSV/Parquet'
  spec.description   = 'High-throughput, resumable snapshots of MongoDB collections with partitioning, multi-threaded readers, and size-based sharded outputs.'
  spec.license       = 'MIT'

  spec.required_ruby_version = '>= 3.2'
  spec.metadata['rubygems_mfa_required'] = 'true'

  spec.metadata['homepage_uri'] = 'https://github.com/alexandernicholson/purplelight'
  spec.metadata['source_code_uri'] = 'https://github.com/alexandernicholson/purplelight'
  spec.metadata['changelog_uri'] = 'https://github.com/alexandernicholson/purplelight/releases'

  spec.files = Dir.chdir(__dir__) do
    Dir['lib/**/*.rb', 'bin/*', 'README.md', 'LICENSE', 'Rakefile']
  end
  spec.bindir        = 'bin'
  spec.executables   = ['purplelight']

  # Runtime deps
  spec.add_dependency 'csv'
  spec.add_dependency 'logger', '>= 1.6'
  spec.add_dependency 'mongo', '>= 2.21.3'
  # zstd compression is optional; if the zstd gem is not installed, we fallback to gzip.
  # Supported gems include 'ruby-zstds' (provides ZSTDS) or 'zstd-ruby'. We don't hard-depend to keep install light.

  # Dev deps are declared in Gemfile
end

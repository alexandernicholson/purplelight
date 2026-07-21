# frozen_string_literal: true

require 'bson'
require 'json'
require 'time'
require 'securerandom'
require 'digest'
require 'fileutils'

module Purplelight
  # Manifest persists snapshot run metadata and progress to a JSON file.
  #
  # It records configuration, partition checkpoints, and per-part byte/row
  # counts so interrupted runs can resume safely and completed runs are
  # reproducible. Methods are thread-safe where mutation occurs.
  class Manifest
    DEFAULT_VERSION = 2

    attr_reader :path, :data

    def self.query_digest(query, projection)
      payload = { query: query, projection: projection }
      Digest::SHA256.hexdigest(JSON.generate(payload))
    end

    def initialize(path:, data: nil)
      @path = path
      @directory = File.dirname(path)
      @temporary_path = "#{path}.tmp"
      FileUtils.mkdir_p(@directory)
      @data = data || {
        'version' => DEFAULT_VERSION,
        'run_id' => SecureRandom.uuid,
        'created_at' => Time.now.utc.iso8601,
        'collection' => nil,
        'format' => nil,
        'compression' => nil,
        'query_digest' => nil,
        'options' => {},
        'partition_filters' => nil,
        'parts' => [],
        'partitions' => []
      }
      @mutex = Mutex.new
      @last_save_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    def self.load(path)
      data = JSON.parse(File.read(path))
      new(path: path, data: data)
    end

    def save!
      File.write(@temporary_path, JSON.pretty_generate(@data))
      File.rename(@temporary_path, path)
    end

    def configure!(collection:, format:, compression:, query_digest:, options: {}, partition_count: nil)
      @mutex.synchronize do
        @data['collection'] = collection
        @data['format'] = format.to_s
        @data['compression'] = compression.to_s
        @data['query_digest'] = query_digest
        @data['options'] = options
        normalize_partition_data!(partition_count) if partition_count
        save!
      end
    end

    def compatible_with?(collection:, format:, compression:, query_digest:)
      return false unless @data['version'] == DEFAULT_VERSION

      @data['collection'] == collection &&
        @data['format'] == format.to_s &&
        @data['compression'] == compression.to_s &&
        @data['query_digest'] == query_digest
    end

    def ensure_partitions!(count)
      @mutex.synchronize do
        if @data['partitions'].empty?
          normalize_partition_data!(count)
          save!
        end
      end
    end

    def update_partition_checkpoint!(index, last_id_exclusive)
      @mutex.synchronize do
        part = @data['partitions'][index]
        part['last_id_exclusive'] = serialize_checkpoint(last_id_exclusive)
        save_maybe!
      end
    end

    def mark_partition_complete!(index)
      @mutex.synchronize do
        part = @data['partitions'][index]
        part['completed'] = true
        save!
      end
    end

    def open_part!(path)
      @mutex.synchronize do
        idx = @data['parts'].size
        @data['parts'] << { 'index' => idx, 'path' => path, 'bytes' => 0, 'rows' => 0, 'complete' => false,
                            'checksum' => nil }
        save!
        idx
      end
    end

    def add_progress_to_part!(index:, rows_delta:, bytes_delta:)
      @mutex.synchronize do
        part = @data['parts'][index]
        part['rows'] += rows_delta
        part['bytes'] += bytes_delta
        save_maybe!
      end
    end

    def complete_part!(index:, checksum: nil)
      @mutex.synchronize do
        part = @data['parts'][index]
        part['complete'] = true
        part['checksum'] = checksum
        save!
      end
    end

    def parts
      @data['parts']
    end

    def partitions
      @data['partitions']
    end

    def partition_checkpoint(index)
      raw_checkpoint = @data['partitions'][index]&.fetch('last_id_exclusive', nil)
      BSON::ExtJSON.parse_obj(raw_checkpoint) if raw_checkpoint
    end

    def partition_filters
      raw_filters = @data['partition_filters']
      return unless raw_filters

      BSON::ExtJSON.parse_obj(raw_filters).map do |filter_spec|
        {
          filter: filter_spec[:filter] || filter_spec['filter'],
          sort: filter_spec[:sort] || filter_spec['sort'],
          hint: filter_spec[:hint] || filter_spec['hint']
        }
      end
    end

    def configure_partition_filters!(filters)
      @mutex.synchronize do
        @data['partition_filters'] = filters.as_extended_json
      end
    end

    private

    def serialize_checkpoint(value)
      case value
      when nil, String, Numeric, true, false
        value
      else
        value.as_extended_json
      end
    end

    def normalize_partition_data!(count)
      return unless @data['partitions'].empty?

      @data['partitions'] = Array.new(count) do |index|
        { 'index' => index, 'last_id_exclusive' => nil, 'completed' => false }
      end
    end

    def save_maybe!(interval_seconds: 2.0)
      now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      return unless (now - @last_save_at) >= interval_seconds

      save!
      @last_save_at = now
    end
  end
end

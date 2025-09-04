# frozen_string_literal: true

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
    DEFAULT_VERSION = 1

    attr_reader :path, :data

    def self.query_digest(query, projection)
      payload = { query: query, projection: projection }
      Digest::SHA256.hexdigest(JSON.generate(payload))
    end

    def initialize(path:, data: nil)
      @path = path
      @data = data || {
        'version' => DEFAULT_VERSION,
        'run_id' => SecureRandom.uuid,
        'created_at' => Time.now.utc.iso8601,
        'collection' => nil,
        'format' => nil,
        'compression' => nil,
        'query_digest' => nil,
        'options' => {},
        'parts' => [],
        'partitions' => []
      }
      @mutex = Mutex.new
      @last_save_at = Time.now
    end

    def self.load(path)
      data = JSON.parse(File.read(path))
      new(path: path, data: data)
    end

    def save!
      dir = File.dirname(path)
      FileUtils.mkdir_p(dir)
      tmp = "#{path}.tmp"
      File.write(tmp, JSON.pretty_generate(@data))
      FileUtils.mv(tmp, path)
    end

    def configure!(collection:, format:, compression:, query_digest:, options: {})
      @data['collection'] = collection
      @data['format'] = format.to_s
      @data['compression'] = compression.to_s
      @data['query_digest'] = query_digest
      @data['options'] = options
      save!
    end

    def compatible_with?(collection:, format:, compression:, query_digest:)
      @data['collection'] == collection &&
        @data['format'] == format.to_s &&
        @data['compression'] == compression.to_s &&
        @data['query_digest'] == query_digest
    end

    def ensure_partitions!(count)
      @mutex.synchronize do
        if @data['partitions'].empty?
          @data['partitions'] = Array.new(count) do |i|
            { 'index' => i, 'last_id_exclusive' => nil, 'completed' => false }
          end
          save!
        end
      end
    end

    def update_partition_checkpoint!(index, last_id_exclusive)
      @mutex.synchronize do
        part = @data['partitions'][index]
        part['last_id_exclusive'] = last_id_exclusive
        save!
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

    private

    def save_maybe!(interval_seconds: 2.0)
      now = Time.now
      return unless (now - @last_save_at) >= interval_seconds

      save!
      @last_save_at = now
    end
  end
end

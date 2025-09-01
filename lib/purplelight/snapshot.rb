# frozen_string_literal: true

require 'mongo'
require 'etc'
require 'fileutils'
require_relative 'partitioner'
require_relative 'queue'
require_relative 'writer_jsonl'
require_relative 'writer_csv'
require_relative 'manifest'
require_relative 'errors'

module Purplelight
  class Snapshot
    DEFAULTS = {
      format: :jsonl,
      compression: :zstd,
      batch_size: 2_000,
      partitions: [Etc.respond_to?(:nprocessors) ? [Etc.nprocessors * 2, 4].max : 4, 32].min,
      queue_size_bytes: 128 * 1024 * 1024,
      rotate_bytes: 256 * 1024 * 1024,
      read_concern: { level: :majority },
      read_preference: :primary,
      no_cursor_timeout: true
    }

    def self.snapshot(**options)
      new(**options).run
    end

    def initialize(client:, collection:, output:, format: DEFAULTS[:format], compression: DEFAULTS[:compression],
                   partitions: DEFAULTS[:partitions], batch_size: DEFAULTS[:batch_size],
                   queue_size_bytes: DEFAULTS[:queue_size_bytes], rotate_bytes: DEFAULTS[:rotate_bytes],
                   query: {}, projection: nil, hint: nil, mapper: nil,
                   resume: { enabled: true, state_path: nil, overwrite_incompatible: false },
                   sharding: { mode: :by_size, part_bytes: DEFAULTS[:rotate_bytes], prefix: nil },
                   logger: nil, on_progress: nil, read_concern: DEFAULTS[:read_concern], read_preference: DEFAULTS[:read_preference],
                   no_cursor_timeout: DEFAULTS[:no_cursor_timeout])
      @client = client
      @collection = client[collection]
      @output = output
      @format = format.to_sym
      @compression = compression.to_sym
      @partitions = partitions
      @batch_size = batch_size
      @queue_size_bytes = queue_size_bytes
      @rotate_bytes = rotate_bytes
      @query = query || {}
      @projection = projection
      @hint = hint
      @mapper = mapper
      @resume = resume || { enabled: true }
      @sharding = sharding || { mode: :by_size }
      @logger = logger
      @on_progress = on_progress
      @read_concern = read_concern
      @read_preference = read_preference
      @no_cursor_timeout = no_cursor_timeout

      @running = true
    end

    def run
      dir, prefix = resolve_output(@output, @format)
      manifest_path = File.join(dir, "#{prefix}.manifest.json")
      query_digest = Manifest.query_digest(@query, @projection)

      manifest = if @resume && @resume[:enabled] && File.exist?(manifest_path)
                   m = Manifest.load(manifest_path)
                   unless m.compatible_with?(collection: @collection.name, format: @format, compression: @compression, query_digest: query_digest)
                     if @resume[:overwrite_incompatible]
                       m = Manifest.new(path: manifest_path)
                     else
                       raise IncompatibleResumeError, "existing manifest incompatible with request; pass overwrite_incompatible: true to reset"
                     end
                   end
                   m
                 else
                   Manifest.new(path: manifest_path)
                 end

      manifest.configure!(collection: @collection.name, format: @format, compression: @compression, query_digest: query_digest, options: {
        partitions: @partitions, batch_size: @batch_size, rotate_bytes: @rotate_bytes
      })
      manifest.ensure_partitions!(@partitions)

      # Plan partitions
      partition_filters = Partitioner.object_id_partitions(collection: @collection, query: @query, partitions: @partitions)

      # Reader queue
      queue = ByteQueue.new(max_bytes: @queue_size_bytes)

      # Writer
      writer = case @format
               when :jsonl
                 WriterJSONL.new(directory: dir, prefix: prefix, compression: @compression, rotate_bytes: @rotate_bytes, logger: @logger, manifest: manifest)
               when :csv
                 single_file = (@sharding && @sharding[:mode].to_s == 'single_file')
                 WriterCSV.new(directory: dir, prefix: prefix, compression: @compression, rotate_bytes: @rotate_bytes, logger: @logger, manifest: manifest, single_file: single_file)
               else
                 raise ArgumentError, "format not implemented: #{@format}"
               end

      # Start reader threads
      readers = partition_filters.each_with_index.map do |pf, idx|
        Thread.new do
          read_partition(idx: idx, filter_spec: pf, queue: queue, batch_size: @batch_size, manifest: manifest)
        end
      end

      # Writer loop
      writer_thread = Thread.new do
        loop do
          batch = queue.pop
          break if batch.nil?
          writer.write_many(batch)
        end
      ensure
        writer.close
      end

      progress_thread = Thread.new do
        last = Time.now
        loop do
          sleep 2
          break unless @running
          @on_progress&.call({ queue_bytes: queue.size_bytes })
        end
      end

      # Join readers
      readers.each(&:join)
      queue.close
      writer_thread.join
      @running = false
      progress_thread.join
      true
    end

    private

    def resolve_output(output, format)
      if File.directory?(output) || output.end_with?("/")
        dir = output
        prefix = @sharding[:prefix] || @collection.name
      else
        dir = File.dirname(output)
        basename = File.basename(output)
        prefix = basename.sub(/\.(jsonl|csv|parquet)(\.(zst|gz))?\z/, '')
      end
      FileUtils.mkdir_p(dir)
      [dir, prefix]
    end

    def read_partition(idx:, filter_spec:, queue:, batch_size:, manifest:)
      filter = filter_spec[:filter]
      sort = filter_spec[:sort] || { _id: 1 }
      hint = filter_spec[:hint] || { _id: 1 }

      # Resume from checkpoint if present
      checkpoint = manifest.partitions[idx] && manifest.partitions[idx]['last_id_exclusive']
      if checkpoint
        filter = filter.dup
        filter['_id'] = (filter['_id'] || {}).merge({ '$gt' => checkpoint })
      end

      opts = { sort: sort, hint: hint }
      opts[:projection] = @projection if @projection
      opts[:batch_size] = batch_size if batch_size
      opts[:no_cursor_timeout] = @no_cursor_timeout
      opts[:read] = { mode: @read_preference }
      # Mongo driver expects read_concern as a hash like { level: :majority }
      if @read_concern
        opts[:read_concern] = @read_concern.is_a?(Hash) ? @read_concern : { level: @read_concern }
      end

      cursor = @collection.find(filter, opts)

      buffer = []
      buffer_bytes = 0
      last_id = checkpoint
      begin
        cursor.each do |doc|
          last_id = doc['_id']
          doc = @mapper.call(doc) if @mapper
          json = Oj.dump(doc, mode: :compat)
          bytes = json.bytesize + 1 # newline later
          buffer << doc
          buffer_bytes += bytes
          if buffer.length >= batch_size || buffer_bytes >= 1_000_000
            queue.push(buffer, bytes: buffer_bytes)
            manifest.update_partition_checkpoint!(idx, last_id)
            buffer = []
            buffer_bytes = 0
          end
        end
        unless buffer.empty?
          queue.push(buffer, bytes: buffer_bytes)
          manifest.update_partition_checkpoint!(idx, last_id)
          buffer = []
          buffer_bytes = 0
        end
        manifest.mark_partition_complete!(idx)
      rescue => e
        # Re-raise to fail the thread; could implement retry/backoff
        raise e
      end
    end
  end
end



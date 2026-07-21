# frozen_string_literal: true

require 'mongo'
require 'etc'
require 'fileutils'
require_relative 'partitioner'
require_relative 'queue'
require_relative 'writer_jsonl'
require_relative 'writer_csv'
require_relative 'writer_parquet'
require_relative 'manifest'
require_relative 'errors'
require_relative 'telemetry'

module Purplelight
  # Snapshot orchestrates partition planning, parallel reads, and writing.
  class Snapshot
    DEFAULTS = {
      format: :jsonl,
      compression: :zstd,
      batch_size: 2_000,
      partitions: (Etc.nprocessors * 2).clamp(4, 32),
      queue_size_bytes: 256 * 1024 * 1024,
      rotate_bytes: 256 * 1024 * 1024,
      read_concern: { level: :majority },
      read_preference: :primary,
      no_cursor_timeout: true
    }.freeze

    def self.snapshot(...)
      new(...).run
    end

    def initialize(client:, collection:, output:, format: DEFAULTS[:format], compression: DEFAULTS[:compression],
                   partitions: DEFAULTS[:partitions], batch_size: DEFAULTS[:batch_size],
                   queue_size_bytes: DEFAULTS[:queue_size_bytes], rotate_bytes: DEFAULTS[:rotate_bytes],
                   query: {}, projection: nil, hint: nil, mapper: nil,
                   resume: { enabled: true, state_path: nil, overwrite_incompatible: false },
                   sharding: { mode: :by_size, part_bytes: DEFAULTS[:rotate_bytes], prefix: nil },
                   logger: nil, on_progress: nil, read_concern: DEFAULTS[:read_concern], read_preference: DEFAULTS[:read_preference],
                   no_cursor_timeout: DEFAULTS[:no_cursor_timeout], telemetry: nil,
                   compression_level: nil, writer_threads: 1, write_chunk_bytes: nil, parquet_row_group: nil,
                   parquet_max_rows: nil)
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
      @compression_level = compression_level
      @writer_threads = writer_threads || 1
      @write_chunk_bytes = write_chunk_bytes
      @parquet_row_group = parquet_row_group
      @parquet_max_rows = parquet_max_rows

      @running = true
      if @on_progress
        @progress_mutex = Mutex.new
        @progress_cv = ConditionVariable.new
      end
      @telemetry_enabled = telemetry ? telemetry.enabled? : (ENV['PL_TELEMETRY'] == '1')
      @telemetry = telemetry || (
        @telemetry_enabled ? Telemetry.new(enabled: true) : Telemetry::NULL
      )
    end

    # rubocop:disable Naming/PredicateMethod
    def run
      dir, prefix = resolve_output(@output, @format)
      manifest_path = File.join(dir, "#{prefix}.manifest.json")
      query_digest = Manifest.query_digest(@query, @projection)

      manifest = if @resume && @resume[:enabled] && File.exist?(manifest_path)
                   m = Manifest.load(manifest_path)
                   unless m.compatible_with?(collection: @collection.name, format: @format, compression: @compression,
                                             query_digest: query_digest)
                     if @resume[:overwrite_incompatible]
                       m = Manifest.new(path: manifest_path)
                     else
                       raise IncompatibleResumeError,
                             'existing manifest incompatible with request; pass overwrite_incompatible: true to reset'
                     end
                   end
                   m
                 else
                   Manifest.new(path: manifest_path)
                 end

      manifest.configure!(collection: @collection.name, format: @format, compression: @compression, query_digest: query_digest, options: {
                            partitions: @partitions,
                            batch_size: @batch_size,
                            queue_size_bytes: @queue_size_bytes,
                            rotate_bytes: @rotate_bytes,
                            hint: @hint,
                            read_concern: (@read_concern.is_a?(Hash) ? @read_concern : { level: @read_concern }),
                            no_cursor_timeout: @no_cursor_timeout,
                            writer_threads: @writer_threads,
                            compression_level: @compression_level || ENV['PL_ZSTD_LEVEL']&.to_i,
                            write_chunk_bytes: @write_chunk_bytes || ENV['PL_WRITE_CHUNK_BYTES']&.to_i,
                            parquet_row_group: @parquet_row_group || ENV['PL_PARQUET_ROW_GROUP']&.to_i,
                            parquet_max_rows: @parquet_max_rows,
                            sharding: @sharding,
                            resume_overwrite_incompatible: @resume && @resume[:overwrite_incompatible] ? true : false,
                            telemetry: @telemetry_enabled
                          }, partition_count: @partitions)

      # Reuse the original ranges on resume. Replanning against a growing collection
      # can move partition boundaries and replay rows under the old checkpoints.
      t_plan = @telemetry.start(:partition_plan_time)
      partition_filters = manifest.partition_filters
      unless partition_filters
        partition_filters = Partitioner.object_id_partitions(collection: @collection, query: @query,
                                                             partitions: @partitions, telemetry: @telemetry)
        manifest.configure_partition_filters!(partition_filters)
      end
      @telemetry.finish(:partition_plan_time, t_plan)

      # Reader queue
      queue = ByteQueue.new(max_bytes: @queue_size_bytes)

      # Writer
      writer_count = @format == :jsonl ? [@writer_threads.to_i, 1].max : 1
      part_sequence = WriterJSONL::PartSequence.new(manifest.parts.length) if @format == :jsonl
      writers = case @format
                when :jsonl
                  Array.new(writer_count) do
                    WriterJSONL.new(directory: dir, prefix: prefix, compression: @compression,
                                    rotate_bytes: @rotate_bytes, logger: @logger, manifest: manifest,
                                    compression_level: @compression_level, write_chunk_bytes: @write_chunk_bytes,
                                    part_sequence:)
                  end
                when :csv
                  single_file = @sharding && @sharding[:mode].to_s == 'single_file'
                  [WriterCSV.new(directory: dir, prefix: prefix, compression: @compression,
                                 rotate_bytes: @rotate_bytes, logger: @logger, manifest: manifest,
                                 single_file:, compression_level: @compression_level)]
                when :parquet
                  single_file = @sharding && @sharding[:mode].to_s == 'single_file'
                  row_group = @parquet_row_group || ENV['PL_PARQUET_ROW_GROUP']&.to_i ||
                              WriterParquet::DEFAULT_ROW_GROUP_SIZE
                  [WriterParquet.new(directory: dir, prefix: prefix, compression: @compression, logger: @logger,
                                     manifest:, single_file:, row_group_size: row_group,
                                     rotate_rows: @parquet_max_rows)]
                else
                  raise ArgumentError, "format not implemented: #{@format}"
                end

      # Start reader threads
      readers = partition_filters.each_with_index.map do |pf, idx|
        Thread.new do
          local_telemetry = @telemetry_enabled ? Telemetry.new(enabled: true) : Telemetry::NULL
          read_partition(idx: idx, filter_spec: pf, queue: queue, batch_size: @batch_size, manifest: manifest, telemetry: local_telemetry)
          # Merge per-thread telemetry
          @telemetry.merge!(local_telemetry) if @telemetry_enabled
        end
      end

      # Writer loops
      writer_telemetries = []
      writer_threads = writers.map do |writer|
        writer_telemetry = @telemetry_enabled ? Telemetry.new(enabled: true) : Telemetry::NULL
        writer_telemetries << writer_telemetry
        Thread.new(writer, writer_telemetry) do |worker_writer, worker_telemetry|
          Thread.current[:pl_telemetry] = worker_telemetry if @telemetry_enabled
          loop do
            batch = queue.pop
            break if batch.nil?

            worker_writer.write_many(batch)
          end
        ensure
          worker_writer.close
        end
      end

      progress_thread = if @on_progress
                          Thread.new do
                            loop do
                              @progress_mutex.synchronize { @progress_cv.wait(@progress_mutex, 2) }
                              break unless @running

                              @on_progress.call({ queue_bytes: queue.size_bytes })
                            end
                          end
                        end

      # Join readers
      readers.each(&:join)
      queue.close
      writer_threads.each(&:join)
      writer_telemetries.each { |writer_telemetry| @telemetry.merge!(writer_telemetry) } if @telemetry_enabled
      if progress_thread
        @progress_mutex.synchronize do
          @running = false
          @progress_cv.broadcast
        end
        progress_thread.join
      else
        @running = false
      end
      if @telemetry_enabled
        total = @telemetry.timers.values.sum
        breakdown = @telemetry.timers
                              .sort_by { |_k, v| -v }
                              .map { |k, v| [k, v, total.zero? ? 0 : ((v / total) * 100.0)] }
        if @logger
          @logger.info('Telemetry (seconds and % of timed work):')
          breakdown.each { |k, v, pct| @logger.info("  #{k}: #{v.round(3)}s (#{pct.round(1)}%)") }
        else
          puts 'Telemetry (seconds and % of timed work):'
          breakdown.each { |k, v, pct| puts "  #{k}: #{v.round(3)}s (#{pct.round(1)}%)" }
        end
      end
      true
    end
    # rubocop:enable Naming/PredicateMethod

    private

    def resolve_output(output, _format)
      if File.directory?(output) || output.end_with?('/')
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

    def read_partition(idx:, filter_spec:, queue:, batch_size:, manifest:, telemetry: Telemetry::NULL)
      filter = filter_spec[:filter]
      sort = filter_spec[:sort] || { _id: 1 }
      hint = @hint || filter_spec[:hint] || { _id: 1 }

      # Resume from checkpoint if present
      checkpoint = manifest.partition_checkpoint(idx)
      if checkpoint
        filter = filter.dup
        filter['_id'] = (filter['_id'] || {}).merge({ '$gt' => checkpoint })
      end

      opts = { sort: sort, hint: hint }
      opts[:projection] = @projection if @projection
      opts[:batch_size] = batch_size if batch_size
      opts[:no_cursor_timeout] = @no_cursor_timeout
      # Read preference can be a symbol (mode) or a full hash with tag_sets
      if @read_preference
        opts[:read] = @read_preference.is_a?(Hash) ? @read_preference : { mode: @read_preference }
      end
      # Mongo driver expects read_concern as a hash like { level: :majority }
      if @read_concern
        opts[:read_concern] = @read_concern.is_a?(Hash) ? @read_concern : { level: @read_concern }
      end

      cursor = @collection.find(filter, opts)

      encode_lines = (@format == :jsonl)
      # When JSONL, build one big string per batch to offload join cost from writer.
      string_batch = +''
      buffer = []
      buffer_bytes = 0
      buffer_rows = 0
      json_state = if encode_lines
                     JSON::Ext::Generator::State.new(ascii_only: false, max_nesting: false,
                                                     buffer_initial_length: 4_096)
                   end
      bson_size_buffer = BSON::ByteBuffer.new unless encode_lines
      last_id = checkpoint
      begin
        cursor.each do |doc|
          last_id = doc['_id']
          doc = @mapper.call(doc) if @mapper
          t_ser = telemetry.start(:serialize_time)
          if encode_lines
            json = json_state.generate(doc)
            telemetry.finish(:serialize_time, t_ser)
            string_batch << json
            string_batch << "\n"
            bytes = json.bytesize + 1
          else
            previous_length = bson_size_buffer.length
            doc.to_bson(bson_size_buffer)
            bytes = bson_size_buffer.length - previous_length
            telemetry.finish(:serialize_time, t_ser)
            buffer << doc
          end
          buffer_bytes += bytes
          buffer_rows += 1
          # Flush on the configured row count or before a batch exceeds 1 MB.
          ready = buffer_rows >= batch_size || buffer_bytes >= 1_000_000
          next unless ready

          t_q = telemetry.start(:queue_wait_time)
          if encode_lines
            batch = WriterJSONL::EncodedBatch.new(data: string_batch, rows: buffer_rows, bytes: buffer_bytes)
            queue.push(batch, bytes: buffer_bytes)
            string_batch = +''
          else
            queue.push(buffer, bytes: buffer_bytes)
            buffer = []
            bson_size_buffer = BSON::ByteBuffer.new
          end
          telemetry.finish(:queue_wait_time, t_q)
          manifest.update_partition_checkpoint!(idx, last_id)
          buffer_bytes = 0
          buffer_rows = 0
        end
        if encode_lines
          unless string_batch.empty?
            t_q = telemetry.start(:queue_wait_time)
            batch = WriterJSONL::EncodedBatch.new(data: string_batch, rows: buffer_rows, bytes: buffer_bytes)
            queue.push(batch, bytes: buffer_bytes)
            telemetry.finish(:queue_wait_time, t_q)
            manifest.update_partition_checkpoint!(idx, last_id)
            string_batch = +''
            buffer_bytes = 0
            buffer_rows = 0
          end
        elsif !buffer.empty?
          t_q = telemetry.start(:queue_wait_time)
          queue.push(buffer, bytes: buffer_bytes)
          telemetry.finish(:queue_wait_time, t_q)
          manifest.update_partition_checkpoint!(idx, last_id)
          buffer = []
          buffer_bytes = 0
          buffer_rows = 0
        end
        manifest.mark_partition_complete!(idx)
      end
    end
  end
end

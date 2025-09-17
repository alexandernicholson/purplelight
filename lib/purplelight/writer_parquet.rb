# frozen_string_literal: true

begin
  require 'arrow'
  require 'parquet'
rescue LoadError
  # Arrow/Parquet not available; writer will refuse to run
end

require 'fileutils'

module Purplelight
  # WriterParquet writes Parquet files via Apache Arrow when available.
  class WriterParquet
    DEFAULT_ROW_GROUP_SIZE = 10_000

    def initialize(directory:, prefix:, compression: :zstd, row_group_size: DEFAULT_ROW_GROUP_SIZE, logger: nil,
                   manifest: nil, single_file: true, schema: nil, rotate_rows: nil)
      @directory = directory
      @prefix = prefix
      @compression = compression
      @row_group_size = row_group_size
      @logger = logger
      @manifest = manifest
      @single_file = single_file
      @schema = schema
      @rotate_rows = rotate_rows

      @closed = false
      @file_seq = 0
      @part_index = nil
      @pq_writer = nil
      @rows_in_current_file = 0

      ensure_dependencies!
      reset_buffers
    end

    def write_many(array_of_docs)
      ensure_open!
      array_of_docs.each { |doc| @buffer_docs << doc }
      flush_row_groups_if_needed
    end

    def close
      return if @closed

      flush_all_row_groups
      finalize_current_part! if @writer_path
      @closed = true
    end

    private

    def ensure_dependencies!
      return if defined?(Arrow) && defined?(Parquet)

      raise ArgumentError, 'Parquet support requires gems: red-arrow and red-parquet. Add them to your Gemfile.'
    end

    def reset_buffers
      @buffer_docs = []
      @columns = nil
      @writer_path = nil
    end

    def ensure_open!
      return if @writer_path

      FileUtils.mkdir_p(@directory)
      @writer_path = next_part_path
      @part_index = @manifest&.open_part!(@writer_path) if @manifest
      @rows_in_current_file = 0
    end

    # No-op; we now write once on close for simplicity

    def build_table(docs)
      # Infer columns
      @columns ||= infer_columns(docs)
      columns = {}
      @columns.each do |name|
        values = docs.map { |d| extract_value(d, name) }
        columns[name] = Arrow::ArrayBuilder.build(values)
      end
      Arrow::Table.new(columns)
    end

    def write_table(table, path, append: false) # rubocop:disable Lint/UnusedMethodArgument
      # Stream via ArrowFileWriter when available to avoid building huge tables
      if defined?(Parquet::ArrowFileWriter)
        unless @pq_writer
          props = build_writer_properties_for_compression(@compression)
          @pq_writer = create_arrow_file_writer(table.schema, path, props)
        end
        # Prefer passing row_group_size; fallback to single-arg for older APIs
        begin
          @pq_writer.write_table(table, @row_group_size)
        rescue ArgumentError
          @pq_writer.write_table(table)
        end
        return
      end
      # Fallback to one-shot save when streaming API is not available
      if table.respond_to?(:save)
        begin
          table.save(path, format: :parquet, compression: normalize_parquet_compression_name(@compression))
        rescue StandardError
          table.save(path, format: :parquet)
        end
        return
      end
      raise 'Parquet writer not available in this environment'
    end

    def finalize_current_part!
      return if @writer_path.nil?

      if @pq_writer
        @pq_writer.close
        @pq_writer = nil
      end
      @manifest&.complete_part!(index: @part_index, checksum: nil)
      @file_seq += 1 unless @single_file
      @writer_path = nil
      @part_index = nil
      @rows_in_current_file = 0
    end

    def next_part_path
      ext = 'parquet'
      filename = if @single_file
                   "#{@prefix}.#{ext}"
                 else
                   format('%<prefix>s-part-%<seq>06d.%<ext>s', prefix: @prefix, seq: @file_seq, ext: ext)
                 end
      File.join(@directory, filename)
    end

    def infer_columns(docs)
      keys = {}
      docs.each do |d|
        d.each_key { |k| keys[k.to_s] = true }
      end
      keys.keys.sort
    end

    def extract_value(doc, key)
      value = doc[key] || doc[key.to_sym]
      # Normalize common MongoDB/BSON types to Parquet-friendly values
      return value.to_s if defined?(BSON) && value.is_a?(BSON::ObjectId)

      value
    end

    def flush_row_groups_if_needed
      return if @buffer_docs.empty?

      while @buffer_docs.length >= @row_group_size
        ensure_open!
        group = @buffer_docs.shift(@row_group_size)
        if @rotate_rows && !@single_file && (@rows_in_current_file + group.length) > @rotate_rows
          # Write a partial chunk to fill the current file, then rotate and write the rest
          remaining_allowed = @rotate_rows - @rows_in_current_file
          if remaining_allowed.positive?
            part_a = group.first(remaining_allowed)
            t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
            table_a = build_table(part_a)
            Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

            t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
            write_table(table_a, @writer_path, append: true)
            Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
            @manifest&.add_progress_to_part!(index: @part_index, rows_delta: part_a.length, bytes_delta: 0)
            @rows_in_current_file += part_a.length
          end

          finalize_current_part!
          ensure_open!

          part_b = group.drop(remaining_allowed)
          unless part_b.empty?
            t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
            table_b = build_table(part_b)
            Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

            t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
            write_table(table_b, @writer_path, append: true)
            Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
            @manifest&.add_progress_to_part!(index: @part_index, rows_delta: part_b.length, bytes_delta: 0)
            @rows_in_current_file += part_b.length
            maybe_rotate!
          end
        else
          t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
          table = build_table(group)
          Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

          t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
          write_table(table, @writer_path, append: true)
          Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
          @manifest&.add_progress_to_part!(index: @part_index, rows_delta: group.length, bytes_delta: 0)
          @rows_in_current_file += group.length
          maybe_rotate!
        end
      end
    end

    def flush_all_row_groups
      return if @buffer_docs.empty?

      # Flush any full groups first
      flush_row_groups_if_needed
      return if @buffer_docs.empty?

      # Flush remaining as a final smaller group
      remaining = @buffer_docs.length
      t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
      table = build_table(@buffer_docs)
      Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

      ensure_open!
      # Pre-rotate to avoid exceeding rotate_rows on this final write
      if @rotate_rows && !@single_file && @rows_in_current_file.positive? && (@rows_in_current_file + remaining) > @rotate_rows
        finalize_current_part!
        ensure_open!
      end

      t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
      write_table(table, @writer_path, append: true)
      Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
      rows_written = (table.respond_to?(:n_rows) ? table.n_rows : remaining)
      @manifest&.add_progress_to_part!(index: @part_index, rows_delta: rows_written, bytes_delta: 0)
      @rows_in_current_file += rows_written
      @buffer_docs.clear
      maybe_rotate!
    end

    def maybe_rotate!
      return if @single_file
      return unless @rotate_rows && @rows_in_current_file >= @rotate_rows

      finalize_current_part!
      # Next write will open a new part
    end

    def build_writer_properties_for_compression(requested)
      codec_const = parquet_codec_constant(requested)
      return nil unless codec_const

      # Prefer WriterProperties builder if available
      begin
        if defined?(Parquet::WriterProperties) && Parquet::WriterProperties.respond_to?(:builder)
          builder = Parquet::WriterProperties.builder
          if builder.respond_to?(:compression)
            builder = builder.compression(codec_const)
          elsif builder.respond_to?(:set_compression)
            builder = builder.set_compression(codec_const)
          end
          return builder.build if builder.respond_to?(:build)
        end
      rescue StandardError
        # fall through to other strategies
      end

      # Alternative builder class naming fallback
      begin
        if defined?(Parquet::WriterPropertiesBuilder)
          b = Parquet::WriterPropertiesBuilder.new
          if b.respond_to?(:compression)
            b.compression(codec_const)
          elsif b.respond_to?(:set_compression)
            b.set_compression(codec_const)
          end
          return b.build if b.respond_to?(:build)
        end
      rescue StandardError
        # ignore
      end
      nil
    end

    def create_arrow_file_writer(schema, path, props)
      attempts = []
      if props
        attempts << -> { Parquet::ArrowFileWriter.open(schema, path, props) }
        attempts << -> { Parquet::ArrowFileWriter.open(schema, path, properties: props) }
      end
      attempts << -> { Parquet::ArrowFileWriter.open(schema, path) }

      attempts.each do |call|
        return call.call
      rescue StandardError
        next
      end
      raise 'failed to open Parquet::ArrowFileWriter'
    end

    def parquet_codec_constant(requested)
      name = normalize_parquet_compression_name(requested)
      return nil unless name

      up = case name
           when 'zstd', 'zstandard' then 'ZSTD'
           when 'gzip' then 'GZIP'
           when 'snappy' then 'SNAPPY'
           when 'none' then 'UNCOMPRESSED'
           else name.upcase
           end
      candidates = %w[CompressionType Compression CompressionCodec]
      candidates.each do |mod|
        m = Parquet.const_get(mod)
        return m.const_get(up) if m.const_defined?(up)
      rescue StandardError
        next
      end
      nil
    end

    def normalize_parquet_compression_name(requested)
      return nil if requested.nil?

      s = requested.to_s.downcase
      return 'none' if s == 'none'
      return 'gzip' if s == 'gzip'
      return 'snappy' if s == 'snappy'
      return 'zstd' if %w[zstd zstandard].include?(s)

      nil
    end
  end
end

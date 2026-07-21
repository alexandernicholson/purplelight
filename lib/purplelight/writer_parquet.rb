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
      @file_seq = manifest ? manifest.parts.length : 0
      @part_index = nil
      @pq_writer = nil
      @rows_in_current_file = 0

      ensure_dependencies!
      reset_buffers
    end

    def write_many(array_of_docs)
      @buffer_docs.concat(array_of_docs)
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
      @buffer_head = 0
      @writer_path = nil
    end

    def ensure_open!
      return if @writer_path

      FileUtils.mkdir_p(@directory)
      @writer_path = next_part_path
      @part_index = @manifest.open_part!(@writer_path) if @manifest
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
      unless @pq_writer
        properties = build_writer_properties_for_compression(@compression)
        @pq_writer = Parquet::ArrowFileWriter.open(table.schema, path, properties)
      end
      @pq_writer.write(table, chunk_size: @row_group_size)
    end

    def finalize_current_part!
      @pq_writer.close
      @pq_writer = nil
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
      value = doc[key]
      value = doc[key.to_sym] if value.nil? && !doc.key?(key)
      # Normalize common MongoDB/BSON types to Parquet-friendly values
      return value.to_s if value.is_a?(BSON::ObjectId)

      value
    end

    def flush_row_groups_if_needed
      return if buffered_count.zero?

      while buffered_count >= @row_group_size
        ensure_open!
        group = take_buffered(@row_group_size)
        if @rotate_rows && !@single_file && (@rows_in_current_file + group.length) > @rotate_rows
          # Write a partial chunk to fill the current file, then rotate and write the rest
          remaining_allowed = @rotate_rows - @rows_in_current_file
          part_a = group.first(remaining_allowed)
          t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
          table_a = build_table(part_a)
          Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

          t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
          write_table(table_a, @writer_path, append: true)
          Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
          @manifest&.add_progress_to_part!(index: @part_index, rows_delta: part_a.length, bytes_delta: 0)
          @rows_in_current_file += part_a.length

          finalize_current_part!
          ensure_open!

          part_b = group.drop(remaining_allowed)
          t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
          table_b = build_table(part_b)
          Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

          t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
          write_table(table_b, @writer_path, append: true)
          Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
          @manifest&.add_progress_to_part!(index: @part_index, rows_delta: part_b.length, bytes_delta: 0)
          @rows_in_current_file += part_b.length
        else
          t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
          table = build_table(group)
          Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

          t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
          write_table(table, @writer_path, append: true)
          Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
          @manifest&.add_progress_to_part!(index: @part_index, rows_delta: group.length, bytes_delta: 0)
          @rows_in_current_file += group.length
        end
        maybe_rotate!
      end
    end

    def flush_all_row_groups
      return if buffered_count.zero?

      # Flush any full groups first
      flush_row_groups_if_needed

      # Flush remaining as a final smaller group
      remaining = buffered_count
      t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
      table = build_table(take_buffered(remaining))
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
      rows_written = table.n_rows
      @manifest&.add_progress_to_part!(index: @part_index, rows_delta: rows_written, bytes_delta: 0)
      @rows_in_current_file += rows_written
      @buffer_docs.clear
      @buffer_head = 0
      maybe_rotate!
    end

    def maybe_rotate!
      return if @single_file
      return unless @rotate_rows && @rows_in_current_file >= @rotate_rows

      finalize_current_part!
      # Next write will open a new part
    end

    def build_writer_properties_for_compression(requested)
      compression = normalize_parquet_compression_name(requested)
      return nil unless compression

      properties = Parquet::WriterProperties.new
      properties.set_compression(compression == 'none' ? 'uncompressed' : compression)
      properties
    end

    def buffered_count
      @buffer_docs.length - @buffer_head
    end

    def take_buffered(count)
      documents = @buffer_docs.slice(@buffer_head, count)
      @buffer_head += count
      if @buffer_head >= @row_group_size && @buffer_head * 2 >= @buffer_docs.length
        @buffer_docs = @buffer_docs.drop(@buffer_head)
        @buffer_head = 0
      end
      documents
    end

    def normalize_parquet_compression_name(requested)
      return nil if requested.nil?

      s = requested.to_s.downcase
      return 'none' if s == 'none'
      return 'gzip' if s == 'gzip'
      return 'snappy' if s == 'snappy'
      return 'zstd' if s == 'zstd'

      nil
    end
  end
end

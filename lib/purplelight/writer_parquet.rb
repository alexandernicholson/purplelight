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
                   manifest: nil, single_file: true, schema: nil)
      @directory = directory
      @prefix = prefix
      @compression = compression
      @row_group_size = row_group_size
      @logger = logger
      @manifest = manifest
      @single_file = single_file
      @schema = schema

      @closed = false
      @file_seq = 0
      @part_index = nil
      @pq_writer = nil

      ensure_dependencies!
      reset_buffers
    end

    def write_many(array_of_docs)
      ensure_open!
      array_of_docs.each { |doc| @buffer_docs << doc }
      flush_row_groups_if_needed
      @manifest&.add_progress_to_part!(index: @part_index, rows_delta: array_of_docs.length, bytes_delta: 0)
    end

    def close
      return if @closed

      ensure_open!
      flush_all_row_groups
      finalize_current_part!
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
          @pq_writer = Parquet::ArrowFileWriter.open(table.schema, path)
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
        table.save(path, format: :parquet)
        return
      end
      raise 'Parquet writer not available in this environment'
    end

    def finalize_current_part!
      if @pq_writer
        @pq_writer.close
        @pq_writer = nil
      end
      @manifest&.complete_part!(index: @part_index, checksum: nil)
      @file_seq += 1 unless @single_file
      @writer_path = nil
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
        group = @buffer_docs.shift(@row_group_size)
        t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
        table = build_table(group)
        Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

        t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
        write_table(table, @writer_path, append: true)
        Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
      end
    end

    def flush_all_row_groups
      return if @buffer_docs.empty?

      # Flush any full groups first
      flush_row_groups_if_needed
      return if @buffer_docs.empty?

      # Flush remaining as a final smaller group
      t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
      table = build_table(@buffer_docs)
      Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

      t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
      write_table(table, @writer_path, append: true)
      Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
      @buffer_docs.clear
    end
  end
end

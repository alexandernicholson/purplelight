# frozen_string_literal: true

require 'csv'
require 'oj'
require 'zlib'
require 'fileutils'

begin
  require 'zstds'
rescue LoadError
end

module Purplelight
  class WriterCSV
    DEFAULT_ROTATE_BYTES = 256 * 1024 * 1024

    def initialize(directory:, prefix:, compression: :zstd, rotate_bytes: DEFAULT_ROTATE_BYTES, logger: nil,
                   manifest: nil, single_file: false, columns: nil, headers: true)
      @directory = directory
      @prefix = prefix
      @compression = compression
      @rotate_bytes = rotate_bytes
      @logger = logger
      @manifest = manifest
      @single_file = single_file

      @columns = columns&.map(&:to_s)
      @headers = headers

      @part_index = nil
      @io = nil
      @csv = nil
      @bytes_written = 0
      @rows_written = 0
      @file_seq = 0
      @closed = false

      @effective_compression = determine_effective_compression(@compression)
      if @effective_compression.to_s != @compression.to_s
        @logger&.warn("requested compression '#{@compression}' not available; using '#{@effective_compression}'")
      end
    end

    def write_many(array_of_docs)
      ensure_open!

      # infer columns if needed from docs
      if @columns.nil?
        sample_docs = array_of_docs.is_a?(Array) ? array_of_docs : []
        sample_docs = sample_docs.reject { |d| d.is_a?(String) }
        @columns = infer_columns(sample_docs)
        @csv << @columns if @headers
      end

      array_of_docs.each do |doc|
        next if doc.is_a?(String)

        row = @columns.map { |k| extract_value(doc, k) }
        @csv << row
        @rows_written += 1
      end
      @manifest&.add_progress_to_part!(index: @part_index, rows_delta: array_of_docs.size, bytes_delta: 0)

      rotate_if_needed
    end

    def rotate_if_needed
      return if @single_file
      return if @rotate_bytes.nil?

      raw_bytes = @io.respond_to?(:pos) ? @io.pos : @bytes_written
      return if raw_bytes < @rotate_bytes

      rotate!
    end

    def close
      return if @closed

      if @csv
        @csv.flush
      end
      if @io
        finalize_current_part!
        @io.close
      end
      @closed = true
    end

    private

    def ensure_open!
      return if @io

      FileUtils.mkdir_p(@directory)
      path = next_part_path
      @part_index = @manifest&.open_part!(path) if @manifest
      raw = File.open(path, 'wb')
      @io = build_compressed_io(raw)
      @csv = CSV.new(@io)
      @bytes_written = 0
      @rows_written = 0
    end

    def build_compressed_io(raw)
      case @effective_compression.to_s
      when 'zstd'
        if defined?(ZSTDS)
          return ZSTDS::Writer.open(raw, level: 10)
        else
          @logger&.warn("zstd gem not loaded; using gzip")
          return Zlib::GzipWriter.new(raw)
        end
      when 'gzip'
        return Zlib::GzipWriter.new(raw)
      when 'none'
        return raw
      else
        raise ArgumentError, "unknown compression: #{@effective_compression}"
      end
    end

    def rotate!
      return unless @io

      finalize_current_part!
      @io.close
      @io = nil
      @csv = nil
      ensure_open!
    end

    def finalize_current_part!
      # Avoid flushing compressed writer explicitly to prevent Zlib::BufError; close will finish the stream.
      @manifest&.complete_part!(index: @part_index, checksum: nil)
      @file_seq += 1 unless @single_file
    end

    def next_part_path
      ext = 'csv'
      if @single_file
        filename = format("%s.%s", @prefix, ext)
      else
        filename = format("%s-part-%06d.%s", @prefix, @file_seq, ext)
      end
      filename += ".zst" if @effective_compression.to_s == 'zstd'
      filename += ".gz" if @effective_compression.to_s == 'gzip'
      File.join(@directory, filename)
    end

    def determine_effective_compression(requested)
      case requested.to_s
      when 'zstd'
        return (defined?(ZSTDS) ? :zstd : :gzip)
      when 'gzip'
        return :gzip
      when 'none'
        return :none
      else
        return :gzip
      end
    end

    def infer_columns(docs)
      keys = {}
      docs.each do |d|
        (d.keys - ['_id']).each { |k| keys[k.to_s] = true }
      end
      # Put _id first if present, then other keys sorted
      cols = []
      cols << '_id' if docs.first.key?('_id') || docs.first.key?(:_id)
      cols + keys.keys.sort
    end

    def extract_value(doc, key)
      val = doc[key] || doc[key.to_sym]
      case val
      when Hash, Array
        Oj.dump(val, mode: :compat)
      else
        val
      end
    end
  end
end

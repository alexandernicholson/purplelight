# frozen_string_literal: true

require 'csv'
require 'json'
require 'zlib'
require 'fileutils'

begin
  require 'zstds'
rescue LoadError
  # zstd not available; fallback handled later via gzip
end

begin
  require 'zstd-ruby'
rescue LoadError
  # alternative zstd gem not available
end

module Purplelight
  # WriterCSV writes documents to CSV files with optional compression.
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
      env_level = ENV['PL_ZSTD_LEVEL']&.to_i
      @compression_level = (env_level&.positive? ? env_level : nil)
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
      return unless @effective_compression.to_s != @compression.to_s

      @logger&.warn("requested compression '#{@compression}' not available; using '#{@effective_compression}'")
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

      @csv&.flush
      if @io
        t = Thread.current[:pl_telemetry]&.start(:rotate_time)
        finalize_current_part!
        @io.close
        Thread.current[:pl_telemetry]&.finish(:rotate_time, t)
      end
      @closed = true
    end

    private

    # Minimal wrapper to count bytes written for rotate logic when
    # underlying compressed writer doesn't expose position (e.g., zstd-ruby).
    class CountingIO
      def initialize(io, on_write:)
        @io = io
        @on_write = on_write
      end

      def write(data)
        bytes_written = @io.write(data)
        @on_write.call(bytes_written) if bytes_written && @on_write
        bytes_written
      end

      # CSV calls '<<' on the underlying IO in some code paths
      def <<(data)
        write(data)
      end

      # CSV#flush may forward flush to underlying IO; make it a no-op if unavailable
      def flush
        @io.flush if @io.respond_to?(:flush)
      end

      def method_missing(method_name, *, &)
        @io.send(method_name, *, &)
      end

      def respond_to_missing?(method_name, include_private = false)
        @io.respond_to?(method_name, include_private)
      end
    end

    def ensure_open!
      return if @io

      FileUtils.mkdir_p(@directory)
      path = next_part_path
      @part_index = @manifest&.open_part!(path) if @manifest
      raw = File.open(path, 'wb')
      compressed = build_compressed_io(raw)
      @io = CountingIO.new(compressed, on_write: ->(n) { @bytes_written += n })
      @csv = CSV.new(@io)
      @bytes_written = 0
      @rows_written = 0
    end

    def build_compressed_io(raw)
      case @effective_compression.to_s
      when 'zstd'
        if Object.const_defined?(:Zstd) && defined?(::Zstd::StreamWriter)
          level = @compression_level || 10
          return ::Zstd::StreamWriter.new(raw, level: level)
        elsif defined?(ZSTDS)
          level = @compression_level || 10
          return ZSTDS::Stream::Writer.new(raw, compression_level: level)
        end

        @logger&.warn('zstd gem not loaded; using gzip')
        Zlib::GzipWriter.new(raw)

      when 'gzip'
        Zlib::GzipWriter.new(raw)
      when 'none'
        raw
      else
        raise ArgumentError, "unknown compression: #{@effective_compression}"
      end
    end

    def rotate!
      return unless @io

      t = Thread.current[:pl_telemetry]&.start(:rotate_time)
      finalize_current_part!
      @io.close
      Thread.current[:pl_telemetry]&.finish(:rotate_time, t)
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
      filename = if @single_file
                   format('%<prefix>s.%<ext>s', prefix: @prefix, ext: ext)
                 else
                   format('%<prefix>s-part-%<seq>06d.%<ext>s', prefix: @prefix, seq: @file_seq, ext: ext)
                 end
      filename += '.zst' if @effective_compression.to_s == 'zstd'
      filename += '.gz' if @effective_compression.to_s == 'gzip'
      File.join(@directory, filename)
    end

    def determine_effective_compression(requested)
      case requested.to_s
      when 'zstd'
        (defined?(ZSTDS) || (Object.const_defined?(:Zstd) && defined?(::Zstd::StreamWriter)) ? :zstd : :gzip)
      when 'none'
        :none
      else
        :gzip
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
        JSON.generate(val)
      else
        val
      end
    end
  end
end

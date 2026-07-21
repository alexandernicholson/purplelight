# frozen_string_literal: true

require 'json'
require 'zlib'
require 'fileutils'

# simplecov:disable
begin
  require 'zstd-ruby'
rescue LoadError
  begin
    require 'zstds'
  rescue LoadError
    # no zstd backend; gzip fallback used later
  end
end
# simplecov:enable

module Purplelight
  # WriterCSV writes documents to CSV files with optional compression.
  class WriterCSV
    DEFAULT_ROTATE_BYTES = 256 * 1024 * 1024
    DEFAULT_ZSTD_LEVEL = 9
    DEFAULT_GZIP_LEVEL = 1

    def initialize(directory:, prefix:, compression: :zstd, rotate_bytes: DEFAULT_ROTATE_BYTES, logger: nil,
                   manifest: nil, single_file: false, columns: nil, headers: true, compression_level: nil)
      @directory = directory
      @prefix = prefix
      @compression = compression
      @rotate_bytes = rotate_bytes
      @logger = logger
      @manifest = manifest
      env_level = ENV['PL_ZSTD_LEVEL']&.to_i
      @compression_level = compression_level || (env_level&.positive? ? env_level : nil)
      @single_file = single_file

      @columns = columns&.map(&:to_s)
      @headers = headers

      @part_index = nil
      @io = nil
      @file_seq = manifest ? manifest.parts.length : 0
      @closed = false

      @effective_compression = determine_effective_compression(@compression)
      return unless @effective_compression.to_s != @compression.to_s

      @logger&.warn("requested compression '#{@compression}' not available; using '#{@effective_compression}'")
    end

    def write_many(array_of_docs)
      ensure_open!

      output = +''
      # Infer columns if needed from docs.
      if @columns.nil?
        sample_docs = array_of_docs.is_a?(Array) ? array_of_docs : []
        sample_docs = sample_docs.grep_v(String)
        @columns = infer_columns(sample_docs)
        append_csv_row(output, @columns) if @headers
      end

      rows = 0
      array_of_docs.each do |doc|
        next if doc.is_a?(String)

        append_csv_document(output, doc)
        rows += 1
      end
      @io.write(output) unless output.empty?
      @manifest&.add_progress_to_part!(index: @part_index, rows_delta: rows, bytes_delta: 0)

      rotate_if_needed
    end

    def rotate_if_needed
      return if @single_file
      return if @rotate_bytes.nil?

      raw_bytes = @io.respond_to?(:pos) ? @io.pos : @io.bytes_written
      return if raw_bytes < @rotate_bytes

      rotate!
    end

    def close
      return if @closed

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
      attr_reader :bytes_written

      def initialize(io)
        @io = io
        @bytes_written = 0
      end

      def write(data)
        bytes_written = @io.write(data)
        @bytes_written += bytes_written
        bytes_written
      end

      alias << write

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
      @part_index = @manifest.open_part!(path) if @manifest
      # The compression stream owns and closes this file handle.
      # rubocop:disable Style/FileOpen
      raw = File.open(path, 'wb')
      # rubocop:enable Style/FileOpen
      compressed = build_compressed_io(raw)
      @io = CountingIO.new(compressed)
      return unless @headers && @columns

      header = +''
      append_csv_row(header, @columns)
      @io.write(header)
    end

    def build_compressed_io(raw)
      case @effective_compression.to_s
      when 'zstd'
        level = @compression_level || DEFAULT_ZSTD_LEVEL
        if Object.const_defined?(:Zstd) && defined?(::Zstd::StreamWriter)
          ::Zstd::StreamWriter.new(raw, level: level)
        else
          ZSTDS::Stream::Writer.new(raw, compression_level: level)
        end

      when 'gzip'
        level = @compression_level || DEFAULT_GZIP_LEVEL
        Zlib::GzipWriter.new(raw, level)
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
      # Order: explicit request -> zstd-ruby -> zstds -> gzip
      req = requested.to_s
      return :none if req == 'none'
      return :gzip if req == 'gzip'

      if req == 'zstd'
        return :zstd if Object.const_defined?(:Zstd) && defined?(::Zstd::StreamWriter)
        return :zstd if defined?(ZSTDS)

        return :gzip
      end
      # Default auto-select
      return :zstd if Object.const_defined?(:Zstd) && defined?(::Zstd::StreamWriter)
      return :zstd if defined?(ZSTDS)

      :gzip
    end

    def append_csv_document(output, document)
      index = 0
      while index < @columns.length
        output << ',' unless index.zero?
        append_csv_value(output, extract_value(document, @columns[index]))
        index += 1
      end
      output << "\n"
    end

    def append_csv_row(output, values)
      values.each_with_index do |value, index|
        output << ',' unless index.zero?
        append_csv_value(output, value)
      end
      output << "\n"
    end

    def append_csv_value(output, value)
      return if value.nil?

      string = value.to_s
      contains_quote = string.include?('"')
      unless string.empty? || contains_quote || string.include?(',') || string.include?("\n") || string.include?("\r")
        output << string
        return
      end

      output << '"'
      output << (contains_quote ? string.gsub('"', '""') : string)
      output << '"'
    end

    def infer_columns(docs)
      keys = {}
      docs.each do |d|
        (d.keys - ['_id']).each { |k| keys[k.to_s] = true }
      end
      # Put _id first if present, then other keys sorted
      cols = []
      first = docs.first
      cols << '_id' if first && (first.key?('_id') || first.key?(:_id))
      cols + keys.keys.sort
    end

    def extract_value(doc, key)
      val = doc[key]
      val = doc[key.to_sym] if val.nil? && !doc.key?(key)
      case val
      when Hash, Array
        JSON.generate(val)
      else
        val
      end
    end
  end
end

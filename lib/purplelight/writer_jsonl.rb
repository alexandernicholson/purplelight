# frozen_string_literal: true

require 'oj'
require 'zlib'
require 'fileutils'

begin
  require 'zstds'
rescue LoadError
  # zstd not available; will fallback to gzip
end

module Purplelight
  class WriterJSONL
    DEFAULT_ROTATE_BYTES = 256 * 1024 * 1024

    def initialize(directory:, prefix:, compression: :zstd, rotate_bytes: DEFAULT_ROTATE_BYTES, logger: nil, manifest: nil)
      @directory = directory
      @prefix = prefix
      @compression = compression
      @rotate_bytes = rotate_bytes
      @logger = logger
      @manifest = manifest

      @part_index = nil
      @io = nil
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
      buffer = array_of_docs.map { |doc| Oj.dump(doc, mode: :compat) + "\n" }.join
      write_buffer(buffer)
      @rows_written += array_of_docs.size
      @manifest&.add_progress_to_part!(index: @part_index, rows_delta: array_of_docs.size, bytes_delta: buffer.bytesize)
    end

    def rotate_if_needed
      return if @rotate_bytes.nil?
      return if @bytes_written < @rotate_bytes
      rotate!
    end

    def close
      return if @closed
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
      @bytes_written = 0
      @rows_written = 0
    end

    def build_compressed_io(raw)
      case @effective_compression.to_s
      when 'zstd'
        if defined?(ZSTDS)
          # ZSTDS::Writer supports IO-like interface
          return ZSTDS::Writer.open(raw, level: 10)
        else
          @logger&.warn("zstd gem not loaded; this should have been handled earlier")
          return Zlib::GzipWriter.new(raw)
        end
      when 'gzip'
        return Zlib::GzipWriter.new(raw)
      when 'none'
        return raw
      else
        raise ArgumentError, "unknown compression: #{@compression}"
      end
    end

    def write_buffer(buffer)
      @io.write(buffer)
      @bytes_written += buffer.bytesize
      rotate_if_needed
    end

    def rotate!
      return unless @io
      finalize_current_part!
      @io.close
      @io = nil
      ensure_open!
    end

    def finalize_current_part!
      @io.flush if @io.respond_to?(:flush)
      # Could compute checksum here by re-reading, or maintain on the fly; omit for v1
      @manifest&.complete_part!(index: @part_index, checksum: nil)
      @file_seq += 1
    end

    def next_part_path
      ext = 'jsonl'
      filename = format("%s-part-%06d.%s", @prefix, @file_seq, ext)
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
  end
end



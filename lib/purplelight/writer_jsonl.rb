# frozen_string_literal: true

require 'json'
require 'zlib'
require 'fileutils'

begin
  require 'zstds'
rescue LoadError
  # zstd not available; will fallback to gzip
end

begin
  require 'zstd-ruby'
rescue LoadError
  # alternative zstd gem not available
end

module Purplelight
  # WriterJSONL writes newline-delimited JSON with optional compression.
  class WriterJSONL
    DEFAULT_ROTATE_BYTES = 256 * 1024 * 1024

    def initialize(directory:, prefix:, compression: :zstd, rotate_bytes: DEFAULT_ROTATE_BYTES, logger: nil,
                   manifest: nil, compression_level: nil)
      @directory = directory
      @prefix = prefix
      @compression = compression
      @rotate_bytes = rotate_bytes
      @logger = logger
      @manifest = manifest
      env_level = ENV['PL_ZSTD_LEVEL']&.to_i
      @compression_level = compression_level || (env_level&.positive? ? env_level : nil)

      @part_index = nil
      @io = nil
      @bytes_written = 0
      @rows_written = 0
      @file_seq = 0
      @closed = false

      @effective_compression = determine_effective_compression(@compression)
      if @logger
        level_disp = @compression_level || (ENV['PL_ZSTD_LEVEL']&.to_i if @effective_compression.to_s == 'zstd')
        @logger.info("WriterJSONL using compression='#{@effective_compression}' level='#{level_disp || 'default'}'")
      end
      return unless @effective_compression.to_s != @compression.to_s

      @logger&.warn("requested compression '#{@compression}' not available; using '#{@effective_compression}'")
    end

    def write_many(batch)
      ensure_open!

      chunk_threshold = ENV['PL_WRITE_CHUNK_BYTES']&.to_i || (8 * 1024 * 1024)
      total_bytes = 0
      rows = 0

      if batch.is_a?(String)
        # Fast-path: writer received a preassembled buffer string
        buffer = batch
        rows = buffer.count("\n")
        write_buffer(buffer)
        total_bytes = buffer.bytesize
      elsif batch.first.is_a?(String)
        # Join and write in chunks to avoid large intermediate allocations
        chunk = +''
        chunk_bytes = 0
        batch.each do |line|
          chunk << line
          rows += 1
          chunk_bytes += line.bytesize
          next unless chunk_bytes >= chunk_threshold

          write_buffer(chunk)
          total_bytes += chunk.bytesize
          chunk = +''
          chunk_bytes = 0
        end
        unless chunk.empty?
          write_buffer(chunk)
          total_bytes += chunk.bytesize
        end
      else
        # Fallback: encode docs here (JSON.fast_generate preferred) and write in chunks
        chunk = +''
        chunk_bytes = 0
        batch.each do |doc|
          line = "#{JSON.fast_generate(doc)}\n"
          rows += 1
          chunk << line
          chunk_bytes += line.bytesize
          next unless chunk_bytes >= chunk_threshold

          write_buffer(chunk)
          total_bytes += chunk.bytesize
          chunk = +''
          chunk_bytes = 0
        end
        unless chunk.empty?
          write_buffer(chunk)
          total_bytes += chunk.bytesize
        end
      end

      @rows_written += rows
      @manifest&.add_progress_to_part!(index: @part_index, rows_delta: rows, bytes_delta: total_bytes)
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
        # Prefer zstd-ruby if available, else ruby-zstds
        if Object.const_defined?(:Zstd) && defined?(::Zstd::StreamWriter)
          level = @compression_level || 3
          return ::Zstd::StreamWriter.new(raw, level: level)
        elsif defined?(ZSTDS)
          level = @compression_level || 3
          return ZSTDS::Stream::Writer.new(raw, compression_level: level)
        end

        @logger&.warn('zstd gems not loaded; falling back to gzip')
        level = @compression_level || Zlib::DEFAULT_COMPRESSION
        Zlib::GzipWriter.new(raw, level)
      when 'gzip'
        level = @compression_level || 1
        Zlib::GzipWriter.new(raw, level)
      when 'none'
        raw
      else
        raise ArgumentError, "unknown compression: #{@compression}"
      end
    end

    def write_buffer(buffer)
      t = Thread.current[:pl_telemetry]&.start(:write_time)
      @io.write(buffer)
      Thread.current[:pl_telemetry]&.finish(:write_time, t)
      @bytes_written += buffer.bytesize
      rotate_if_needed
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
      @io.flush if @io.respond_to?(:flush)
      # Could compute checksum here by re-reading, or maintain on the fly; omit for v1
      @manifest&.complete_part!(index: @part_index, checksum: nil)
      @file_seq += 1
    end

    def next_part_path
      ext = 'jsonl'
      filename = format('%<prefix>s-part-%<seq>06d.%<ext>s', prefix: @prefix, seq: @file_seq, ext: ext)
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
  end
end

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
    # no zstd backend; gzip fallback
  end
end
# simplecov:enable

module Purplelight
  # WriterJSONL writes newline-delimited JSON with optional compression.
  class WriterJSONL
    DEFAULT_ROTATE_BYTES = 256 * 1024 * 1024
    DEFAULT_ZSTD_LEVEL = 3
    DEFAULT_GZIP_LEVEL = 1
    EncodedBatch = Data.define(:data, :rows, :bytes)

    # Allocates globally unique output part numbers across writer threads.
    class PartSequence
      def initialize(next_value = 0)
        @next_value = next_value
        @mutex = Mutex.new
      end

      def next
        @mutex.synchronize do
          value = @next_value
          @next_value += 1
          value
        end
      end
    end

    def initialize(directory:, prefix:, compression: :zstd, rotate_bytes: DEFAULT_ROTATE_BYTES, logger: nil,
                   manifest: nil, compression_level: nil, write_chunk_bytes: nil, part_sequence: nil)
      @directory = directory
      @prefix = prefix
      @compression = compression
      @rotate_bytes = rotate_bytes
      @logger = logger
      @manifest = manifest
      env_level = ENV['PL_ZSTD_LEVEL']&.to_i
      @compression_level = compression_level || (env_level&.positive? ? env_level : nil)
      @write_chunk_bytes = write_chunk_bytes
      @part_sequence = part_sequence

      @part_index = nil
      @io = nil
      @bytes_written = 0
      @file_seq = manifest ? manifest.parts.length : 0
      @closed = false
      @thread_telemetry = false

      @effective_compression = determine_effective_compression(@compression)
      @json_state = JSON::Ext::Generator::State.new(ascii_only: false, max_nesting: false)
      if @logger
        level_disp = @compression_level
        @logger.info("WriterJSONL using compression='#{@effective_compression}' level='#{level_disp || 'default'}'")
      end
      return unless @effective_compression.to_s != @compression.to_s

      @logger&.warn("requested compression '#{@compression}' not available; using '#{@effective_compression}'")
    end

    def write_many(batch)
      ensure_open!

      chunk_threshold = @write_chunk_bytes || ENV['PL_WRITE_CHUNK_BYTES']&.to_i || (8 * 1024 * 1024)
      total_bytes = 0
      rows = 0

      if batch.is_a?(EncodedBatch)
        write_buffer(batch.data)
        rows = batch.rows
        total_bytes = batch.bytes
      elsif batch.is_a?(String)
        # Fast path for callers that don't provide row metadata.
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
          json = @json_state.generate(doc)
          rows += 1
          bytes = json.bytesize + 1
          chunk << json
          chunk << "\n"
          chunk_bytes += bytes
          next unless chunk_bytes >= chunk_threshold

          write_buffer(chunk)
          total_bytes += chunk_bytes
          chunk = +''
          chunk_bytes = 0
        end
        unless chunk.empty?
          write_buffer(chunk)
          total_bytes += chunk_bytes
        end
      end

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
      @part_index = @manifest.open_part!(path) if @manifest
      # The compression stream owns and closes this file handle.
      # rubocop:disable Style/FileOpen
      raw = File.open(path, 'wb')
      # rubocop:enable Style/FileOpen
      @io = build_compressed_io(raw)
      @bytes_written = 0
    end

    def build_compressed_io(raw)
      case @effective_compression.to_s
      when 'zstd'
        # Prefer zstd-ruby if available, else ruby-zstds
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
        raise ArgumentError, "unknown compression: #{@compression}"
      end
    end

    def write_buffer(buffer)
      telemetry = @thread_telemetry
      if telemetry.equal?(false)
        telemetry = Thread.current[:pl_telemetry]
        @thread_telemetry = telemetry
      end
      if telemetry
        ticket = telemetry.start(:write_time)
        @io.write(buffer)
        telemetry.finish(:write_time, ticket)
      else
        @io.write(buffer)
      end
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
      sequence = @part_sequence ? @part_sequence.next : @file_seq
      filename = format('%<prefix>s-part-%<seq>06d.%<ext>s', prefix: @prefix, seq: sequence, ext: ext)
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
  end
end

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
    DICTIONARY_CARDINALITY_LIMIT = 16
    DICTIONARY_MIN_REPETITIONS = 4

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
      @arrow_schema = nil
      @arrow_data_types = nil
      @dictionary_paths = nil
      @dictionary_paths_resolved = false
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

    def build_record_batch(documents)
      @columns ||= infer_columns(documents)
      resolve_dictionary_paths = !@dictionary_paths_resolved
      dictionary_paths = [] if resolve_dictionary_paths && @columns.none? { |name| name.include?('.') }
      @dictionary_paths_resolved = true if resolve_dictionary_paths
      arrays = @columns.each_with_index.map do |name, index|
        values = documents.map { |document| extract_value(document, name) }
        array = build_arrow_array(values, @arrow_data_types&.at(index))
        if dictionary_paths
          data_type = array.value_data_type
          path = list_dictionary_path(name, data_type)
          path ||= name if data_type.is_a?(Arrow::StringDataType) && low_cardinality_strings?(values)
          dictionary_paths << path if path
        end
        array
      end
      @dictionary_paths = dictionary_paths if resolve_dictionary_paths
      batch = if @arrow_schema
                Arrow::RecordBatch.new(@arrow_schema, arrays)
              else
                Arrow::RecordBatch.new(@columns.zip(arrays).to_h)
              end
      unless @arrow_schema
        @arrow_schema = batch.schema
        @arrow_data_types = @arrow_schema.fields.map(&:data_type)
      end
      batch
    end

    def build_arrow_array(values, data_type = nil)
      first = values.find { |value| !value.nil? }
      if first.is_a?(BSON::ObjectId)
        return build_object_id_array(values) if values.all? { |value| value.nil? || value.is_a?(BSON::ObjectId) }

        return build_string_array(values)
      end
      return build_string_array(values) if data_type.is_a?(Arrow::StringDataType) ||
                                           first.is_a?(String) || first.is_a?(Hash)
      if (data_type.is_a?(Arrow::ListDataType) || first.is_a?(Array)) &&
         values.all? { |value| value.nil? || value.is_a?(Array) } &&
         (data_type || values.any? { |value| value && !value.empty? })
        return build_list_array(values, data_type)
      end
      if (data_type.is_a?(Arrow::BooleanDataType) || first.equal?(true) || first.equal?(false)) &&
         values.all? { |value| value.nil? || value.equal?(true) || value.equal?(false) }
        return build_boolean_array(values)
      end

      integer_spec = integer_array_spec(data_type)
      integer_values = (integer_spec || first.is_a?(Integer)) &&
                       values.all? { |value| value.nil? || value.is_a?(Integer) }
      return build_integer_array(values, data_type, integer_spec) if integer_values
      if (data_type.is_a?(Arrow::DoubleDataType) || first.is_a?(Float)) &&
         values.all? { |value| value.nil? || value.is_a?(Float) }
        return build_double_array(values)
      end

      data_type ? data_type.build_array(values) : Arrow::ArrayBuilder.build(values)
    end

    def build_string_array(values)
      offsets = Array.new(values.length + 1, 0)
      data = +''.b
      values.each_with_index do |value, index|
        data << (value.is_a?(String) ? value : value.to_s) unless value.nil?
        offsets[index + 1] = data.bytesize
      end
      validity, null_count = build_validity(values)
      Arrow::StringArray.new(
        values.length,
        Arrow::Buffer.new(offsets.pack('l<*').freeze),
        Arrow::Buffer.new(data.freeze),
        validity,
        null_count
      )
    end

    def build_object_id_array(values)
      offsets = Array.new(values.length + 1, 0)
      raw = String.new(capacity: values.length * 12, encoding: Encoding::BINARY)
      values.each_with_index do |value, index|
        # #marshal_dump is BSON's public accessor for an ObjectId's 12 raw bytes.
        raw << value.marshal_dump if value
        offsets[index + 1] = raw.bytesize * 2
      end
      data = raw.unpack1('H*').force_encoding(Encoding::UTF_8)
      validity, null_count = build_validity(values)
      Arrow::StringArray.new(
        values.length,
        Arrow::Buffer.new(offsets.pack('l<*').freeze),
        Arrow::Buffer.new(data.freeze),
        validity,
        null_count
      )
    end

    def build_list_array(values, data_type)
      offsets = Array.new(values.length + 1, 0)
      flattened = []
      values.each_with_index do |value, index|
        flattened.concat(value) if value
        offsets[index + 1] = flattened.length
      end
      child_type = data_type&.field&.data_type
      child = build_arrow_array(flattened, child_type)
      data_type ||= Arrow::ListDataType.new(child.value_data_type)
      validity, null_count = build_validity(values)
      Arrow::ListArray.new(
        data_type,
        values.length,
        Arrow::Buffer.new(offsets.pack('l<*').freeze),
        child,
        validity,
        null_count
      )
    end

    def build_boolean_array(values)
      data = "\0".b * ((values.length + 7) / 8)
      values.each_with_index do |value, index|
        next unless value

        byte_index = index >> 3
        data.setbyte(byte_index, data.getbyte(byte_index) | (1 << (index & 7)))
      end
      validity, null_count = build_validity(values)
      Arrow::BooleanArray.new(values.length, Arrow::Buffer.new(data.freeze), validity, null_count)
    end

    def build_integer_array(values, data_type, spec)
      non_null_values = values.compact
      minimum, maximum = non_null_values.minmax
      data_type ||= infer_integer_data_type(minimum, maximum)
      spec ||= integer_array_spec(data_type)
      return Arrow::ArrayBuilder.build(values) unless spec

      array_class, pack_directive, lower_bound, upper_bound = spec
      outside_bounds = minimum && (minimum < lower_bound || maximum > upper_bound)
      raise RangeError, "integer range #{minimum}..#{maximum} exceeds #{data_type}" if outside_bounds

      packed_values = if non_null_values.length == values.length
                        values
                      else
                        values.map { |value| value.nil? ? 0 : value }
                      end
      validity, null_count = build_validity(values)
      array_class.new(
        values.length,
        Arrow::Buffer.new(packed_values.pack(pack_directive).freeze),
        validity,
        null_count
      )
    end

    def build_double_array(values)
      packed_values = if values.include?(nil)
                        values.map { |value| value.nil? ? 0.0 : value }
                      else
                        values
                      end
      validity, null_count = build_validity(values)
      Arrow::DoubleArray.new(
        values.length,
        Arrow::Buffer.new(packed_values.pack('E*').freeze),
        validity,
        null_count
      )
    end

    def build_validity(values)
      null_count = values.count(nil)
      return [nil, 0] if null_count.zero?

      bytes = "\0".b * ((values.length + 7) / 8)
      values.each_with_index do |value, index|
        next if value.nil?

        byte_index = index >> 3
        bytes.setbyte(byte_index, bytes.getbyte(byte_index) | (1 << (index & 7)))
      end
      [Arrow::Buffer.new(bytes.freeze), null_count]
    end

    def infer_integer_data_type(minimum, maximum)
      if minimum.negative?
        return Arrow::Int8DataType.new if minimum >= -128 && maximum <= 127
        return Arrow::Int16DataType.new if minimum >= -32_768 && maximum <= 32_767
        return Arrow::Int32DataType.new if minimum >= -2_147_483_648 && maximum <= 2_147_483_647

        int64_range = minimum >= -9_223_372_036_854_775_808 && maximum <= 9_223_372_036_854_775_807
        return Arrow::Int64DataType.new if int64_range
      else
        return Arrow::UInt8DataType.new if maximum <= 255
        return Arrow::UInt16DataType.new if maximum <= 65_535
        return Arrow::UInt32DataType.new if maximum <= 4_294_967_295
        return Arrow::UInt64DataType.new if maximum <= 18_446_744_073_709_551_615
      end
      nil
    end

    def integer_array_spec(data_type)
      case data_type
      when Arrow::UInt8DataType
        [Arrow::UInt8Array, 'C*', 0, 255]
      when Arrow::UInt16DataType
        [Arrow::UInt16Array, 'S<*', 0, 65_535]
      when Arrow::UInt32DataType
        [Arrow::UInt32Array, 'L<*', 0, 4_294_967_295]
      when Arrow::UInt64DataType
        [Arrow::UInt64Array, 'Q<*', 0, 18_446_744_073_709_551_615]
      when Arrow::Int8DataType
        [Arrow::Int8Array, 'c*', -128, 127]
      when Arrow::Int16DataType
        [Arrow::Int16Array, 's<*', -32_768, 32_767]
      when Arrow::Int32DataType
        [Arrow::Int32Array, 'l<*', -2_147_483_648, 2_147_483_647]
      when Arrow::Int64DataType
        [Arrow::Int64Array, 'q<*', -9_223_372_036_854_775_808, 9_223_372_036_854_775_807]
      end
    end

    def list_dictionary_path(name, data_type)
      return unless data_type.is_a?(Arrow::ListDataType)

      path = name.dup
      while data_type.is_a?(Arrow::ListDataType)
        path << '.list.element'
        data_type = data_type.field.data_type
      end
      return if data_type.is_a?(Arrow::BooleanDataType) || data_type.is_a?(Arrow::NullDataType)

      path
    end

    def low_cardinality_strings?(values)
      first = values.find { |value| !value.nil? }
      return false unless first.is_a?(String)

      distinct = {}
      count = 0
      values.each do |value|
        next if value.nil?
        return false unless value.is_a?(String)

        count += 1
        distinct[value] = true
        return false if distinct.length > DICTIONARY_CARDINALITY_LIMIT
      end
      !distinct.empty? && count >= distinct.length * DICTIONARY_MIN_REPETITIONS
    end

    def write_record_batch(batch, path)
      unless @pq_writer
        properties = build_writer_properties_for_compression(@compression)
        properties ||= Parquet::WriterProperties.new
        if @dictionary_paths
          properties.disable_dictionary
          @dictionary_paths.each { |column_path| properties.enable_dictionary(column_path) }
        end
        properties.max_row_group_length = @row_group_size
        @pq_writer = Parquet::ArrowFileWriter.open(batch.schema, path, properties)
      end
      @pq_writer.write(batch)
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
          batch_a = build_record_batch(part_a)
          Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

          t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
          write_record_batch(batch_a, @writer_path)
          Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
          @manifest&.add_progress_to_part!(index: @part_index, rows_delta: part_a.length, bytes_delta: 0)
          @rows_in_current_file += part_a.length

          finalize_current_part!
          ensure_open!

          part_b = group.drop(remaining_allowed)
          t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
          batch_b = build_record_batch(part_b)
          Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

          t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
          write_record_batch(batch_b, @writer_path)
          Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
          @manifest&.add_progress_to_part!(index: @part_index, rows_delta: part_b.length, bytes_delta: 0)
          @rows_in_current_file += part_b.length
        else
          t_tbl = Thread.current[:pl_telemetry]&.start(:parquet_table_build_time)
          batch = build_record_batch(group)
          Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

          t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
          write_record_batch(batch, @writer_path)
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
      batch = build_record_batch(take_buffered(remaining))
      Thread.current[:pl_telemetry]&.finish(:parquet_table_build_time, t_tbl)

      ensure_open!
      # Pre-rotate to avoid exceeding rotate_rows on this final write
      if @rotate_rows && !@single_file && @rows_in_current_file.positive? && (@rows_in_current_file + remaining) > @rotate_rows
        finalize_current_part!
        ensure_open!
      end

      t_w = Thread.current[:pl_telemetry]&.start(:parquet_write_time)
      write_record_batch(batch, @writer_path)
      Thread.current[:pl_telemetry]&.finish(:parquet_write_time, t_w)
      rows_written = batch.n_rows
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

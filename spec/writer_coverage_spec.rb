# frozen_string_literal: true

require 'spec_helper'
require 'csv'
require 'stringio'
require 'tmpdir'

# Test-local doubles intentionally live beside their behavioral contracts.
# rubocop:disable Lint/ConstantDefinitionInBlock
RSpec.describe 'writer path coverage' do
  class CoverageCompressedWriter
    def initialize(io, **)
      @io = io
    end

    def write(data) = @io.write(data)
    def flush = @io.flush
    def close = @io.close
  end

  def with_env(name, value)
    previous = ENV.fetch(name, nil)
    value.nil? ? ENV.delete(name) : ENV[name] = value
    yield
  ensure
    previous.nil? ? ENV.delete(name) : ENV[name] = previous
  end

  def zstds_module
    stream = Module.new
    stream.const_set(:Writer, CoverageCompressedWriter)
    Module.new.tap { |backend| backend.const_set(:Stream, stream) }
  end

  describe Purplelight::WriterJSONL do
    it 'writes raw strings, encoded lines, documents, manifests, and rotations' do
      Dir.mktmpdir do |directory|
        manifest = instance_double(Purplelight::Manifest)
        allow(manifest).to receive(:parts).and_return([])
        allow(manifest).to receive(:open_part!).and_return(0)
        allow(manifest).to receive(:add_progress_to_part!)
        allow(manifest).to receive(:complete_part!)
        telemetry = Purplelight::Telemetry.new
        Thread.current[:pl_telemetry] = telemetry
        with_env('PL_WRITE_CHUNK_BYTES', '1') do
          writer = described_class.new(directory:, prefix: 'records', compression: :none, rotate_bytes: 1,
                                       manifest:)
          writer.write_many("{\"raw\":true}\n")
          writer.write_many(["{\"line\":1}\n", "{\"line\":2}\n"])
          writer.write_many([{ 'document' => 1 }, { 'document' => 2 }])
          writer.close
          writer.close
        end
        expect(manifest).to have_received(:add_progress_to_part!).exactly(3).times
        expect(telemetry.timers).to include(:write_time, :rotate_time)
      ensure
        Thread.current[:pl_telemetry] = nil
      end
    end

    it 'covers compression selection, logging, levels, and path suffixes' do
      Dir.mktmpdir do |directory|
        logger = instance_double(Logger, info: nil, warn: nil)
        with_env('PL_ZSTD_LEVEL', '4') do
          writer = described_class.new(directory:, prefix: 'zstd', compression: :zstd, logger:)
          writer.write_many([])
          writer.close
        end
        gzip = described_class.new(directory:, prefix: 'gzip', compression: :gzip, compression_level: 2)
        gzip.write_many([])
        gzip.close
        expect(Dir[File.join(directory, '*.zst')]).not_to be_empty
        expect(Dir[File.join(directory, '*.gz')]).not_to be_empty
        expect(logger).to have_received(:info)
      end
    end

    it 'uses ZSTDS and gzip fallback paths when zstd-ruby is unavailable' do
      hide_const('Zstd')
      stub_const('ZSTDS', zstds_module)
      writer = described_class.new(directory: Dir.mktmpdir, prefix: 'zstds', compression: :zstd)
      writer.write_many([{ a: 1 }])
      writer.close

      hide_const('ZSTDS')
      logger = instance_double(Logger, warn: nil, info: nil)
      fallback = described_class.new(directory: Dir.mktmpdir, prefix: 'fallback', compression: :zstd, logger:)
      fallback.write_many([{ a: 1 }])
      fallback.close
      expect(fallback.send(:determine_effective_compression, :auto)).to eq(:gzip)
      expect(logger).to have_received(:warn).at_least(:once)
    end

    it 'flushes the final line chunk' do
      Dir.mktmpdir do |directory|
        with_env('PL_WRITE_CHUNK_BYTES', '1024') do
          writer = described_class.new(directory:, prefix: 'lines', compression: :none)
          writer.write_many(%W[one\n two\n])
          writer.close
        end
      end
    end

    it 'rejects an unknown effective compression and safely rotates unopened writers' do
      writer = described_class.new(directory: Dir.mktmpdir, prefix: 'invalid', compression: :none)
      writer.instance_variable_set(:@effective_compression, :invalid)
      expect { writer.send(:build_compressed_io, StringIO.new) }.to raise_error(ArgumentError, /unknown compression/)
      expect(writer.send(:rotate!)).to be_nil
    end
    it 'allocates shared part numbers atomically' do
      sequence = described_class::PartSequence.new(7)
      numbers = Queue.new
      workers = Array.new(4) do
        Thread.new { 100.times { numbers << sequence.next } }
      end
      workers.each(&:join)

      expect(Array.new(400) { numbers.pop }.sort).to eq((7...407).to_a)
    end

    it 'covers nil rotation, telemetry-free rotation, fallback logging, and both auto backends' do
      previous_telemetry = Thread.current[:pl_telemetry]
      Thread.current[:pl_telemetry] = nil
      Dir.mktmpdir do |directory|
        unbounded = described_class.new(directory:, prefix: 'unbounded', compression: :none, rotate_bytes: nil)
        unbounded.write_many([{ value: 1 }])
        unbounded.close

        rotating = described_class.new(directory:, prefix: 'rotating', compression: :none, rotate_bytes: 1)
        rotating.write_many([{ value: 1 }])
        rotating.close

        selector = described_class.new(directory:, prefix: 'selector', compression: :none)
        expect(selector.send(:determine_effective_compression, :auto)).to eq(:zstd)
        selector.close

        hide_const('Zstd')
        stub_const('ZSTDS', zstds_module)
        expect(selector.send(:determine_effective_compression, :auto)).to eq(:zstd)
        hide_const('ZSTDS')

        fallback = described_class.new(directory:, prefix: 'fallback-nil-logger', compression: :zstd)
        fallback.write_many([{ value: 1 }])
        fallback.close
      end
    ensure
      Thread.current[:pl_telemetry] = previous_telemetry
    end
  end

  describe Purplelight::WriterCSV do
    it 'writes inferred and explicit columns, skips strings, serializes nested values, and rotates' do
      Dir.mktmpdir do |directory|
        manifest = instance_double(Purplelight::Manifest)
        allow(manifest).to receive(:parts).and_return([])
        allow(manifest).to receive(:open_part!).and_return(0)
        allow(manifest).to receive(:add_progress_to_part!)
        allow(manifest).to receive(:complete_part!)
        telemetry = Purplelight::Telemetry.new
        Thread.current[:pl_telemetry] = telemetry
        writer = described_class.new(directory:, prefix: 'records', compression: :none, rotate_bytes: 1,
                                     manifest:)
        writer.write_many([{ _id: 1, nested: { a: 1 }, list: [1, 2] }, 'already encoded'])
        writer.write_many([])
        writer.close
        writer.close
        expect(manifest).to have_received(:add_progress_to_part!).with(index: 0, rows_delta: 1, bytes_delta: 0)
        expect(telemetry.timers).to include(:rotate_time)
      ensure
        Thread.current[:pl_telemetry] = nil
      end
    end

    it 'supports single files without headers, gzip, zstd, and environment levels' do
      Dir.mktmpdir do |directory|
        with_env('PL_ZSTD_LEVEL', '5') do
          zstd = described_class.new(directory:, prefix: 'records', compression: :zstd, single_file: true,
                                     columns: %i[_id value], headers: false)
          zstd.write_many([{ '_id' => 1, 'value' => nil }])
          zstd.close
        end
        gzip = described_class.new(directory:, prefix: 'gzip', compression: :gzip)
        gzip.write_many([{ '_id' => 1 }])
        gzip.close
        expect(Dir[File.join(directory, '*.zst')]).not_to be_empty
        expect(Dir[File.join(directory, '*.gz')]).not_to be_empty
      end
    end

    it 'covers alternate and unavailable compression backends' do
      hide_const('Zstd')
      stub_const('ZSTDS', zstds_module)
      writer = described_class.new(directory: Dir.mktmpdir, prefix: 'zstds', compression: :zstd)
      writer.write_many([{ '_id' => 1 }])
      writer.close

      hide_const('ZSTDS')
      logger = instance_double(Logger, warn: nil)
      fallback = described_class.new(directory: Dir.mktmpdir, prefix: 'fallback', compression: :zstd, logger:)
      fallback.write_many([{ '_id' => 1 }])
      fallback.close
      expect(fallback.send(:determine_effective_compression, :auto)).to eq(:gzip)
      expect(logger).to have_received(:warn).at_least(:once)
    end

    it 'auto-selects both available zstd backends' do
      writer = described_class.new(directory: Dir.mktmpdir, prefix: 'auto', compression: :none)
      expect(writer.send(:determine_effective_compression, :auto)).to eq(:zstd)
      hide_const('Zstd')
      stub_const('ZSTDS', zstds_module)
      expect(writer.send(:determine_effective_compression, :auto)).to eq(:zstd)
    end

    it 'delegates counting IO behavior and rejects unknown compression' do
      raw = StringIO.new
      counting = described_class::CountingIO.new(raw)
      counting << 'abc'
      counting.flush
      expect(counting.string).to eq('abc')
      expect(counting.respond_to?(:string)).to be true
      expect(counting.bytes_written).to eq(3)

      writer = described_class.new(directory: Dir.mktmpdir, prefix: 'invalid', compression: :none)
      writer.instance_variable_set(:@effective_compression, :invalid)
      expect { writer.send(:build_compressed_io, StringIO.new) }.to raise_error(ArgumentError, /unknown compression/)
      expect(writer.send(:rotate!)).to be_nil
    end
    it 'matches CSV escaping and preserves false and nil values' do
      Dir.mktmpdir do |directory|
        columns = %w[_id false_value nil_value empty comma quote newline nested]
        document = {
          '_id' => 1,
          'false_value' => false,
          'nil_value' => nil,
          'empty' => '',
          'comma' => 'a,b',
          'quote' => 'a"b',
          'newline' => "a\nb",
          'nested' => { 'a' => 1 }
        }
        writer = described_class.new(directory:, prefix: 'escaping', compression: :none, single_file: true,
                                     columns:, headers: false)
        writer.write_many([document])
        writer.close

        values = [1, false, nil, '', 'a,b', 'a"b', "a\nb", JSON.generate('a' => 1)]
        expect(File.read(File.join(directory, 'escaping.csv'))).to eq(CSV.generate_line(values))
      end
    end

    it 'covers lazy input, empty inference, nil rotation, unopened close, and telemetry-free fallback rotation' do
      previous_telemetry = Thread.current[:pl_telemetry]
      Thread.current[:pl_telemetry] = nil
      Dir.mktmpdir do |directory|
        lazy = described_class.new(directory:, prefix: 'lazy', compression: :none, rotate_bytes: nil,
                                   headers: false)
        lazy.write_many([{ '_id' => 1 }].each)
        lazy.close

        empty = described_class.new(directory:, prefix: 'empty', compression: :none)
        empty.write_many([])
        empty.close

        unopened = described_class.new(directory:, prefix: 'unopened', compression: :none)
        unopened.close

        rotating = described_class.new(directory:, prefix: 'rotate', compression: :none, rotate_bytes: 1)
        rotating.write_many([{ '_id' => 1, 'value' => 'one' }])
        rotating.close
        Dir[File.join(directory, 'rotate-part-*.csv')].each do |path|
          expect(File.foreach(path).first.chomp).to eq('_id,value')
        end

        hide_const('Zstd')
        hide_const('ZSTDS')
        fallback = described_class.new(directory:, prefix: 'fallback-nil-logger', compression: :zstd)
        fallback.write_many([{ '_id' => 1 }])
        fallback.close
      end
    ensure
      Thread.current[:pl_telemetry] = previous_telemetry
    end
  end

  describe Purplelight::WriterParquet do
    it 'streams row groups, splits rotations, records progress, and normalizes BSON IDs' do
      Dir.mktmpdir do |directory|
        manifest = instance_double(Purplelight::Manifest)
        allow(manifest).to receive(:parts).and_return([])
        allow(manifest).to receive(:open_part!).and_return(0)
        allow(manifest).to receive(:add_progress_to_part!)
        allow(manifest).to receive(:complete_part!)
        telemetry = Purplelight::Telemetry.new
        Thread.current[:pl_telemetry] = telemetry
        writer = described_class.new(directory:, prefix: 'records', compression: :zstd, row_group_size: 2,
                                     single_file: false, rotate_rows: 3, manifest:)
        documents = Array.new(5) { |index| { '_id' => BSON::ObjectId.new, 'value' => index } }
        writer.write_many(documents)
        writer.close
        writer.close
        expect(Dir[File.join(directory, '*.parquet')].length).to be >= 2
        expect(manifest).to have_received(:add_progress_to_part!).at_least(:twice)
        expect(telemetry.timers).to include(:parquet_table_build_time, :parquet_write_time)
      ensure
        Thread.current[:pl_telemetry] = nil
      end
    end

    it 'writes a final partial group and supports every current compression name' do
      Dir.mktmpdir do |directory|
        writer = described_class.new(directory:, prefix: 'single', compression: :none, row_group_size: 10)
        writer.write_many([{ value: 1 }])
        writer.close
        expect(File).to exist(File.join(directory, 'single.parquet'))

        %i[none gzip snappy zstd].each do |compression|
          candidate = described_class.new(directory:, prefix: compression.to_s, compression:)
          expect(candidate.send(:build_writer_properties_for_compression, compression)).to be_a(Parquet::WriterProperties)
          candidate.close
        end
        expect(writer.send(:build_writer_properties_for_compression, nil)).to be_nil
        expect(writer.send(:build_writer_properties_for_compression, :invalid)).to be_nil
      end
    end

    it 'round trips nullable primitive and list columns across row groups' do
      Dir.mktmpdir do |directory|
        object_ids = 3.times.map { |index| BSON::ObjectId.from_string(format('%024x', index + 1)) }
        documents = [
          {
            '_id' => object_ids[0], 'mixed_id' => object_ids[0],
            'u8' => 0, 'u16' => 0, 'u32' => 0, 'u64' => 0,
            'i8' => -128, 'i16' => -129, 'i32' => -32_769, 'i64' => -2_147_483_649,
            'too_large' => 18_446_744_073_709_551_616, 'too_small' => -9_223_372_036_854_775_809,
            'active' => true, 'ratio' => 1.25, 'name' => 'alpha', 'nested' => { 'x' => 1 },
            'tags' => %w[a b], 'numbers' => [1, 2], 'nested_numbers' => [[1], [2]],
            'object_ids' => [object_ids[0]],
            'hashes' => [{ 'x' => 1 }], 'mixed_numeric' => 1, 'mixed_float' => 1.5,
            'empty_lists' => [], 'date' => Date.new(2026, 1, 1), 'time' => Time.utc(2026, 1, 1, 0, 0, 1)
          },
          {
            '_id' => object_ids[1], 'mixed_id' => 'literal',
            'u8' => 255, 'u16' => 256, 'u32' => 65_536, 'u64' => 4_294_967_296,
            'i8' => 127, 'i16' => 32_767, 'i32' => 2_147_483_647, 'i64' => 9_223_372_036_854_775_807,
            'too_large' => 18_446_744_073_709_551_616, 'too_small' => -9_223_372_036_854_775_809,
            'active' => false, 'ratio' => -2.5, 'name' => '', 'nested' => { 'x' => 2 },
            'tags' => [], 'numbers' => [], 'nested_numbers' => [[]],
            'object_ids' => [object_ids[1]],
            'hashes' => [{ 'x' => 2 }], 'mixed_numeric' => 2.5, 'mixed_float' => 2,
            'empty_lists' => [], 'date' => Date.new(2026, 1, 2), 'time' => Time.utc(2026, 1, 1, 0, 0, 2)
          },
          {
            '_id' => nil, 'mixed_id' => nil,
            'u8' => nil, 'u16' => nil, 'u32' => nil, 'u64' => nil,
            'i8' => nil, 'i16' => nil, 'i32' => nil, 'i64' => nil,
            'too_large' => nil, 'too_small' => nil,
            'active' => nil, 'ratio' => nil, 'name' => nil, 'nested' => nil,
            'tags' => nil, 'numbers' => nil, 'nested_numbers' => nil,
            'object_ids' => nil,
            'hashes' => nil, 'mixed_numeric' => nil, 'mixed_float' => nil,
            'empty_lists' => nil, 'date' => nil, 'time' => nil
          },
          {
            '_id' => object_ids[2], 'mixed_id' => object_ids[2],
            'u8' => 7, 'u16' => 7, 'u32' => 7, 'u64' => 7,
            'i8' => 7, 'i16' => 7, 'i32' => 7, 'i64' => 7,
            'too_large' => 18_446_744_073_709_551_616, 'too_small' => -9_223_372_036_854_775_809,
            'active' => true, 'ratio' => 3.5, 'name' => 'omega', 'nested' => { 'x' => 4 },
            'tags' => ['c'], 'numbers' => [3], 'nested_numbers' => [[3]],
            'object_ids' => [object_ids[2]],
            'hashes' => [{ 'x' => 4 }], 'mixed_numeric' => 3.5, 'mixed_float' => 4.5,
            'empty_lists' => [], 'date' => Date.new(2026, 1, 4), 'time' => Time.utc(2026, 1, 1, 0, 0, 4)
          }
        ]
        writer = described_class.new(directory:, prefix: 'types', compression: :zstd, row_group_size: 2)
        writer.write_many(documents)
        writer.close

        table = Arrow::Table.load(File.join(directory, 'types.parquet'), format: :parquet)
        normalize = lambda do |value|
          value.is_a?(Arrow::Array) ? value.to_a.map { |item| normalize.call(item) } : value
        end
        actual = table.column_names.to_h do |name|
          [name, table[name].to_a.map { |value| normalize.call(value) }]
        end
        expected = documents.first.keys.to_h { |name| [name, documents.map { |document| document[name] }] }
        %w[_id mixed_id too_large too_small nested empty_lists].each do |name|
          expected[name] = expected[name].map { |value| value&.to_s }
        end
        expected['hashes'] = expected['hashes'].map { |values| values&.map(&:to_s) }
        expected['object_ids'] = expected['object_ids'].map { |values| values&.map(&:to_s) }
        expected['mixed_numeric'] = expected['mixed_numeric'].map { |value| value&.to_f }
        expected['mixed_float'] = expected['mixed_float'].map { |value| value&.to_f }

        expect(actual).to eq(expected)
      end
    end

    it 'uses read-safe dictionary paths without changing round-trip values' do
      Dir.mktmpdir do |directory|
        documents = Array.new(68) do |index|
          object_id = BSON::ObjectId.from_string(format('%024x', index + 1))
          {
            'category' => ((index % 17).zero? ? nil : "category-#{index % 4}"),
            'dates' => [Date.new(2026, 1, 1) + (index % 7)],
            'flags' => [index.even?, index.odd?],
            'hash_lists' => [{ 'index' => index }],
            'labels' => ["label-#{index % 8}"],
            'mixed' => (index.even? ? "literal-#{index}" : object_id),
            'nested' => { 'index' => index },
            'nested_numbers' => [[index], [index + 1]],
            'numbers' => [index, index + 1],
            'object_ids' => [object_id],
            'unique_text' => "row-#{index}"
          }
        end
        writer = described_class.new(directory:, prefix: 'read-safe', compression: :zstd, row_group_size: 68)
        writer.write_many(documents)
        writer.close

        expect(writer.instance_variable_get(:@dictionary_paths)).to eq(
          [
            'category',
            'dates.list.element',
            'hash_lists.list.element',
            'labels.list.element',
            'nested_numbers.list.element.list.element',
            'numbers.list.element',
            'object_ids.list.element'
          ]
        )

        table = Arrow::Table.load(File.join(directory, 'read-safe.parquet'), format: :parquet)
        normalize = lambda do |value|
          value.is_a?(Arrow::Array) ? value.to_a.map { |item| normalize.call(item) } : value
        end
        %w[category dates flags labels nested_numbers numbers].each do |name|
          actual = table[name].to_a.map { |value| normalize.call(value) }
          expect(actual).to eq(documents.map { |document| document.fetch(name) })
        end
        expect(table['object_ids'].to_a.map { |value| normalize.call(value) }).to eq(
          documents.map { |document| document.fetch('object_ids').map(&:to_s) }
        )

        dotted = described_class.new(directory:, prefix: 'dotted', compression: :zstd, row_group_size: 4)
        dotted.write_many(Array.new(4) { { 'dotted.name' => 'same' } })
        dotted.close
        expect(dotted.instance_variable_get(:@dictionary_paths)).to be_nil
        dotted_table = Arrow::Table.load(File.join(directory, 'dotted.parquet'), format: :parquet)
        expect(dotted_table['dotted.name'].to_a).to eq(Array.new(4, 'same'))
      end
    end

    it 'rejects integer values that outgrow the inferred row-group schema' do
      Dir.mktmpdir do |directory|
        writer = described_class.new(directory:, prefix: 'integer-overflow', compression: :none, row_group_size: 2)
        writer.write_many([{ 'value' => 0 }, { 'value' => 255 }])
        expect do
          writer.write_many([{ 'value' => 256 }, { 'value' => 1 }])
        end.to raise_error(RangeError, /integer range 1\.\.256 exceeds/)
        writer.close
      end
    end

    it 'pre-rotates final groups and rotates exact-size final files' do
      Dir.mktmpdir do |directory|
        pre_rotate = described_class.new(directory:, prefix: 'pre', compression: :none, row_group_size: 3,
                                         single_file: false, rotate_rows: 4)
        pre_rotate.write_many(Array.new(3) { |index| { value: index } })
        pre_rotate.write_many([{ value: 4 }, { value: 5 }])
        pre_rotate.close

        exact = described_class.new(directory:, prefix: 'exact', compression: :none, row_group_size: 10,
                                    single_file: false, rotate_rows: 2)
        exact.write_many([{ value: 1 }, { value: 2 }])
        exact.close
        expect(Dir[File.join(directory, 'pre-*.parquet')].length).to eq(2)
        expect(Dir[File.join(directory, 'exact-*.parquet')].length).to eq(1)
      end
    end

    it 'fails clearly when Arrow dependencies are unavailable' do
      hide_const('Arrow')
      hide_const('Parquet')
      expect do
        described_class.new(directory: Dir.mktmpdir, prefix: 'missing', compression: :none)
      end.to raise_error(ArgumentError, /Parquet support requires/)
    end
    it 'covers empty internals and split rotation without telemetry or a manifest' do
      previous_telemetry = Thread.current[:pl_telemetry]
      Thread.current[:pl_telemetry] = nil
      Dir.mktmpdir do |directory|
        split = described_class.new(directory:, prefix: 'split-nil', compression: :none, row_group_size: 4,
                                    single_file: false, rotate_rows: 3)
        split.write_many(Array.new(4) { |index| { value: index } })
        split.close
        expect(Dir[File.join(directory, 'split-nil-*.parquet')].length).to eq(2)

        empty = described_class.new(directory:, prefix: 'empty', compression: :none)
        empty.write_many([])
        empty.close
        expect(File).not_to exist(File.join(directory, 'empty.parquet'))
      end
    ensure
      Thread.current[:pl_telemetry] = previous_telemetry
    end
  end
end
# rubocop:enable Lint/ConstantDefinitionInBlock

# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'timeout'

# Test-local doubles intentionally live beside their behavioral contracts.
# rubocop:disable Lint/ConstantDefinitionInBlock
RSpec.describe 'core path coverage' do
  class CoverageCursor
    include Enumerable

    def initialize(documents)
      @documents = documents
      @direction = 1
      @skip = 0
      @limit = nil
    end

    def projection(*) = self
    def hint(*) = self
    def batch_size(*) = self
    def no_cursor_timeout = self

    def sort(spec)
      @direction = spec[:_id] || spec['_id'] || 1
      self
    end

    def skip(count)
      @skip = count
      self
    end

    def limit(count)
      @limit = count
      self
    end

    def to_a
      documents = @direction.negative? ? @documents.reverse : @documents
      documents = documents.drop(@skip)
      @limit ? documents.first(@limit) : documents
    end

    def first = to_a.first
    def each(&) = to_a.each(&)
  end

  class CoverageCollection
    attr_reader :estimated_document_count

    def initialize(documents, estimated_document_count: documents.length)
      @documents = documents
      @estimated_document_count = estimated_document_count
    end

    def find(filter = {}, *)
      documents = if (range = filter['_id'] || filter[:_id]) && range['$gt']
                    @documents.select { |document| document['_id'] > range['$gt'] }
                  else
                    @documents
                  end
      CoverageCursor.new(documents)
    end
  end

  describe Purplelight::ByteQueue do
    it 'reports bytes and rejects pushes after close' do
      queue = described_class.new(max_bytes: 8)
      queue.push(:item, bytes: 8)
      expect(queue.size_bytes).to eq(8)
      expect(queue.pop).to eq(:item)
      queue.close
      expect(queue.pop).to be_nil
      expect { queue.push(:item, bytes: 1) }.to raise_error('queue closed')
    end

    it 'applies and releases byte backpressure' do
      queue = described_class.new(max_bytes: 1)
      queue.push(:first, bytes: 1)
      pushed = Queue.new
      producer = Thread.new do
        queue.push(:second, bytes: 1)
        pushed << true
      end
      sleep 0.01 until producer.status == 'sleep'
      expect(queue.pop).to eq(:first)
      expect(pushed.pop).to be true
      expect(queue.pop).to eq(:second)
      producer.join
    end

    it 'admits one oversized item and wakes blocked producers when closed' do
      oversized = described_class.new(max_bytes: 1)
      oversized.push(:oversized, bytes: 2)
      expect(oversized.pop).to eq(:oversized)

      queue = described_class.new(max_bytes: 1)
      queue.push(:first, bytes: 1)
      failure = Queue.new
      producer = Thread.new do
        queue.push(:blocked, bytes: 1)
      rescue RuntimeError => e
        failure << e
      end
      Timeout.timeout(1) { Thread.pass until producer.status == 'sleep' }
      queue.close
      producer.join
      expect(failure.pop.message).to eq('queue closed')
    end
    it 'discards buffered items when closed after a worker failure' do
      queue = described_class.new(max_bytes: 8)
      queue.push(:buffered, bytes: 8)
      queue.close(discard: true)

      expect(queue.size_bytes).to be_zero
      expect(queue.pop).to be_nil
    end
  end

  describe Purplelight::Telemetry do
    it 'records and merges enabled telemetry' do
      telemetry = described_class.new
      ticket = telemetry.start(:read)
      telemetry.finish(:read, ticket - 1.0)
      telemetry.add(:documents, 2)
      other = described_class.new
      other.add(:documents, 3)
      other.finish(:read, other.start(:read))
      expect(telemetry.merge!(other)).to equal(telemetry)
      expect(telemetry.enabled?).to be true
      expect(telemetry.counters[:documents]).to eq(5)
      expect(telemetry.timers[:read]).to be_positive
    end

    it 'does no work when disabled or given no ticket' do
      telemetry = described_class.new(enabled: false)
      expect(telemetry.start(:read)).to be_nil
      expect(telemetry.finish(:read, nil)).to be_nil
      expect(telemetry.add(:documents)).to be_nil
      expect(telemetry.merge!(described_class.new)).to equal(telemetry)
      expect(telemetry.enabled?).to be false
      expect(telemetry.counters).to be_empty
      expect(telemetry.timers).to be_empty
    end
  end

  describe Purplelight::Manifest do
    it 'loads, exposes state, and performs delayed progress saves' do
      Dir.mktmpdir do |directory|
        path = File.join(directory, 'state.json')
        manifest = described_class.new(path:)
        manifest.configure!(collection: 'items', format: :csv, compression: :none, query_digest: 'digest')
        manifest.ensure_partitions!(1)
        manifest.ensure_partitions!(1)
        expect(manifest.partition_checkpoint(99)).to be_nil
        part_index = manifest.open_part!('items.csv')
        manifest.instance_variable_set(:@last_save_at, 0.0)
        manifest.add_progress_to_part!(index: part_index, rows_delta: 1, bytes_delta: 2)
        manifest.update_partition_checkpoint!(0, 42)
        expect(manifest.partition_checkpoint(0)).to eq(42)
        loaded = described_class.load(path)
        expect(loaded.parts.first).to include('rows' => 1, 'bytes' => 2)
        expect(loaded.partitions.length).to eq(1)
        legacy = described_class.new(path: File.join(directory, 'legacy.json'),
                                     data: manifest.data.merge('version' => 1))
        expect(legacy.compatible_with?(collection: 'items', format: :csv, compression: :none,
                                       query_digest: 'digest')).to be false
      end
    end
  end

  describe Purplelight::Partitioner do
    let(:ids) do
      [1, 11, 21, 31].map { |second| BSON::ObjectId.from_time(Time.at(second)) }
    end

    it 'covers empty and populated simple ranges' do
      empty = CoverageCollection.new([])
      expect(described_class.simple_ranges(collection: empty, query: nil, partitions: 2)).to eq(
        [{ filter: {}, sort: { _id: 1 } }]
      )
      populated = CoverageCollection.new(ids.map { |id| { '_id' => id } })
      ranges = described_class.simple_ranges(collection: populated, query: { 'active' => true }, partitions: 3)
      expect(ranges.length).to eq(3)
      expect(ranges.first[:filter]).to include('active' => true)
      expect(described_class.simple_ranges(collection: populated, query: nil, partitions: 3).length).to eq(3)
    end

    it 'uses timestamp planning with and without telemetry' do
      collection = CoverageCollection.new(ids.map { |id| { '_id' => id } })
      expect(described_class.object_id_partitions(collection:, query: nil, partitions: 2, telemetry: nil).length).to eq(2)
      telemetry = Purplelight::Telemetry.new
      expect(described_class.timestamp_partitions(collection:, query: {}, partitions: 2, telemetry:).length).to eq(2)
      boundaryless_class = Class.new(CoverageCollection) do
        def find(filter = {}, *)
          return CoverageCursor.new([]) if filter.dig('_id', '$gt')

          super
        end
      end
      boundaryless = boundaryless_class.new(ids.map { |id| { '_id' => id } })
      expect(described_class.timestamp_partitions(collection: boundaryless, query: nil, partitions: 2).length).to eq(1)
    end

    it 'falls back for empty, non-ObjectId, and equal timestamp IDs' do
      empty = CoverageCollection.new([])
      expect(described_class.timestamp_partitions(collection: empty, query: nil, partitions: 2)).to eq(
        [{ filter: {}, sort: { _id: 1 } }]
      )
      non_object_ids = CoverageCollection.new([{ '_id' => 1 }, { '_id' => 2 }])
      expect(described_class.timestamp_partitions(collection: non_object_ids, query: {}, partitions: 2).length).to eq(2)
      same_time = ids.first(2).map { |id| BSON::ObjectId.from_time(id.generation_time) }
      equal_ids = CoverageCollection.new(same_time.map { |id| { '_id' => id } })
      expect(described_class.timestamp_partitions(collection: equal_ids, query: nil, partitions: 1).length).to eq(1)
    end

    it 'covers cursor sampling, its small fast path, and all range shapes' do
      small = CoverageCollection.new(ids.map { |id| { '_id' => id } })
      expect(described_class.object_id_partitions(collection: small, query: {}, partitions: 2, mode: :cursor).length).to eq(2)

      many_documents = Array.new(5_001) { |index| { '_id' => index } }
      large = CoverageCollection.new(many_documents, estimated_document_count: 10_001)
      expect(described_class.cursor_sampling_partitions(collection: large, query: nil, partitions: 2).length).to eq(2)
      expect(described_class.cursor_sampling_partitions(collection: large, query: { 'active' => true },
                                                        partitions: 2).length).to eq(2)
      expect(described_class.build_range(1, 2)).to eq('$gt' => 1, '$lte' => 2)
      expect(described_class.build_range(1, nil)).to eq('$gt' => 1)
      expect(described_class.build_range(nil, 2)).to eq('$lte' => 2)
      expect(described_class.build_range(nil, nil)).to eq({})
    end
  end
end
# rubocop:enable Lint/ConstantDefinitionInBlock

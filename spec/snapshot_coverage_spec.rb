# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'

# Test-local doubles intentionally live beside their behavioral contracts.
# rubocop:disable Lint/ConstantDefinitionInBlock
RSpec.describe Purplelight::Snapshot do
  class SnapshotCursor
    include Enumerable

    def initialize(documents, delay: 0)
      @documents = documents
      @delay = delay
      @direction = 1
      @limit = nil
    end

    def projection(*) = self
    def hint(*) = self
    def batch_size(*) = self
    def no_cursor_timeout = self

    def limit(count)
      @limit = count
      self
    end

    def skip(*) = self

    def sort(spec)
      @direction = spec[:_id] || spec['_id'] || 1
      self
    end

    def to_a
      documents = @direction.negative? ? @documents.reverse : @documents
      @limit ? documents.first(@limit) : documents
    end

    def first = to_a.first

    def each
      to_a.each do |document|
        sleep @delay if @delay.positive?
        yield document.dup
      end
    end
  end

  class SnapshotCollection
    attr_reader :name

    def initialize(name, documents, delay: 0)
      @name = name
      @documents = documents
      @delay = delay
    end

    def find(filter = {}, *)
      documents = if (range = filter['_id']) && range['$gt']
                    checkpoint = range['$gt'].to_s
                    @documents.select { |document| document['_id'].to_s > checkpoint }
                  else
                    @documents
                  end
      SnapshotCursor.new(documents, delay: @delay)
    end

    def estimated_document_count = @documents.length
  end

  class SnapshotClient
    def initialize(collection)
      @collection = collection
    end

    def [](*) = @collection
  end

  def build_snapshot(directory, documents: [], **options)
    collection = SnapshotCollection.new('records', documents, delay: options.delete(:delay) || 0)
    described_class.new(client: SnapshotClient.new(collection), collection: :records, output: directory,
                        compression: :none, partitions: 1, **options)
  end

  def with_environment(overrides)
    previous = overrides.to_h { |key, _value| [key, ENV.fetch(key, nil)] }
    overrides.each { |key, value| ENV[key] = value }
    yield
  ensure
    previous.each { |key, value| value.nil? ? ENV.delete(key) : ENV[key] = value }
  end

  it 'overwrites incompatible resume state when requested' do
    Dir.mktmpdir do |directory|
      path = File.join(directory, 'records.manifest.json')
      manifest = Purplelight::Manifest.new(path:)
      manifest.configure!(collection: 'other', format: :csv, compression: :gzip, query_digest: 'wrong')
      snapshot = build_snapshot(directory, resume: { enabled: true, overwrite_incompatible: true })
      expect(snapshot.run).to be true
      expect(Purplelight::Manifest.load(path).data['collection']).to eq('records')
    end
  end

  it 'rejects incompatible resume state without overwrite' do
    Dir.mktmpdir do |directory|
      path = File.join(directory, 'records.manifest.json')
      manifest = Purplelight::Manifest.new(path:)
      manifest.configure!(collection: 'other', format: :csv, compression: :gzip, query_digest: 'wrong')
      snapshot = build_snapshot(directory, resume: { enabled: true, overwrite_incompatible: false })
      expect { snapshot.run }.to raise_error(Purplelight::IncompatibleResumeError)
    end
  end

  it 'rejects unsupported formats and resolves explicit output paths' do
    Dir.mktmpdir do |directory|
      output = File.join(directory, 'custom.jsonl.gz')
      snapshot = build_snapshot(output, format: :xml)
      expect(snapshot.send(:resolve_output, output, :xml)).to eq([directory, 'custom'])
      expect { snapshot.run }.to raise_error(ArgumentError, /format not implemented/)
    end
  end

  it 'reports enabled telemetry through a logger and standard output' do
    Dir.mktmpdir do |directory|
      logger = instance_double(Logger, info: nil)
      logged = build_snapshot(File.join(directory, 'logged'), telemetry: Purplelight::Telemetry.new, logger:)
      expect(logged.run).to be true
      expect(logger).to have_received(:info).with(/Telemetry/)

      printed = build_snapshot(File.join(directory, 'printed'), telemetry: Purplelight::Telemetry.new)
      expect { printed.run }.to output(/Telemetry/).to_stdout
    end
  end

  it 'delivers progress callbacks without imposing shutdown latency' do
    documents = Array.new(100) { |index| { '_id' => BSON::ObjectId.new, 'value' => index } }
    Dir.mktmpdir do |directory|
      events = Queue.new
      snapshot = build_snapshot(directory, documents:, delay: 0.001, on_progress: ->(event) { events << event })
      worker = Thread.new { snapshot.run }
      sleep 0.02
      snapshot.instance_variable_get(:@progress_mutex).synchronize do
        snapshot.instance_variable_get(:@progress_cv).broadcast
      end
      worker.join
      expect(events.pop).to include(:queue_bytes)
    end
  end

  it 'maps and batches non-JSON rows while resuming from a checkpoint' do
    documents = Array.new(3) { |index| { '_id' => BSON::ObjectId.new, 'value' => index } }
    Dir.mktmpdir do |directory|
      snapshot = build_snapshot(directory, documents:, format: :csv, batch_size: 1,
                                           mapper: ->(document) { document.merge('mapped' => true) },
                                           read_concern: :local, read_preference: { mode: :secondary }, projection: { value: 1 })
      expect(snapshot.run).to be true
      manifest = Purplelight::Manifest.load(File.join(directory, 'records.manifest.json'))
      manifest.update_partition_checkpoint!(0, documents.first['_id'])
      resumed = build_snapshot(directory, documents:, format: :csv, batch_size: 1, projection: { value: 1 },
                                          resume: { enabled: true, overwrite_incompatible: false })
      expect(resumed.run).to be true
    end
  end
  it 'uses the requested JSONL writer concurrency without losing rows' do
    documents = Array.new(100) do |index|
      { '_id' => BSON::ObjectId.new.tap(&:to_s), 'value' => index }
    end
    Dir.mktmpdir do |directory|
      allow(Purplelight::WriterJSONL).to receive(:new).and_call_original
      snapshot = build_snapshot(directory, documents:, format: :jsonl, batch_size: 1, writer_threads: 2)

      expect(snapshot.run).to be true
      expect(Purplelight::WriterJSONL).to have_received(:new).twice
      rows = Dir[File.join(directory, 'records-part-*.jsonl')].flat_map { |path| File.readlines(path) }
      expect(rows.length).to eq(documents.length)
      expect(rows.map { |line| JSON.parse(line).fetch('value') }.sort).to eq((0...documents.length).to_a)

      original_paths = Dir[File.join(directory, 'records-part-*.jsonl')]
      saved_checkpoint = Purplelight::Manifest.load(File.join(directory, 'records.manifest.json'))
                                              .partition_checkpoint(0)
      expect(saved_checkpoint).to eq(documents.last['_id'])
      additional = Array.new(10) do |index|
        { '_id' => BSON::ObjectId.new.tap(&:to_s), 'value' => documents.length + index }
      end
      resumed = build_snapshot(directory, documents: documents + additional, format: :jsonl, batch_size: 1,
                                          writer_threads: 2)
      expect(resumed.run).to be true
      resumed_paths = Dir[File.join(directory, 'records-part-*.jsonl')]
      expect(resumed_paths.length).to be > original_paths.length
      expect(resumed_paths).to include(*original_paths)
      resumed_rows = resumed_paths.flat_map { |path| File.readlines(path) }
      expect(resumed_rows.length).to eq(documents.length + additional.length)
    end
  end

  it 'delegates the class entrypoint to a snapshot instance' do
    runner = instance_double(described_class, run: :complete)
    allow(described_class).to receive(:new).with(client: :client).and_return(runner)
    expect(described_class.snapshot(client: :client)).to eq(:complete)
    allow(described_class).to receive(:snapshot).with(client: :top).and_return(:top_complete)
    expect(Purplelight.snapshot(client: :top)).to eq(:top_complete)
  end

  it 'uses environment defaults and both Parquet sharding modes' do
    Dir.mktmpdir do |directory|
      with_environment('PL_ZSTD_LEVEL' => '5', 'PL_WRITE_CHUNK_BYTES' => '4096',
                       'PL_PARQUET_ROW_GROUP' => '7') do
        single = build_snapshot(File.join(directory, 'single'), format: :parquet,
                                                                sharding: { mode: :single_file })
        expect(single.run).to be true
        options = Purplelight::Manifest.load(File.join(directory, 'single.manifest.json')).data['options']
        expect(options).to include('compression_level' => 5, 'write_chunk_bytes' => 4096,
                                   'parquet_row_group' => 7)
      end

      multipart = build_snapshot(File.join(directory, 'multipart'), format: :parquet,
                                                                    parquet_row_group: 3,
                                                                    sharding: { mode: :by_size })
      expect(multipart.run).to be true

      defaulted = build_snapshot(File.join(directory, 'defaulted'), format: :parquet)
      expect(defaulted.run).to be true
    end
  end

  it 'enables telemetry from the environment and reports a zero-total telemetry object' do
    Dir.mktmpdir do |directory|
      with_environment('PL_TELEMETRY' => '1') do
        expect { build_snapshot(File.join(directory, 'environment')).run }.to output(/Telemetry/).to_stdout
      end

      telemetry = instance_double(Purplelight::Telemetry, enabled?: true, start: nil, finish: nil,
                                                          timers: { idle: 0.0 })
      allow(telemetry).to receive(:merge!).and_return(telemetry)
      expect do
        build_snapshot(File.join(directory, 'zero'), telemetry:).run
      end.to output(/idle: 0.0s \(0(?:\.0)?%\)/).to_stdout
    end
  end
  it 'omits nil cursor options and flushes a final partial non-JSON batch' do
    Dir.mktmpdir do |directory|
      empty = build_snapshot(File.join(directory, 'empty'), batch_size: nil, read_preference: nil,
                                                            read_concern: nil)
      expect(empty.run).to be true

      documents = Array.new(3) { |index| { '_id' => BSON::ObjectId.new, 'value' => index } }
      partial = build_snapshot(File.join(directory, 'partial'), documents:, format: :csv, batch_size: 10)
      expect(partial.run).to be true
    end
  end
end
# rubocop:enable Lint/ConstantDefinitionInBlock

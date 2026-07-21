#!/usr/bin/env ruby
# frozen_string_literal: true

require 'bundler/setup'
require 'fileutils'
require 'json'
require 'stringio'
require 'tmpdir'
require 'time'
require 'purplelight'
require_relative 'harness'

DOCS = Array.new(200) do |index|
  {
    '_id' => BSON::ObjectId.new,
    'index' => index,
    'active' => index.even?,
    'tags' => %w[alpha beta],
    'nested' => { 'value' => index }
  }
end.freeze
ENCODED_JSONL = DOCS.map { |document| "#{JSON.generate(document)}\n" }.join * 10
ENCODED_BATCH = Purplelight::WriterJSONL::EncodedBatch.new(
  data: ENCODED_JSONL,
  rows: DOCS.length * 10,
  bytes: ENCODED_JSONL.bytesize
)
PARTITION_DOCS = Array.new(5_001) { |index| { '_id' => index } }.freeze
pipeline_random = Random.new(12_345)
PIPELINE_DOCS = Array.new(20_000) do |index|
  object_id = BSON::ObjectId.new.tap(&:to_s)
  { '_id' => object_id, 'index' => index, 'payload' => pipeline_random.bytes(512).unpack1('H*') }
end.freeze

FakeCollection = Data.define(:name)
FakeClient = Class.new do
  def [](name)
    FakeCollection.new(name.to_s)
  end
end
EmptyCursor = Class.new do
  def projection(*) = self
  def sort(*) = self
  def limit(*) = self
  def first = nil
  def each = nil
end

EmptyCollection = Data.define(:name) do
  def find(*) = EmptyCursor.new
  def estimated_document_count = 0
end

EmptyClient = Class.new do
  def [](name) = EmptyCollection.new(name.to_s)
end

RawCursor = Class.new do
  def initialize(documents)
    @documents = documents
  end

  def each(&) = @documents.each(&)
end

RawCollection = Data.define(:name, :documents) do
  def find(*) = RawCursor.new(documents)
end

CollectionClient = Data.define(:collection) do
  def [](*) = collection
end

PipelineCursor = Class.new do
  include Enumerable

  def initialize(documents)
    @documents = documents
    @direction = 1
    @limit = nil
  end

  def projection(*) = self
  def hint(*) = self
  def batch_size(*) = self
  def no_cursor_timeout = self
  def skip(*) = self

  def sort(specification)
    @direction = specification[:_id] || specification['_id'] || 1
    self
  end

  def limit(count)
    @limit = count
    self
  end

  def to_a
    documents = @direction.negative? ? @documents.reverse : @documents
    @limit ? documents.first(@limit) : documents
  end

  def first = to_a.first
  def each(&) = to_a.each(&)
end

PipelineCollection = Data.define(:name, :documents) do
  def find(*) = PipelineCursor.new(documents)
  def estimated_document_count = documents.length
end

SinkQueue = Class.new do
  def push(*) = nil
end

SinkManifest = Class.new do
  def partition_checkpoint(*) = nil
  def update_partition_checkpoint!(*) = nil
  def mark_partition_complete!(*) = nil
end

PlannerCursor = Class.new do
  include Enumerable

  def initialize(documents)
    @documents = documents
  end

  def projection(*) = self
  def batch_size(*) = self
  def no_cursor_timeout = self
  def each(&) = @documents.each(&)
end

PlannerCollection = Data.define(:documents) do
  def find(*) = PlannerCursor.new(documents)
  def estimated_document_count = 10_001
end

harness = Purplelight::Microbench::Harness.new

Dir.mktmpdir('purplelight-microbench') do |directory|
  sequence = 0

  harness.register('byte_queue_round_trip', paths: :queue, iterations: 20_000) do
    queue = Purplelight::ByteQueue.new(max_bytes: 1024)
    queue.push(sequence, bytes: 8)
    queue.pop
  end
  harness.register('byte_queue_bulk_drain', paths: :queue, iterations: 10) do
    queue = Purplelight::ByteQueue.new(max_bytes: 50_000)
    5_000.times { queue.push(sequence, bytes: 8) }
    5_000.times { queue.pop }
  end

  telemetry = Purplelight::Telemetry.new
  harness.register('telemetry_counter_and_timer', paths: :telemetry, iterations: 50_000) do
    ticket = telemetry.start(:operation)
    telemetry.add(:documents)
    telemetry.finish(:operation, ticket)
  end

  harness.register('telemetry_construction', paths: :telemetry, iterations: 100_000) do
    Purplelight::Telemetry.new
  end

  harness.register('manifest_progress', paths: :manifest, iterations: 250) do
    sequence += 1
    path = File.join(directory, "manifest-#{sequence}.json")
    manifest = Purplelight::Manifest.new(path:)
    manifest.configure!(collection: 'records', format: :jsonl, compression: :none,
                        query_digest: Purplelight::Manifest.query_digest({}, nil))
    manifest.ensure_partitions!(1)
    index = manifest.open_part!("part-#{sequence}")
    manifest.add_progress_to_part!(index:, rows_delta: 200, bytes_delta: 4096)
    manifest.update_partition_checkpoint!(0, sequence)
    manifest.mark_partition_complete!(0)
    manifest.complete_part!(index:)
  end

  harness.register('partition_range_planning', paths: :partitioner, iterations: 100_000) do
    Purplelight::Partitioner.build_range(sequence, sequence + 1)
  end

  planner_collection = PlannerCollection.new(PARTITION_DOCS)
  harness.register('partition_cursor_sampling', paths: :partitioner, iterations: 10) do
    Purplelight::Partitioner.cursor_sampling_partitions(collection: planner_collection, query: nil, partitions: 2)
  end

  client = FakeClient.new
  harness.register('snapshot_configuration', paths: :snapshot, iterations: 10_000) do
    snapshot = Purplelight::Snapshot.new(client:, collection: :records, output: directory,
                                         format: :jsonl, compression: :none, partitions: 1)
    snapshot.send(:resolve_output, directory, :jsonl)
  end
  empty_client = EmptyClient.new
  harness.register('snapshot_empty_export', paths: :snapshot, iterations: 20) do
    sequence += 1
    output = File.join(directory, "snapshot-#{sequence}")
    Purplelight::Snapshot.new(client: empty_client, collection: :records, output:, format: :jsonl,
                              compression: :none, partitions: 1, resume: { enabled: false }).run
  end

  raw_collection = RawCollection.new('records', DOCS)
  raw_snapshot = Purplelight::Snapshot.new(client: CollectionClient.new(raw_collection), collection: :records,
                                           output: directory, format: :csv, compression: :none, batch_size: DOCS.length,
                                           partitions: 1, resume: { enabled: false })
  sink_queue = SinkQueue.new
  sink_manifest = SinkManifest.new
  harness.register('snapshot_raw_batching', paths: :snapshot, iterations: 100) do
    raw_snapshot.send(:read_partition, idx: 0, filter_spec: { filter: {} }, queue: sink_queue,
                                       batch_size: DOCS.length, manifest: sink_manifest)
  end

  pipeline_collection = PipelineCollection.new('pipeline', PIPELINE_DOCS)
  pipeline_client = CollectionClient.new(pipeline_collection)
  harness.register('snapshot_jsonl_serial_pipeline', paths: %i[manifest queue snapshot writer_jsonl], iterations: 3) do
    sequence += 1
    output = File.join(directory, "pipeline-serial-#{sequence}.jsonl")
    Purplelight::Snapshot.new(client: pipeline_client, collection: :pipeline, output:, format: :jsonl,
                              compression: :zstd, batch_size: 250, partitions: 1, writer_threads: 1,
                              resume: { enabled: false }).run
  end

  harness.register('snapshot_jsonl_pipeline', paths: %i[manifest queue snapshot writer_jsonl], iterations: 3) do
    sequence += 1
    output = File.join(directory, "pipeline-#{sequence}.jsonl")
    Purplelight::Snapshot.new(client: pipeline_client, collection: :pipeline, output:, format: :jsonl,
                              compression: :zstd, batch_size: 250, partitions: 1, writer_threads: 2,
                              resume: { enabled: false }).run
  end

  harness.register('jsonl_batch_write', paths: :writer_jsonl, iterations: 100) do
    sequence += 1
    writer = Purplelight::WriterJSONL.new(directory:, prefix: "jsonl-#{sequence}", compression: :none)
    writer.write_many(DOCS)
    writer.close
  end

  harness.register('jsonl_zstd_batch_write', paths: :writer_jsonl, iterations: 50) do
    sequence += 1
    writer = Purplelight::WriterJSONL.new(directory:, prefix: "jsonl-zstd-#{sequence}", compression: :zstd)
    writer.write_many(DOCS)
    writer.close
  end

  accounting_writer = Purplelight::WriterJSONL.new(
    directory:, prefix: 'accounting', compression: :none, rotate_bytes: nil
  )
  accounting_io = StringIO.new
  accounting_writer.instance_variable_set(:@io, accounting_io)
  harness.register('jsonl_preencoded_accounting', paths: :writer_jsonl, iterations: 10_000) do
    accounting_io.truncate(0)
    accounting_io.rewind
    accounting_writer.write_many(ENCODED_BATCH)
  end

  harness.register('csv_batch_write', paths: :writer_csv, iterations: 100) do
    sequence += 1
    writer = Purplelight::WriterCSV.new(directory:, prefix: "csv-#{sequence}", compression: :none,
                                        single_file: true)
    writer.write_many(DOCS)
    writer.close
  end

  harness.register('csv_zstd_batch_write', paths: :writer_csv, iterations: 50) do
    sequence += 1
    writer = Purplelight::WriterCSV.new(directory:, prefix: "csv-zstd-#{sequence}", compression: :zstd,
                                        single_file: true)
    writer.write_many(DOCS)
    writer.close
  end

  harness.register('csv_gzip_batch_write', paths: :writer_csv, iterations: 50) do
    sequence += 1
    writer = Purplelight::WriterCSV.new(directory:, prefix: "csv-gzip-#{sequence}", compression: :gzip,
                                        compression_level: 1, single_file: true)
    writer.write_many(DOCS)
    writer.close
  end
  raw_csv_io = StringIO.new
  counting_io_class = Purplelight::WriterCSV::CountingIO
  has_callback = counting_io_class.instance_method(:initialize).parameters.any? { |_, name| name == :on_write }
  counting_io = if has_callback
                  counting_io_class.new(raw_csv_io, on_write: ->(*) {})
                else
                  counting_io_class.new(raw_csv_io)
                end
  harness.register('csv_counting_io_write', paths: :writer_csv, iterations: 100_000) do
    counting_io.write('payload')
  end
  csv_serializer = Purplelight::WriterCSV.new(directory:, prefix: 'csv-serializer', compression: :none,
                                              columns: DOCS.first.keys)
  csv_serialized_row = +''
  harness.register('csv_row_serialization', paths: :writer_csv, iterations: 25_000) do
    csv_serialized_row.clear
    csv_serializer.send(:append_csv_document, csv_serialized_row, DOCS.first)
  end

  parquet_buffer_writer = Purplelight::WriterParquet.new(directory:, prefix: 'parquet-buffer', compression: :none,
                                                         row_group_size: 1_000_000, single_file: true)
  harness.register('parquet_buffer_append', paths: :writer_parquet, iterations: 5_000) do
    parquet_buffer_writer.write_many(DOCS)
    parquet_buffer_writer.instance_variable_get(:@buffer_docs).clear
  end

  object_id = BSON::ObjectId.new
  harness.register('parquet_bson_value', paths: :writer_parquet, iterations: 100_000) do
    parquet_buffer_writer.send(:extract_value, { '_id' => object_id }, '_id')
  end

  harness.register('parquet_batch_write', paths: :writer_parquet, iterations: 10) do
    sequence += 1
    writer = Purplelight::WriterParquet.new(directory:, prefix: "parquet-#{sequence}", compression: :zstd,
                                            row_group_size: 100, single_file: true)
    writer.write_many(DOCS)
    writer.close
  end

  baseline_index = ARGV.index('--baseline')
  output_index = ARGV.index('--output')
  report = harness.run(baseline_path: baseline_index && ARGV.fetch(baseline_index + 1))
  rendered = JSON.pretty_generate(report)
  puts rendered
  File.write(ARGV.fetch(output_index + 1), "#{rendered}\n") if output_index
end

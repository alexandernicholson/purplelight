# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'json'

RSpec.describe 'Performance benchmark (1M docs, gated by BENCH=1)' do
  def docker?
    system('docker --version > /dev/null 2>&1')
  end

  it 'exports 1,000,000 documents and reports throughput' do
    skip 'Set BENCH=1 to run this benchmark' unless ENV['BENCH'] == '1'
    skip 'Docker not available' unless docker?

    Dir.mktmpdir('purplelight-bench') do |dir|
      container = "pl-mongo-bench-#{Time.now.to_i}"
      begin
        system("docker run -d --rm --name #{container} -p 27018:27017 mongo:7 > /dev/null") or skip 'failed to start docker'
        # Wait for readiness
        60.times do
          ok = system("docker exec #{container} mongosh --eval 'db.runCommand({hello:1})' > /dev/null 2>&1")
          break if ok

          sleep 1
        end

        client = Mongo::Client.new('mongodb://127.0.0.1:27018', server_api: { version: '1' })
        db = client.use('bench')
        coll = db[:items]

        doc_count = (ENV['BENCH_DOCS'] || '1000000').to_i
        batch = (ENV['BENCH_INSERT_BATCH'] || '20000').to_i
        start_time = Time.now
        inserted = 0
        id_gen = 0
        while inserted < doc_count
          n = [batch, doc_count - inserted].min
          docs = Array.new(n) do
            i = id_gen
            id_gen += 1
            { _id: BSON::ObjectId.new, seq: i, email: "u#{i}@ex.com", active: true, v: i }
          end
          coll.insert_many(docs, ordered: false)
          inserted += n
        end
        insert_elapsed = Time.now - start_time

        # Export
        export_dir = dir
        bench_format = (ENV['BENCH_FORMAT'] || 'jsonl').downcase
        prefix = bench_format == 'parquet' ? 'bench_items_parquet' : 'bench_items'
        t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        partitions = if ENV['BENCH_PARTITIONS'] && ENV['BENCH_PARTITIONS'].to_i > 0
                       ENV['BENCH_PARTITIONS'].to_i
                     else
                       [
                         Etc.respond_to?(:nprocessors) ? Etc.nprocessors * 2 : 4, 32
                       ].min
                     end
        batch_size = ENV['BENCH_BATCH_SIZE'] && ENV['BENCH_BATCH_SIZE'].to_i > 0 ? ENV['BENCH_BATCH_SIZE'].to_i : 5000
        queue_mb = ENV['BENCH_QUEUE_MB'] && ENV['BENCH_QUEUE_MB'].to_i > 0 ? ENV['BENCH_QUEUE_MB'].to_i : 256
        rotate_mb = ENV['BENCH_ROTATE_MB'] && ENV['BENCH_ROTATE_MB'].to_i > 0 ? ENV['BENCH_ROTATE_MB'].to_i : 512
        compression = ENV['BENCH_COMPRESSION'] && !ENV['BENCH_COMPRESSION'].empty? ? ENV['BENCH_COMPRESSION'].to_sym : :gzip
        if bench_format == 'parquet'
          begin
            require 'arrow'
            require 'parquet'
          rescue LoadError
            skip 'Parquet not available for BENCH_FORMAT=parquet'
          end
          parquet_row_group = (ENV['BENCH_PARQUET_ROW_GROUP'] || '50000').to_i
          Purplelight.snapshot(
            client: db,
            collection: :items,
            output: export_dir,
            format: :parquet,
            compression: compression,
            partitions: partitions,
            batch_size: batch_size,
            query: {},
            sharding: { mode: :single_file, prefix: prefix },
            queue_size_bytes: queue_mb * 1024 * 1024,
            parquet_row_group: parquet_row_group,
            resume: { enabled: true }
          )
        else
          Purplelight.snapshot(
            client: db,
            collection: :items,
            output: export_dir,
            format: :jsonl,
            compression: compression,
            partitions: partitions,
            batch_size: batch_size,
            query: {},
            sharding: { mode: :by_size, part_bytes: rotate_mb * 1024 * 1024, prefix: prefix },
            queue_size_bytes: queue_mb * 1024 * 1024,
            resume: { enabled: true }
          )
        end
        t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        elapsed = t1 - t0

        manifest_path = File.join(export_dir, "#{prefix}.manifest.json")
        data = JSON.parse(File.read(manifest_path))
        parts_meta = data.fetch('parts', [])
        rows = parts_meta.sum { |p| p['rows'].to_i }
        part_paths = parts_meta.map { |p| p['path'] }.compact
        bytes = part_paths.sum { |p| File.exist?(p) ? File.size(p) : 0 }

        docs_per_sec = (rows / elapsed).round(2)
        mb_per_sec = (bytes.to_f / (1024 * 1024) / elapsed).round(2)

        puts 'Benchmark results:'
        puts "  Inserted: #{inserted} docs in #{insert_elapsed.round(2)}s"
        puts "  Exported: #{rows} docs in #{elapsed.round(2)}s"
        puts "  Parts:    #{parts_meta.size}, Bytes: #{bytes}"
        puts "  Throughput: #{docs_per_sec} docs/s, #{mb_per_sec} MB/s"
        if bench_format == 'parquet'
          puts "  Settings: format=parquet, partitions=#{partitions}, batch_size=#{batch_size}, row_group=#{parquet_row_group}, compression=#{compression}"
        else
          puts "  Settings: format=jsonl, partitions=#{partitions}, batch_size=#{batch_size}, queue_mb=#{queue_mb}, rotate_mb=#{rotate_mb}, compression=#{compression}"
        end

        expect(rows).to eq(doc_count)
        expect(parts_meta.size).to be > 0
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1")
      end
    end
  end
end

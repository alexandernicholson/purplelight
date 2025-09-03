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

        client = Mongo::Client.new('mongodb://127.0.0.1:27018', server_api: {version: '1'})
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
        prefix = 'bench_items'
        t0 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        Purplelight.snapshot(
          client: db,
          collection: :items,
          output: export_dir,
          format: :jsonl,
          compression: :gzip, # choose gzip to avoid requiring zstd locally
          partitions: [Etc.respond_to?(:nprocessors) ? Etc.nprocessors * 2 : 4, 32].max,
          batch_size: 5000,
          query: {},
          sharding: { mode: :by_size, part_bytes: 512 * 1024 * 1024, prefix: prefix },
          resume: { enabled: true }
        )
        t1 = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        elapsed = t1 - t0

        manifest_path = File.join(export_dir, "#{prefix}.manifest.json")
        data = JSON.parse(File.read(manifest_path))
        rows = data.fetch('parts', []).sum { |p| p['rows'].to_i }

        parts = Dir[File.join(export_dir, "#{prefix}-part-*.jsonl*")]
        bytes = parts.sum { |p| File.size(p) }

        docs_per_sec = (rows / elapsed).round(2)
        mb_per_sec = (bytes.to_f / (1024 * 1024) / elapsed).round(2)

        puts "Benchmark results:"
        puts "  Inserted: #{inserted} docs in #{insert_elapsed.round(2)}s"
        puts "  Exported: #{rows} docs in #{elapsed.round(2)}s"
        puts "  Parts:    #{parts.size}, Bytes: #{bytes}"
        puts "  Throughput: #{docs_per_sec} docs/s, #{mb_per_sec} MB/s"

        expect(rows).to eq(doc_count)
        expect(parts.size).to be > 0
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1")
      end
    end
  end
end



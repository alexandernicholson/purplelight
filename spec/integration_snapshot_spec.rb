require 'spec_helper'
require 'tmpdir'
require 'json'
require 'zlib'

RSpec.describe 'End-to-end snapshot (JSONL, local Mongo or skip)' do
  def docker?
    system('docker --version > /dev/null 2>&1')
  end

  it 'exports a small collection and supports resume' do
    unless docker?
      skip 'Docker not available; skipping integration test'
    end

    Dir.mktmpdir('purplelight-e2e') do |dir|
      container = "pl-mongo-#{Time.now.to_i}"
      begin
        # Start MongoDB 7 container
        system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or skip 'failed to start docker'
        # Wait for readiness by polling the hello command via mongo shell
        30.times do
          ok = system("docker exec #{container} mongosh --eval 'db.runCommand({hello:1})' > /dev/null 2>&1")
          break if ok
          sleep 1
        end

        client = Mongo::Client.new('mongodb://127.0.0.1:27017', server_api: {version: '1'})
        coll = client[:users]
        docs = 1_000.times.map { |i| { _id: BSON::ObjectId.new, email: "user#{i}@ex.com", active: (i % 2 == 0) } }
        coll.insert_many(docs)
        active_docs = docs.select { |d| d[:active] }
        puts "Inserted docs: total=#{docs.size}, active=#{active_docs.size} (first 3 active):"
        puts active_docs.first(3).map { |d| d.inspect }

        # First run (JSONL)
        Purplelight.snapshot(
          client: client,
          collection: :users,
          output: dir,
          format: :jsonl,
          partitions: 4,
          batch_size: 200,
          query: { active: true },
          sharding: { mode: :by_size, part_bytes: 64 * 1024, prefix: 'users' },
          resume: { enabled: true },
        )

        manifest_path = File.join(dir, 'users.manifest.json')
        expect(File).to exist(manifest_path)
        parts = Dir[File.join(dir, 'users-part-*.jsonl*')]
        expect(parts).not_to be_empty
        puts "Generated parts after first run: #{parts.size}"
        # Show a few lines from the first part (gzip if fallback)
        first_part = parts.sort.first
        if first_part.end_with?(".gz")
          Zlib::GzipReader.open(first_part) do |gz|
            puts "Sample output lines from #{File.basename(first_part)}:"
            3.times do
              line = gz.gets
              break unless line
              puts line.strip
            end
          end
        else
          File.open(first_part, 'r') do |f|
            puts "Sample output lines from #{File.basename(first_part)}:"
            3.times do
              line = f.gets
              break unless line
              puts line.strip
            end
          end
        end

        # Simulate resume: insert more docs and run again; should append new parts and update manifest
        more = 200.times.map { |i| { _id: BSON::ObjectId.new, email: "late#{i}@ex.com", active: true } }
        coll.insert_many(more)
        puts "Inserted additional docs for resume: total=#{more.size} (first 3):"
        puts more.first(3).map { |d| d.inspect }
        Purplelight.snapshot(
          client: client,
          collection: :users,
          output: dir,
          format: :jsonl,
          partitions: 4,
          batch_size: 200,
          query: { active: true },
          sharding: { mode: :by_size, part_bytes: 64 * 1024, prefix: 'users' },
          resume: { enabled: true },
        )

        parts2 = Dir[File.join(dir, 'users-part-*.jsonl*')]
        puts "Generated parts after resume: #{parts2.size} (was #{parts.size})"
        expect(parts2.size).to be >= parts.size

        data = JSON.parse(File.read(manifest_path))
        total_rows = data.fetch('parts', []).sum { |p| p['rows'].to_i }
        puts "Manifest summary: parts=#{data.fetch('parts', []).size}, rows=#{total_rows}"

        # CSV single-file run
        Purplelight.snapshot(
          client: client,
          collection: :users,
          output: dir,
          format: :csv,
          query: { active: true },
          sharding: { mode: :single_file, prefix: 'users_csv' },
          resume: { enabled: true },
        )
        csv_parts = Dir[File.join(dir, 'users_csv.csv*')]
        expect(csv_parts.size).to eq(1)
        csv_path = csv_parts.first
        puts "CSV output path: #{File.basename(csv_path)}"
        if csv_path.end_with?(".gz")
          Zlib::GzipReader.open(csv_path) do |gz|
            puts "CSV sample lines:"
            3.times { puts(gz.gets&.strip) }
          end
        else
          File.open(csv_path, 'r') do |f|
            puts "CSV sample lines:"
            3.times { puts(f.gets&.strip) }
          end
        end
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1")
      end
    end
  end
end



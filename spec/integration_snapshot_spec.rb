# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'json'
require 'zlib'
require 'stringio'

RSpec.describe 'End-to-end snapshot (JSONL, local Mongo or skip)' do
  def docker?
    system('docker --version > /dev/null 2>&1')
  end

  def wait_for_mongo(url, timeout: 60)
    start = Time.now
    loop do
      client = Mongo::Client.new(url, server_api: { version: '1' })
      client.database.command(hello: 1)
      return true
    rescue StandardError => _e
      break if (Time.now - start) > timeout

      sleep 1
    end
    false
  end

  def arrow_available?
    require 'arrow'
    require 'parquet'
    true
  rescue LoadError
    false
  end

  it 'exports a small collection and supports resume' do
    Dir.mktmpdir('purplelight-e2e') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      begin
        # Prefer provided MONGO_URL; otherwise bring up a dockerized mongo
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'

        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' })
        coll = client[:users]
        docs = 1_000.times.map { |i| { _id: BSON::ObjectId.new, email: "user#{i}@ex.com", active: i.even? } }
        coll.insert_many(docs)
        active_docs = docs.select { |d| d[:active] }
        puts "Inserted docs: total=#{docs.size}, active=#{active_docs.size} (first 3 active):"
        puts(active_docs.first(3).map(&:inspect))

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
          resume: { enabled: true }
        )

        manifest_path = File.join(dir, 'users.manifest.json')
        expect(File).to exist(manifest_path)
        parts = Dir[File.join(dir, 'users-part-*.jsonl*')]
        expect(parts).not_to be_empty
        puts "Generated parts after first run: #{parts.size}"
        # Show a few lines from the first part (gzip if fallback)
        first_part = parts.min
        if first_part.end_with?('.gz')
          Zlib::GzipReader.open(first_part) do |gz|
            puts "Sample output lines from #{File.basename(first_part)}:"
            3.times do
              line = gz.gets
              break unless line

              puts line.strip
            end
          end
        elsif first_part.end_with?('.zst')
          # Prefer zstd-ruby single-shot API, fallback to ruby-zstds stream read
          puts "Sample output lines from #{File.basename(first_part)}:"
          if Object.const_defined?(:Zstd)
            data = Zstd.decompress(File.binread(first_part))
            StringIO.new(data).each_line.first(3).each { |line| puts line.strip }
          elsif defined?(ZSTDS)
            ZSTDS::Stream::Reader.open(first_part) do |zr|
              data = zr.read
              StringIO.new(data).each_line.first(3).each { |line| puts line.strip }
            end
          else
            raise 'zstd output produced but no zstd reader is available'
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
        puts(more.first(3).map(&:inspect))
        Purplelight.snapshot(
          client: client,
          collection: :users,
          output: dir,
          format: :jsonl,
          partitions: 4,
          batch_size: 200,
          query: { active: true },
          sharding: { mode: :by_size, part_bytes: 64 * 1024, prefix: 'users' },
          resume: { enabled: true }
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
          resume: { enabled: true }
        )
        csv_parts = Dir[File.join(dir, 'users_csv.csv*')]
        expect(csv_parts.size).to eq(1)
        csv_path = csv_parts.first
        puts "CSV output path: #{File.basename(csv_path)}"
        if csv_path.end_with?('.gz')
          Zlib::GzipReader.open(csv_path) do |gz|
            puts 'CSV sample lines:'
            3.times { puts(gz.gets&.strip) }
          end
        elsif csv_path.end_with?('.zst')
          puts 'CSV sample lines:'
          if Object.const_defined?(:Zstd)
            data = Zstd.decompress(File.binread(csv_path))
            StringIO.new(data).each_line.first(3).each { |line| puts line&.strip }
          elsif defined?(ZSTDS)
            ZSTDS::Stream::Reader.open(csv_path) do |zr|
              data = zr.read
              StringIO.new(data).each_line.first(3).each { |line| puts line&.strip }
            end
          else
            raise 'zstd output produced but no zstd reader is available'
          end
        else
          File.open(csv_path, 'r') do |f|
            puts 'CSV sample lines:'
            3.times { puts(f.gets&.strip) }
          end
        end

        # Parquet single-file run (optional)
        if arrow_available?
          Purplelight.snapshot(
            client: client,
            collection: :users,
            output: dir,
            format: :parquet,
            query: { active: true },
            sharding: { mode: :single_file, prefix: 'users_parquet' },
            resume: { enabled: true }
          )
          pq = Dir[File.join(dir, 'users_parquet.parquet')]
          puts "Parquet output: #{File.basename(pq.first)}" unless pq.empty?
          expect(pq.size).to eq(1)
        else
          puts 'Arrow/Parquet not available; skipping Parquet test'
        end
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end
end

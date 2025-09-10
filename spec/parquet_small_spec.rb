# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'json'

RSpec.describe 'Parquet small collections (requires Arrow/Parquet)' do
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

  before do
    skip 'Arrow/Parquet not available; set Gemfile :parquet group' unless arrow_available?
  end

  it 'exports a tiny collection to Parquet in single_file mode' do
    Dir.mktmpdir('purplelight-parquet-small') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-parquet-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' })
        coll = client[:tiny_pq]
        docs = [
          { _id: BSON::ObjectId.new, email: 'a@ex.com', active: true, n: 1 },
          { _id: BSON::ObjectId.new, email: 'b@ex.com', active: false, n: 2 },
          { _id: BSON::ObjectId.new, email: 'c@ex.com', active: true, n: 3 }
        ]
        coll.insert_many(docs)

        Purplelight.snapshot(
          client: client,
          collection: :tiny_pq,
          output: dir,
          format: :parquet,
          sharding: { mode: :single_file, prefix: 'tiny_parquet' },
          resume: { enabled: true }
        )

        pq = Dir[File.join(dir, 'tiny_parquet.parquet')]
        expect(pq.size).to eq(1)

        # Validate content via Arrow
        table = Arrow::Table.load(pq.first, format: :parquet)
        expect(table.n_rows).to eq(3)
        expect(table.column_names).to include('email', 'active', 'n', '_id')
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'exports a tiny collection to Parquet with by_size sharding producing one part' do
    Dir.mktmpdir('purplelight-parquet-small2') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-parquet2-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' })
        coll = client[:tiny_pq_parts]
        docs = [
          { _id: BSON::ObjectId.new, email: 'x@ex.com', active: true, n: 10 },
          { _id: BSON::ObjectId.new, email: 'y@ex.com', active: true, n: 20 }
        ]
        coll.insert_many(docs)

        Purplelight.snapshot(
          client: client,
          collection: :tiny_pq_parts,
          output: dir,
          format: :parquet,
          sharding: { mode: :by_size, part_bytes: 64 * 1024 * 1024, prefix: 'tiny_parquet_parts' },
          partitions: 2,
          batch_size: 50,
          resume: { enabled: true }
        )

        pq_parts = Dir[File.join(dir, 'tiny_parquet_parts-part-*.parquet')]
        expect(pq_parts.size).to eq(1)

        table = Arrow::Table.load(pq_parts.first, format: :parquet)
        expect(table.n_rows).to eq(2)
        expect(table.column_names).to include('email', 'active', 'n', '_id')
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end
end

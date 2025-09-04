# frozen_string_literal: true

require 'spec_helper'
require 'tmpdir'
require 'json'
require 'zlib'

RSpec.describe 'Query filtering' do
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

  it 'respects the query filter when exporting' do
    Dir.mktmpdir('purplelight-query') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-query-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'

        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' })
        coll = client[:qtest]
        coll.insert_many([
                           { _id: BSON::ObjectId.new, status: 'active', n: 1 },
                           { _id: BSON::ObjectId.new, status: 'inactive', n: 2 },
                           { _id: BSON::ObjectId.new, status: 'active', n: 3 }
                         ])

        Purplelight.snapshot(
          client: client,
          collection: :qtest,
          output: dir,
          format: :jsonl,
          partitions: 2,
          batch_size: 100,
          query: { status: 'active' },
          sharding: { mode: :by_size, part_bytes: 512 * 1024 * 1024, prefix: 'qtest' },
          resume: { enabled: true }
        )

        # Verify that only active docs are present
        files = Dir[File.join(dir, 'qtest-part-*.jsonl*')]
        expect(files.size).to eq(1)
        path = files.first
        lines = []
        if path.end_with?('.gz')
          Zlib::GzipReader.open(path) do |gz|
            lines = gz.each_line.first(10)
          end
        else
          File.open(path, 'r') { |f| lines = f.each_line.first(10) }
        end
        decoded = lines.map { |l| JSON.parse(l) }
        expect(decoded.all? { |d| d['status'] == 'active' }).to be true
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end
end

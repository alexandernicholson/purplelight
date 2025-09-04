require 'spec_helper'
require 'tmpdir'
require 'json'

RSpec.describe 'Hint usage' do
  def docker?
    system('docker --version > /dev/null 2>&1')
  end

  def wait_for_mongo(url, timeout: 60)
    start = Time.now
    loop do
      begin
        client = Mongo::Client.new(url, server_api: { version: '1' })
        client.database.command(hello: 1)
        return true
      rescue => _e
        break if (Time.now - start) > timeout

        sleep 1
      end
    end
    false
  end

  it 'forces index usage when hint is provided' do
    mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
    container = nil
    begin
      if ENV['MONGO_URL']
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url)
      else
        if docker?
          container = "pl-mongo-hint-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
          raise 'Mongo not reachable' unless wait_for_mongo(mongo_url)
        else
          raise 'Mongo not available (no MONGO_URL and no Docker)'
        end
      end

      client = Mongo::Client.new(mongo_url, server_api: { version: '1' })
      coll = client[:hints]
      coll.drop_indexes rescue nil
      coll.insert_many((1..1000).map { |i| { _id: BSON::ObjectId.new, created_at: Time.at(i), n: i } })
      coll.indexes.create_one({ created_at: 1 })

      # Use explain to ensure the hint is propagated
      filter = { created_at: { '$gt' => Time.at(100) } }
      explained = coll.find(filter, hint: { created_at: 1 }).explain
      plan_str = explained.to_h.to_json
      expect(plan_str).to include('created_at_1')

      # Also run a snapshot with the same hint; ensure it runs without error
      Dir.mktmpdir('purplelight-hint') do |dir|
        Purplelight.snapshot(
          client: client,
          collection: :hints,
          output: dir,
          format: :jsonl,
          partitions: 2,
          batch_size: 100,
          query: filter,
          hint: { created_at: 1 },
          sharding: { mode: :by_size, part_bytes: 64 * 1024 * 1024, prefix: 'hints' },
          resume: { enabled: true }
        )
        files = Dir[File.join(dir, 'hints-part-*.jsonl*')]
        expect(files.size).to be > 0
      end
    ensure
      system("docker rm -f #{container} > /dev/null 2>&1") if container
    end
  end
end

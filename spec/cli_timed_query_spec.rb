# frozen_string_literal: true

require 'English'
require 'spec_helper'
require 'tmpdir'
require 'json'
require 'zlib'
require 'stringio'
require 'time'

RSpec.describe 'CLI timed query end-to-end' do
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

  let(:bin) { File.expand_path('../bin/purplelight', __dir__) }

  it 'exports only docs within a time range via CLI --query' do
    Dir.mktmpdir('purplelight-cli-timed') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "tqtest_#{Time.now.to_i}"
      prefix = 'cli_timed'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-cli-timed-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        base = Time.now.utc - 3600
        docs = 10.times.map do |i|
          { _id: BSON::ObjectId.new, created_at: base + (i * 60), n: i }
        end
        coll.insert_many(docs)

        range_start = (base + (3 * 60)).iso8601
        range_end = (base + (7 * 60)).iso8601
        # Extended JSON with $date so CLI parses to Time via BSON::ExtJSON
        query_json = JSON.generate({ 'created_at' => { '$gte' => { '$date' => range_start }, '$lt' => { '$date' => range_end } } })

        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--partitions 2',
          "--query '#{query_json}'"
        ].join(' ')

        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        # Read produced parts
        files = Dir[File.join(dir, "#{prefix}-part-*.jsonl*")]
        expect(files).not_to be_empty
        path = files.first
        lines = []
        if path.end_with?('.gz')
          Zlib::GzipReader.open(path) { |gz| lines = gz.each_line.first(50) }
        elsif path.end_with?('.zst')
          if Object.const_defined?(:Zstd)
            data = Zstd.decompress(File.binread(path))
            lines = StringIO.new(data).each_line.first(50)
          elsif defined?(ZSTDS)
            ZSTDS::Stream::Reader.open(path) do |zr|
              data = zr.read
              lines = StringIO.new(data).each_line.first(50)
            end
          else
            raise 'zstd output produced but no zstd reader is available'
          end
        else
          File.open(path, 'r') { |f| lines = f.each_line.first(50) }
        end

        decoded = lines.map { |l| JSON.parse(l) }
        expect(decoded).not_to be_empty

        rstart = Time.parse(range_start)
        rend = Time.parse(range_end)
        parsed_times = decoded.map { |d| Time.parse(d['created_at']) }
        expect(parsed_times.all? { |t| (t >= rstart) && (t < rend) }).to be true

        expected_count = docs.count { |d| (d[:created_at] >= rstart) && (d[:created_at] < rend) }
        expect(decoded.size).to eq(expected_count)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end
end

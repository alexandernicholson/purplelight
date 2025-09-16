# frozen_string_literal: true

require 'English'
require 'spec_helper'
require 'tmpdir'
require 'json'

RSpec.describe 'CLI tunables via flags' do
  let(:bin) { File.expand_path('../bin/purplelight', __dir__) }

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

  it 'applies --queue-mb and --rotate-mb and records in manifest' do
    Dir.mktmpdir('purplelight-cli-tuning') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_tuning_#{Time.now.to_i}"
      prefix = 'tune'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?
          container = "pl-mongo-cli-tuning-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        coll.insert_many(5.times.map { |i| { _id: BSON::ObjectId.new, n: i } })

        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--queue-mb 300',
          '--rotate-mb 64'
        ].join(' ')
        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        manifest_path = File.join(dir, "#{prefix}.manifest.json")
        data = JSON.parse(File.read(manifest_path))
        opts = data['options']
        expect(opts['queue_size_bytes']).to eq(300 * 1024 * 1024)
        expect(opts['rotate_bytes']).to eq(64 * 1024 * 1024)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'applies --compression-level and records it in manifest' do
    Dir.mktmpdir('purplelight-cli-level') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_level_#{Time.now.to_i}"
      prefix = 'lvl'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?
          container = "pl-mongo-cli-level-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        coll.insert_many(3.times.map { { _id: BSON::ObjectId.new, v: 1 } })

        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--compression zstd',
          '--compression-level 6'
        ].join(' ')
        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        manifest_path = File.join(dir, "#{prefix}.manifest.json")
        data = JSON.parse(File.read(manifest_path))
        expect(data['options']['compression_level']).to eq(6)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'applies --parquet-row-group when format=parquet (if Arrow available)' do
    begin
      require 'arrow'
      require 'parquet'
    rescue LoadError
      skip 'Arrow not available'
    end

    Dir.mktmpdir('purplelight-cli-rowgroup') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_rowgroup_#{Time.now.to_i}"
      prefix = 'pqrg'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?
          container = "pl-mongo-cli-rowgroup-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        coll.insert_many(10.times.map { |i| { _id: BSON::ObjectId.new, n: i } })

        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format parquet',
          '--single-file',
          '--parquet-row-group 12345'
        ].join(' ')
        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        manifest_path = File.join(dir, "#{prefix}.manifest.json")
        data = JSON.parse(File.read(manifest_path))
        expect(data['options']['parquet_row_group']).to eq(12_345)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'applies --read-concern, --no-cursor-timeout, and records them' do
    Dir.mktmpdir('purplelight-cli-read') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_read_#{Time.now.to_i}"
      prefix = 'read'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?
          container = "pl-mongo-cli-read-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        coll.insert_many(2.times.map { { _id: BSON::ObjectId.new, v: 1 } })

        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--read-concern local',
          '--no-cursor-timeout false'
        ].join(' ')
        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        data = JSON.parse(File.read(File.join(dir, "#{prefix}.manifest.json")))
        expect(data['options']['read_concern']).to eq('level' => 'local')
        expect(data['options']['no_cursor_timeout']).to eq(false)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'forces telemetry on/off via --telemetry and records flag' do
    Dir.mktmpdir('purplelight-cli-telemetry') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_tel_#{Time.now.to_i}"
      prefix = 'tel'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?
          container = "pl-mongo-cli-telemetry-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        coll.insert_many([{ _id: BSON::ObjectId.new, v: 1 }])

        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--telemetry on'
        ].join(' ')
        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        data = JSON.parse(File.read(File.join(dir, "#{prefix}.manifest.json")))
        expect(data['options']['telemetry']).to eq(true)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'overwrites incompatible manifests when --resume-overwrite-incompatible is passed' do
    Dir.mktmpdir('purplelight-cli-resume-overwrite') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_res_over_#{Time.now.to_i}"
      prefix = 'ow'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?
          container = "pl-mongo-cli-res-over-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        coll.insert_many([{ _id: BSON::ObjectId.new, v: 1 }])

        # First run creates a manifest
        cmd1 = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl'
        ].join(' ')
        `#{cmd1}`
        expect($CHILD_STATUS.success?).to be true

        manifest_path = File.join(dir, "#{prefix}.manifest.json")
        data1 = JSON.parse(File.read(manifest_path))
        # Tamper with manifest to make it incompatible
        data1['format'] = 'csv'
        File.write(manifest_path, JSON.pretty_generate(data1))

        cmd2 = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--resume-overwrite-incompatible'
        ].join(' ')
        `#{cmd2}`
        expect($CHILD_STATUS.success?).to be true

        data2 = JSON.parse(File.read(manifest_path))
        expect(data2['format']).to eq('jsonl')
        expect(data2['options']['resume_overwrite_incompatible']).to eq(true)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end
end

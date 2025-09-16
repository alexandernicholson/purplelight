# frozen_string_literal: true

require 'English'
require 'spec_helper'
require 'tmpdir'
require 'json'
require 'zlib'
require 'stringio'
require 'time'

begin
  require 'zstds'
rescue LoadError
  warn 'zstds gem not available; falling back to gzip in tests'
end
begin
  require 'zstd-ruby'
rescue LoadError
  warn 'zstd-ruby gem not available; falling back to ruby-zstds or gzip in tests'
end

RSpec.describe 'CLI additional end-to-end' do
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

  def read_text_lines(path, max_lines: nil)
    lines = []
    if path.end_with?('.gz')
      Zlib::GzipReader.open(path) { |gz| lines = gz.each_line.to_a }
    elsif path.end_with?('.zst')
      if Object.const_defined?(:Zstd)
        data = Zstd.decompress(File.binread(path))
        lines = StringIO.new(data).each_line.to_a
      elsif defined?(ZSTDS)
        ZSTDS::Stream::Reader.open(path) do |zr|
          data = zr.read
          lines = StringIO.new(data).each_line.to_a
        end
      else
        raise 'zstd output produced but no zstd reader is available'
      end
    else
      File.open(path, 'r') { |f| lines = f.each_line.to_a }
    end
    max_lines ? lines.first(max_lines) : lines
  end

  let(:bin) { File.expand_path('../bin/purplelight', __dir__) }

  it 'applies projection via --projection and only emits selected fields' do
    Dir.mktmpdir('purplelight-cli-projection') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_proj_#{Time.now.to_i}"
      prefix = 'proj'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-cli-proj-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        docs = 5.times.map do |i|
          { _id: BSON::ObjectId.new, email: "user#{i}@ex.com", active: i.even?, role: (i.even? ? 'reader' : 'writer') }
        end
        coll.insert_many(docs)

        proj = JSON.generate({ '_id' => 1, 'email' => 1 })
        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--compression gzip',
          "--projection '#{proj}'"
        ].join(' ')

        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        files = Dir[File.join(dir, "#{prefix}-part-*.jsonl*")]
        expect(files).not_to be_empty
        decoded = read_text_lines(files.first).map { |l| JSON.parse(l) }
        expect(decoded).not_to be_empty
        decoded.each do |d|
          expect(d.keys.sort).to eq(%w[_id email])
        end
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'emits CSV in single-file mode with gzip and correct header' do
    Dir.mktmpdir('purplelight-cli-csv') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_csv_#{Time.now.to_i}"
      prefix = 'csv_single'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-cli-csv-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        docs = 10.times.map do |i|
          { _id: BSON::ObjectId.new, email: "user#{i}@ex.com", active: i.even? }
        end
        coll.insert_many(docs)

        cmd = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format csv',
          '--single-file',
          '--compression gzip'
        ].join(' ')

        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        files = Dir[File.join(dir, "#{prefix}.csv*")]
        expect(files.size).to eq(1)
        header, *rows = read_text_lines(files.first)
        expect(header&.strip).to eq('_id,active,email')
        expect(rows.size).to eq(docs.size)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'honors compression flag for jsonl: gzip and none' do
    Dir.mktmpdir('purplelight-cli-compression') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_comp_#{Time.now.to_i}"
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-cli-comp-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]
        docs = 6.times.map { { _id: BSON::ObjectId.new, v: rand(1000) } }
        coll.insert_many(docs)

        # gzip
        prefix_gz = 'comp_gz'
        cmd_gz = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix_gz}",
          '--format jsonl',
          '--compression gzip'
        ].join(' ')
        `#{cmd_gz}`
        expect($CHILD_STATUS.success?).to be true
        files_gz = Dir[File.join(dir, "#{prefix_gz}-part-*.jsonl.gz")]
        expect(files_gz).not_to be_empty
        lines_gz = read_text_lines(files_gz.first)
        expect(lines_gz.size).to eq(docs.size)

        # none
        prefix_none = 'comp_none'
        cmd_none = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix_none}",
          '--format jsonl',
          '--compression none'
        ].join(' ')
        `#{cmd_none}`
        expect($CHILD_STATUS.success?).to be true
        files_none = Dir[File.join(dir, "#{prefix_none}-part-*.jsonl")]
        expect(files_none).not_to be_empty
        lines_none = read_text_lines(files_none.first)
        expect(lines_none.size).to eq(docs.size)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'supports resume via CLI and avoids duplicates' do
    Dir.mktmpdir('purplelight-cli-resume') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_resume_#{Time.now.to_i}"
      prefix = 'resume'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-cli-resume-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        client = Mongo::Client.new(mongo_url, server_api: { version: '1' }).use(db_name)
        coll = client[coll_name.to_sym]

        first = 50.times.map { |i| { _id: BSON::ObjectId.new, email: "a#{i}@ex.com", active: true } }
        coll.insert_many(first)

        cmd1 = [
          bin,
          "--uri #{mongo_url}",
          "--db #{db_name}",
          "--collection #{coll_name}",
          "--output #{dir}",
          "--prefix #{prefix}",
          '--format jsonl',
          '--compression gzip'
        ].join(' ')
        `#{cmd1}`
        expect($CHILD_STATUS.success?).to be true

        manifest_path = File.join(dir, "#{prefix}.manifest.json")
        expect(File).to exist(manifest_path)
        data1 = JSON.parse(File.read(manifest_path))
        rows1 = data1.fetch('parts', []).sum { |p| p['rows'].to_i }

        more = 30.times.map { |i| { _id: BSON::ObjectId.new, email: "b#{i}@ex.com", active: true } }
        coll.insert_many(more)

        cmd2 = cmd1
        `#{cmd2}`
        expect($CHILD_STATUS.success?).to be true
        data2 = JSON.parse(File.read(manifest_path))
        rows2 = data2.fetch('parts', []).sum { |p| p['rows'].to_i }
        expect(rows2).to be >= rows1

        files = Dir[File.join(dir, "#{prefix}-part-*.jsonl*")]
        ids = []
        files.sort.each do |f|
          read_text_lines(f).each do |line|
            doc = JSON.parse(line)
            ids << doc['_id']
          end
        end
        expect(ids.uniq.size).to eq(ids.size)
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'honors --prefix for output filenames' do
    Dir.mktmpdir('purplelight-cli-prefix') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_prefix_#{Time.now.to_i}"
      prefix = 'mycustom'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-cli-prefix-#{Time.now.to_i}"
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
          '--compression gzip'
        ].join(' ')
        `#{cmd}`
        expect($CHILD_STATUS.success?).to be true

        files = Dir[File.join(dir, "#{prefix}-part-*.jsonl*")]
        expect(files).not_to be_empty
        expect(File.basename(files.first)).to start_with("#{prefix}-part-")
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end

  it 'exits with error when required options are missing' do
    bin_path = File.expand_path('../bin/purplelight', __dir__)
    out = `#{bin_path} --db db --collection c --output /tmp 2>&1`
    expect($CHILD_STATUS.exitstatus).to eq(1)
    expect(out).to include('Missing required option: --uri')
    expect(out).to include('Usage: purplelight snapshot')
  end

  it 'emits Parquet single-file via CLI when Arrow available' do
    Dir.mktmpdir('purplelight-cli-parquet') do |dir|
      container = nil
      mongo_url = ENV['MONGO_URL'] || 'mongodb://127.0.0.1:27017'
      db_name = 'db'
      coll_name = "cli_parquet_#{Time.now.to_i}"
      prefix = 'pq_single'
      begin
        unless ENV['MONGO_URL']
          raise 'Mongo not available (no MONGO_URL and no Docker)' unless docker?

          container = "pl-mongo-cli-parquet-#{Time.now.to_i}"
          system("docker run -d --rm --name #{container} -p 27017:27017 mongo:7 > /dev/null") or raise 'failed to start docker'
        end
        raise 'Mongo not reachable' unless wait_for_mongo(mongo_url, timeout: 60)

        if arrow_available?
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
            '--format parquet',
            '--single-file'
          ].join(' ')
          `#{cmd}`
          expect($CHILD_STATUS.success?).to be true
          pq = Dir[File.join(dir, "#{prefix}.parquet")]
          expect(pq.size).to eq(1)
        else
          puts 'Arrow/Parquet not available; skipping CLI Parquet test'
        end
      ensure
        system("docker rm -f #{container} > /dev/null 2>&1") if container
      end
    end
  end
end

# frozen_string_literal: true

require 'English'
require 'spec_helper'

RSpec.describe 'CLI' do
  let(:bin) { File.expand_path('../bin/purplelight', __dir__) }

  it '--version prints version' do
    out = `#{bin} --version`
    expect(out.strip).to eq(Purplelight::VERSION)
  end

  it '--help prints usage' do
    out = `#{bin} --help`
    expect(out).to include('Usage: purplelight snapshot')
  end

  it '--dry-run with read-preference and tags prints effective JSON' do
    out = `#{bin} --dry-run --uri mongodb://localhost --db db --collection c --output /tmp --read-preference secondary --read-tags nodeType=ANALYTICS,region=EAST`
    parsed = JSON.parse(out)
    expect(parsed['read_preference']).to eq({ 'mode' => 'secondary',
                                              'tag_sets' => [{ 'nodeType' => 'ANALYTICS', 'region' => 'EAST' }] })
  end

  it '--query accepts Extended JSON with $date (parses to Time) in dry-run mode' do
    # Ensure it doesn't crash parsing the query; --dry-run only prints read_preference JSON today
    query = '{"created_at": {"$gte": {"$date": "2024-01-01T00:00:00Z"}}}'
    `#{bin} --dry-run --uri mongodb://localhost --db db --collection c --output /tmp --query '#{query}'`
    expect($CHILD_STATUS.success?).to be true
  end
end

require 'spec_helper'

RSpec.describe 'CLI' do
  let(:bin) { File.expand_path('../../bin/purplelight', __FILE__) }

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
    expect(parsed['read_preference']).to eq({'mode' => 'secondary', 'tag_sets' => [{'nodeType' => 'ANALYTICS', 'region' => 'EAST'}]})
  end
end



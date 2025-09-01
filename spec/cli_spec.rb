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
end



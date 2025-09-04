require 'spec_helper'

RSpec.describe 'Gem interface' do
  it 'exposes Purplelight.snapshot and version' do
    expect(Purplelight::VERSION).to match(/\d+\.\d+\.\d+/)
    expect(Purplelight).to respond_to(:snapshot)
  end
end

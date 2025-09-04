require 'spec_helper'

RSpec.describe 'Purplelight basic loadability' do
  it 'has a version' do
    expect(Purplelight::VERSION).to match(/\d+\.\d+\.\d+/)
  end
end

# frozen_string_literal: true

require 'rake/testtask'

task default: [:spec]

begin
  require 'rspec/core/rake_task'
  RSpec::Core::RakeTask.new(:spec)
rescue LoadError
  task :spec do
    sh 'echo "RSpec not installed"'
  end
end

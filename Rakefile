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

desc 'Run the bounded microbenchmark suite'
task :microbench do
  ruby 'benchmark/microbench.rb'
end

desc 'Profile all microbenchmarks and render CPU/allocation flamegraphs'
task :profile do
  ruby 'benchmark/profile.rb --mode cpu'
  ruby 'benchmark/profile.rb --mode object'
end

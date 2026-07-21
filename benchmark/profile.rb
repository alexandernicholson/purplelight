# frozen_string_literal: true

require 'bundler/setup'
require 'fileutils'
require 'optparse'
require 'stackprof'

mode = :cpu
OptionParser.new do |parser|
  parser.on('--mode MODE', %w[cpu object], 'StackProf sampling mode') { |value| mode = value.to_sym }
end.parse!

root = File.expand_path('..', __dir__)
profile_directory = File.join(root, 'tmp', 'profiles')
FileUtils.mkdir_p(profile_directory)
dump_path = File.join(profile_directory, "#{mode}.dump")
flamegraph_path = File.join(profile_directory, "#{mode}-flamegraph.html")
benchmark_output = File.join(profile_directory, "#{mode}-benchmark.json")
interval = mode == :object ? 100 : 1_000

original_arguments = ARGV.dup
ARGV.replace(['--output', benchmark_output])
StackProf.run(mode:, interval:, raw: true, out: dump_path) do
  load File.join(__dir__, 'microbench.rb')
end
ARGV.replace(original_arguments)

File.open(flamegraph_path, 'w') do |file|
  StackProf::Report.from_file(dump_path).print_d3_flamegraph(file)
end

puts "StackProf dump: #{dump_path}"
puts "Flamegraph: #{flamegraph_path}"

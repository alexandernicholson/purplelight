# frozen_string_literal: true

require 'benchmark'
require 'json'
require 'timeout'

module Purplelight
  module Microbench
    Case = Data.define(:name, :paths, :iterations, :block)

    # Runs registered microbenchmarks under strict duration and coverage limits.
    class Harness
      MAX_CASE_SECONDS = 2.0
      SAMPLES = 5
      REQUIRED_PATHS = %i[
        manifest partitioner queue snapshot telemetry writer_csv writer_jsonl writer_parquet
      ].freeze

      def initialize
        @cases = []
      end

      def register(name, paths:, iterations: 1, &block)
        raise ArgumentError, 'benchmark block required' unless block

        @cases << Case.new(name:, paths: Array(paths).map(&:to_sym), iterations:, block:)
      end

      def run(baseline_path: nil)
        verify_coverage!
        results = @cases.to_h { |benchmark_case| [benchmark_case.name, measure(benchmark_case)] }
        report = {
          generated_at: Time.now.utc.iso8601,
          coverage: coverage,
          max_case_seconds: MAX_CASE_SECONDS,
          samples: SAMPLES,
          results:
        }
        report[:comparison] = compare(results, JSON.parse(File.read(baseline_path))) if baseline_path
        report
      end

      def coverage
        covered = @cases.flat_map(&:paths).uniq
        {
          covered: covered.sort,
          required: REQUIRED_PATHS,
          percent: (covered.length.fdiv(REQUIRED_PATHS.length) * 100).round(2)
        }
      end

      private

      def verify_coverage!
        missing = REQUIRED_PATHS - @cases.flat_map(&:paths)
        raise "missing benchmark paths: #{missing.join(', ')}" unless missing.empty?
      end

      def measure(benchmark_case)
        samples, allocated_objects = Timeout.timeout(MAX_CASE_SECONDS) do
          elapsed = Array.new(SAMPLES) do
            Benchmark.realtime do
              benchmark_case.iterations.times { benchmark_case.block.call }
            end
          end
          allocations_before = GC.stat(:total_allocated_objects)
          benchmark_case.iterations.times { benchmark_case.block.call }
          allocations = GC.stat(:total_allocated_objects) - allocations_before
          [elapsed, allocations]
        end
        median = samples.sort.fetch(SAMPLES / 2)
        {
          iterations: benchmark_case.iterations,
          seconds: samples.sum.round(6),
          sample_seconds: samples.map { |elapsed| elapsed.round(6) },
          operations_per_second: (benchmark_case.iterations / median).round(2),
          allocated_objects:,
          allocations_per_operation: allocated_objects.fdiv(benchmark_case.iterations).round(2)
        }
      rescue Timeout::Error
        raise "#{benchmark_case.name} exceeded #{MAX_CASE_SECONDS}s"
      end

      def compare(results, baseline)
        old_results = baseline.fetch('results')
        results.to_h do |name, result|
          old_result = old_results[name.to_s]
          next [name, { baseline: false }] unless old_result

          old_rate = old_result.fetch('operations_per_second')
          delta = ((result[:operations_per_second] / old_rate) - 1.0) * 100
          comparison = { percent: delta.round(2), significant: delta.abs > 3.0 }
          if old_result['allocations_per_operation']
            old_allocations = old_result.fetch('allocations_per_operation')
            new_allocations = result[:allocations_per_operation]
            allocation_delta = if old_allocations.zero?
                                 (new_allocations.zero? ? 0.0 : nil)
                               else
                                 (((new_allocations / old_allocations) - 1.0) * 100)
                               end
            comparison[:allocation_percent] = allocation_delta&.round(2)
          end
          [name, comparison]
        end
      end
    end
  end
end

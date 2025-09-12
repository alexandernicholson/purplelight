# frozen_string_literal: true

require 'mongo'

module Purplelight
  # Partitioner builds MongoDB range filters to split work across workers.
  #
  # Given a Mongo collection and an optional base query, it returns N
  # contiguous `_id` ranges that can be processed independently while
  # maintaining ascending order. Optimized for ObjectId-based `_id`.
  class Partitioner
    # Builds contiguous _id range filters for N partitions.
    # For ObjectId _id, we sample quantiles to split into near-equal document counts.
    def self.object_id_partitions(collection:, query:, partitions:, mode: nil, telemetry: nil)
      # Choose planning mode: :timestamp (fast), :cursor (legacy)
      chosen_mode = (mode || ENV['PL_PARTITIONER_MODE'] || :timestamp).to_sym
      telemetry ||= (defined?(Telemetry) ? Telemetry::NULL : nil)

      return cursor_sampling_partitions(collection: collection, query: query, partitions: partitions) if chosen_mode == :cursor

      timestamp_partitions(collection: collection, query: query, partitions: partitions, telemetry: telemetry)
    end

    def self.simple_ranges(collection:, query:, partitions:)
      # Split by _id quantiles using min/max endpoints
      min_id = collection.find(query || {}).projection(_id: 1).sort(_id: 1).limit(1).first&.dig('_id')
      max_id = collection.find(query || {}).projection(_id: 1).sort(_id: -1).limit(1).first&.dig('_id')
      return [{ filter: query || {}, sort: { _id: 1 } }] if min_id.nil? || max_id.nil?

      # Create contiguous ranges using ascending inner boundaries.
      # We intentionally skip the very first _id so the first range includes the smallest document.
      inner_boundaries = collection.find(query || {})
                                   .projection(_id: 1)
                                   .sort(_id: 1)
                                   .skip(1)
                                   .limit([partitions - 1, 0].max)
                                   .to_a
                                   .map { |d| d['_id'] }

      ranges = []
      prev = nil
      inner_boundaries.each do |b|
        ranges << build_range(prev, b)
        prev = b
      end
      ranges << build_range(prev, nil)

      ranges.map do |r|
        filter = query ? query.dup : {}
        filter['_id'] = r
        { filter: filter, sort: { _id: 1 }, hint: { _id: 1 } }
      end
    end

    # Faster planning using ObjectId timestamps: O(partitions) indexed lookups
    def self.timestamp_partitions(collection:, query:, partitions:, telemetry: nil)
      t_minmax = telemetry&.start(:plan_minmax_time)
      min_id = collection.find(query || {}).projection(_id: 1).sort(_id: 1).limit(1).first&.dig('_id')
      max_id = collection.find(query || {}).projection(_id: 1).sort(_id: -1).limit(1).first&.dig('_id')
      telemetry&.finish(:plan_minmax_time, t_minmax)

      return [{ filter: query || {}, sort: { _id: 1 } }] if min_id.nil? || max_id.nil?

      min_ts = min_id.respond_to?(:generation_time) ? min_id.generation_time.to_i : nil
      max_ts = max_id.respond_to?(:generation_time) ? max_id.generation_time.to_i : nil

      # Fallback to cursor sampling if _id isn't anObjectId
      return cursor_sampling_partitions(collection: collection, query: query, partitions: partitions) if min_ts.nil? || max_ts.nil? || max_ts <= min_ts

      step = [(max_ts - min_ts) / partitions, 1].max
      inner_boundaries = []
      t_boundaries = telemetry&.start(:plan_boundary_queries_time)
      1.upto(partitions - 1) do |i|
        target_ts = min_ts + (step * i)
        candidate = BSON::ObjectId.from_time(Time.at(target_ts))
        f = query ? query.dup : {}
        f['_id'] = { '$gt' => candidate }
        b = collection.find(f).projection(_id: 1).sort(_id: 1).hint(_id: 1).limit(1).first&.dig('_id')
        inner_boundaries << b if b
      end
      telemetry&.finish(:plan_boundary_queries_time, t_boundaries)

      # Build ranges: first range has nil lower bound to include min_id,
      # middle ranges are (prev, current], and last is (last, +inf)
      ranges = []
      t_ranges = telemetry&.start(:plan_ranges_build_time)
      prev = nil
      inner_boundaries.each do |b|
        ranges << build_range(prev, b)
        prev = b
      end
      ranges << build_range(prev, nil)
      telemetry&.finish(:plan_ranges_build_time, t_ranges)

      ranges.map do |r|
        filter = query ? query.dup : {}
        filter['_id'] = r
        { filter: filter, sort: { _id: 1 }, hint: { _id: 1 } }
      end
    end

    # Legacy cursor sampling planner
    def self.cursor_sampling_partitions(collection:, query:, partitions:)
      # Ensure sort order for sampling
      base_query = collection.find(query || {}, {}.merge(sort: { _id: 1 }))

      # Fast path: if small dataset, just chunk by count
      total = collection.estimated_document_count
      return simple_ranges(collection: collection, query: query, partitions: partitions) if total <= partitions * 5_000

      # Sample boundaries: take approx quantiles by skipping
      step = [total / partitions, 1].max
      boundaries = []
      cursor = base_query.projection(_id: 1).batch_size(1_000).no_cursor_timeout
      i = 0
      cursor.each do |doc|
        boundaries << doc['_id'] if (i % step).zero?
        i += 1
        break if boundaries.size >= partitions
      end

      ranges = []
      prev = nil
      boundaries.each_with_index do |b, idx|
        if idx.zero?
          prev = nil
          next
        end
        ranges << build_range(prev, b)
        prev = b
      end
      ranges << build_range(prev, nil)

      ranges.map do |r|
        filter = query ? query.dup : {}
        filter['_id'] = r
        { filter: filter, sort: { _id: 1 }, hint: { _id: 1 } }
      end
    end

    def self.build_range(from_id, to_id)
      if from_id && to_id
        { '$gt' => from_id, '$lte' => to_id }
      elsif from_id && !to_id
        { '$gt' => from_id }
      elsif !from_id && to_id
        { '$lte' => to_id }
      else
        {}
      end
    end
  end
end

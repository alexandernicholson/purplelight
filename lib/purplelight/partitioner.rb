# frozen_string_literal: true

require 'mongo'

module Purplelight
  class Partitioner
    # Builds contiguous _id range filters for N partitions.
    # For ObjectId _id, we sample quantiles to split into near-equal document counts.
    def self.object_id_partitions(collection:, query:, partitions:)
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

    def self.simple_ranges(collection:, query:, partitions:)
      # Split by _id quantiles using min/max endpoints
      min_id = collection.find(query || {}).projection(_id: 1).sort(_id: 1).limit(1).first&.dig('_id')
      max_id = collection.find(query || {}).projection(_id: 1).sort(_id: -1).limit(1).first&.dig('_id')
      return [{ filter: query || {}, sort: { _id: 1 } }] if min_id.nil? || max_id.nil?

      # Create numeric-ish interpolation by sampling
      ids = collection.find(query || {}).projection(_id: 1).sort(_id: 1).limit(partitions - 1).to_a.map { |d| d['_id'] }
      boundaries = [min_id] + ids + [max_id]
      ranges = []
      boundaries.each_cons(2) do |a, b|
        ranges << build_range(a, b)
      end

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

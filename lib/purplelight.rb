# frozen_string_literal: true

require_relative "purplelight/version"
require_relative "purplelight/errors"
require_relative "purplelight/manifest"
require_relative "purplelight/snapshot"

module Purplelight
  # Convenience top-level API.
  # See Purplelight::Snapshot for options.
  def self.snapshot(**options)
    Snapshot.snapshot(**options)
  end
end

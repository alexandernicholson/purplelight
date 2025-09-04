# frozen_string_literal: true

require_relative 'purplelight/version'
require_relative 'purplelight/errors'
require_relative 'purplelight/manifest'
require_relative 'purplelight/snapshot'

# Purplelight is a lightweight toolkit for extracting and snapshotting data.
#
# The top-level module exposes a convenience API entrypoint via `.snapshot`.
# See `Purplelight::Snapshot` for supported options and formats.
module Purplelight
  # Convenience top-level API.
  # See Purplelight::Snapshot for options.
  def self.snapshot(...)
    Snapshot.snapshot(...)
  end
end

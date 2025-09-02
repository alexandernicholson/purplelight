# frozen_string_literal: true

module Purplelight
  # Lightweight, low-overhead timing and counters with a ticket API.
  class Telemetry
    def initialize(enabled: true)
      @enabled = enabled
      @counters = Hash.new(0)
      @timers = Hash.new(0.0)
      @mutex = Mutex.new
    end

    def enabled?
      @enabled
    end

    # Start a timer. Returns a ticket (Float) when enabled, or nil when disabled.
    def start(_key)
      return nil unless @enabled

      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end

    # Finish a timer using a ticket from start. No-ops if ticket is nil.
    def finish(key, ticket)
      return unless @enabled && ticket

      dt = Process.clock_gettime(Process::CLOCK_MONOTONIC) - ticket
      @timers[key] += dt
    end

    def add(key, count = 1)
      return unless @enabled

      @counters[key] += count
    end

    def merge!(other)
      return self unless @enabled

      other.counters.each { |k, v| @counters[k] += v }
      other.timers.each { |k, v| @timers[k] += v }
      self
    end

    attr_reader :counters, :timers

    # A disabled singleton for zero overhead checks if needed.
    NULL = new(enabled: false)
  end
end

# frozen_string_literal: true

module Purplelight
  # Sized queue that tracks bytes to apply backpressure.
  class ByteQueue
    def initialize(max_bytes: 128 * 1024 * 1024)
      @max_bytes = max_bytes
      @queue = []
      @bytes = 0
      @closed = false
      @mutex = Mutex.new
      @cv = ConditionVariable.new
    end

    def push(item, bytes:)
      @mutex.synchronize do
        raise 'queue closed' if @closed

        @cv.wait(@mutex) while (@bytes + bytes) > @max_bytes
        @queue << [item, bytes]
        @bytes += bytes
        @cv.broadcast
      end
    end

    def pop
      @mutex.synchronize do
        while @queue.empty?
          return nil if @closed

          @cv.wait(@mutex)
        end
        item, bytes = @queue.shift
        @bytes -= bytes
        @cv.broadcast
        item
      end
    end

    def close
      @mutex.synchronize do
        @closed = true
        @cv.broadcast
      end
    end

    def size_bytes
      @mutex.synchronize { @bytes }
    end
  end
end

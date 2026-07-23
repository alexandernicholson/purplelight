# frozen_string_literal: true

module Purplelight
  # Sized queue that tracks bytes to apply backpressure.
  class ByteQueue
    def initialize(max_bytes: 128 * 1024 * 1024)
      @max_bytes = max_bytes
      @queue = []
      @sizes = []
      @bytes = 0
      @closed = false
      @mutex = Mutex.new
      @cv = ConditionVariable.new
    end

    def push(item, bytes:)
      @mutex.synchronize do
        raise 'queue closed' if @closed

        while !@queue.empty? && (@bytes + bytes) > @max_bytes
          @cv.wait(@mutex)
          raise 'queue closed' if @closed
        end
        @queue << item
        @sizes << bytes
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
        item = @queue.shift
        bytes = @sizes.shift
        @bytes -= bytes
        @cv.broadcast
        item
      end
    end

    def close(discard: false)
      @mutex.synchronize do
        if discard
          @queue.clear
          @sizes.clear
          @bytes = 0
        end
        @closed = true
        @cv.broadcast
      end
    end

    def size_bytes
      @mutex.synchronize { @bytes }
    end
  end
end

# frozen_string_literal: true

module Purplelight
  class Error < StandardError; end

  class IncompatibleResumeError < Error; end
  class OutputExistsError < Error; end
  class WriterClosedError < Error; end
end



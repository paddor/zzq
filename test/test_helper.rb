# frozen_string_literal: true

require "minitest/autorun"
require "async"
require "console"
require "zzq"

Console.logger = Console::Logger.new(Console::Output::Null.new)
Warning[:experimental] = false

TEST_ASYNC_TIMEOUT = 2

module Kernel
  alias_method :_Async_base, :Async
  private :_Async_base


  def Async(&block)
    return _Async_base unless block
    return _Async_base(&block) unless Thread.current == Thread.main
    _Async_base { |task| task.with_timeout(TEST_ASYNC_TIMEOUT) { block.call(task) } }
  end
end

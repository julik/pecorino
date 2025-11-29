# frozen_string_literal: true

require "test_helper"
require_relative "leaky_bucket_concurrency_test_shared"

require "fileutils"
require "csv"
require "redis"
require "active_support/core_ext/numeric/time"

class PecorinoLeakyBucketConcurrencyRedisTest < ActiveSupport::TestCase
  include LeakyBucketConcurrencyTestShared

  def setup
    # Clean up any previous test artifacts
    @test_dir = File.join(Dir.tmpdir, "pecorino_test_#{Process.pid}")
    FileUtils.rm_rf(@test_dir)
    FileUtils.mkdir_p(@test_dir)

    # Use real Redis adapter (not test adapter) to test actual race conditions
    # The test adapter uses in-memory state which won't show the race condition
    # Ensure we have a Redis connection set up
    @key_prefix = "pecorino-test-#{Random.new(Minitest.seed).hex(4)}"
    @redis = Redis.new
    @adapter = Pecorino::Adapters::RedisAdapter.new(@redis, key_prefix: @key_prefix)

    # Set Pecorino to use Redis adapter
    Pecorino.adapter = @adapter
  end

  def teardown
    FileUtils.rm_rf(@test_dir) if File.exist?(@test_dir)
    # Clean up Redis keys
    if @redis
      keys = @redis.keys("#{@key_prefix}:*")
      @redis.del(keys) if keys.any?
    end
  rescue
    # Ignore errors during teardown
  end

  test "concurrent fillup_conditionally calls should not exceed bucket capacity" do
    # This test documents a bug in Pecorino's fillup_conditionally implementation.
    # Currently it fails due to a race condition where concurrent processes can all
    # read the same bucket state before any of them update it, allowing total tokens
    # to exceed capacity. Once Pecorino is fixed, this test should pass.

    key_prefix = @key_prefix
    run_concurrency_test(
      adapter: @adapter,
      setup_forked_process: -> {
        # Reconnect to Redis in forked process
        # Use the same key_prefix as the parent process
        redis = Redis.new
        adapter = Pecorino::Adapters::RedisAdapter.new(redis, key_prefix: key_prefix)
        adapter
      }
    )
  end

  private

  def cleanup_forked_process
    # Close Redis connection in forked process
    # Note: Redis connection is created in setup_forked_process, so we need to track it
    # For simplicity, we'll just ensure cleanup happens
  end
end

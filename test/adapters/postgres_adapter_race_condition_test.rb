# frozen_string_literal: true

require_relative "../test_helper"
require "timeout"

# Isolated test case for the race condition where the CTE `pre` in add_tokens_conditionally
# is evaluated when the bucket doesn't exist, but a concurrent transaction creates the
# bucket before the INSERT completes, causing ON CONFLICT to be triggered with an empty CTE.
#
# This test runs many iterations in a loop to increase the chance of hitting the race condition,
# since the CTE and INSERT are in the same SQL statement and timing is hard to control precisely.
class PostgresAdapterRaceConditionTest < ActiveSupport::TestCase
  def self.establish_connection(**options)
    ActiveRecord::Base.establish_connection(
      adapter: "postgresql",
      connect_timeout: 2,
      **options
    )
  end

  def create_adapter
    Pecorino::Adapters::PostgresAdapter.new(ActiveRecord::Base)
  end

  SEED_DB_NAME = -> { "pecorino_tests_%s" % Random.new(Minitest.seed).hex(4) }

  setup do
    create_postgres_database_if_none
    truncate_test_tables
  end

  teardown do
    truncate_test_tables
  end

  def create_postgres_database_if_none
    self.class.establish_connection(encoding: "unicode", database: SEED_DB_NAME.call)
    ActiveRecord::Base.connection_pool.with_connection { |connection| connection.execute("SELECT 1 FROM pecorino_leaky_buckets") }
  rescue ActiveRecord::NoDatabaseError, ActiveRecord::ConnectionNotEstablished
    create_postgres_database
    retry
  rescue ActiveRecord::StatementInvalid
    retained_adapter = create_adapter
    ActiveRecord::Schema.define(version: 1) do |via_definer|
      retained_adapter.create_tables(via_definer)
    end
    retry
  end

  def create_postgres_database
    ActiveRecord::Migration.verbose = false
    self.class.establish_connection(database: "postgres")
    ActiveRecord::Base.connection_pool.with_connection { |connection| connection.create_database(SEED_DB_NAME.call, charset: :unicode) }
    ActiveRecord::Base.connection.close
    self.class.establish_connection(encoding: "unicode", database: SEED_DB_NAME.call)
  end

  def truncate_test_tables
    begin
      ActiveRecord::Base.connection_pool.with_connection do |connection|
        connection.execute("TRUNCATE TABLE pecorino_leaky_buckets")
        connection.execute("TRUNCATE TABLE pecorino_blocks")
      end
    rescue ActiveRecord::ConnectionNotDefined
      # Ignore if connection is not defined (e.g., after thread cleanup)
    end
  end

  # Test the race condition where the CTE `pre` is evaluated when the bucket doesn't exist,
  # but a concurrent transaction creates the bucket before the INSERT completes, causing
  # ON CONFLICT to be triggered. The ON CONFLICT clause should handle this by computing
  # the level from the existing row instead of relying on the empty CTE.
  #
  # The race condition occurs when:
  # 1. Thread1 starts executing add_tokens_conditionally (CTE evaluates, bucket doesn't exist, CTE is empty)
  # 2. Thread2 creates the bucket concurrently
  # 3. Thread1's INSERT hits ON CONFLICT because the bucket now exists
  # 4. The ON CONFLICT clause tries to use the CTE which is empty, which would return NULL
  #
  # Since the CTE and INSERT are in the same SQL statement, we can't perfectly control the timing,
  # so we run many iterations in a loop to increase the chance of hitting the race condition.
  # The key test is that we NEVER get NULL values for level, regardless of timing.
  def test_conditional_fillup_never_produces_null_level_during_concurrent_inserts
    # Wrap the entire test in a timeout to prevent hanging
    Timeout.timeout(120) do
      capacity = 10.0
      leak_rate = 1.0
      iterations = 20 # Run many iterations to increase chance of hitting the race condition

      iterations.times do |iteration|
        key = "race_test_#{iteration}_#{Random.hex(4)}"
        results_mutex = Mutex.new
        results = []
        threads = []

        # Create multiple threads that will race to create/update the same bucket
        # This increases the chance of hitting the race condition where the CTE
        # is evaluated when the bucket doesn't exist, but then ON CONFLICT is triggered
        2.times do |thread_num|
          threads << Thread.new do
            begin
              # Use the existing connection setup - establish connection in this thread's context
              # but use a separate connection from the pool
              Thread.current[:adapter] = nil
              adapter_instance = nil
              
              # Create adapter in thread - it will use the connection pool
              ActiveRecord::Base.connection_pool.with_connection do
                adapter_instance = create_adapter
              end
              
              # Randomly decide what operation to perform to increase race condition chances
              if thread_num == 0 || rand < 0.5
                # Use conditional fillup - this is where the bug would manifest
                level, is_full, did_accept = adapter_instance.add_tokens_conditionally(
                  key: key,
                  capacity: capacity,
                  leak_rate: leak_rate,
                  n_tokens: 2.0 + rand * 3.0
                )
                results_mutex.synchronize do
                  results << { level: level, is_full: is_full, did_accept: did_accept, error: nil, thread: thread_num }
                end
              else
                # Use regular fillup to create the bucket
                level, is_full = adapter_instance.add_tokens(
                  key: key,
                  capacity: capacity,
                  leak_rate: leak_rate,
                  n_tokens: 2.0 + rand * 3.0
                )
                results_mutex.synchronize do
                  results << { level: level, is_full: is_full, error: nil, thread: thread_num }
                end
              end
            rescue => e
              results_mutex.synchronize do
                results << { error: e, thread: thread_num }
              end
            end
          end
        end

        # Wait for all threads to complete with timeout
        begin
          Timeout.timeout(10) do
            threads.each(&:join)
          end
        rescue Timeout::Error
          # Force kill threads if they're hanging
          threads.each { |t| t.kill if t.alive? }
          threads.each(&:join) # Clean up killed threads
          flunk "Iteration #{iteration}: Threads did not complete within timeout. Results: #{results.inspect}"
        end

        # Verify all results - the critical assertion is that level is NEVER NULL
        assert_equal 2, results.size, "Iteration #{iteration}: Should have 2 results, got #{results.size}"
        
        results.each do |result|
          if result[:error]
            flunk "Iteration #{iteration}, thread #{result[:thread]}: Expected no error, but got: #{result[:error]}\n#{result[:error].backtrace.join("\n")}"
          end

          # The critical assertion: level should NEVER be NULL
          # This is the bug we're testing for - if the CTE is empty and ON CONFLICT doesn't
          # handle it properly, level would be NULL
          assert_not_nil result[:level], "Iteration #{iteration}, thread #{result[:thread]}: Level should not be NULL - this tests the fix for the race condition"
          assert_kind_of Numeric, result[:level], "Iteration #{iteration}, thread #{result[:thread]}: Level should be a number, got: #{result[:level].class}"
          assert result[:level] >= 0, "Iteration #{iteration}, thread #{result[:thread]}: Level should be non-negative, got: #{result[:level]}"
          assert result[:level].finite?, "Iteration #{iteration}, thread #{result[:thread]}: Level should be finite, got: #{result[:level]}"
        end
      end
    end
  end

  Minitest.after_run do
    ActiveRecord::Base.connection.close
    establish_connection(database: "postgres")
    ActiveRecord::Base.connection_pool.with_connection { |connection| connection.drop_database(SEED_DB_NAME.call) }
  end
end


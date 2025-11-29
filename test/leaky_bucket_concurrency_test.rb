# frozen_string_literal: true

require "test_helper"

require "fileutils"
require "csv"

class PecorinoLeakyBucketConcurrencyTest < ActiveSupport::TestCase
  SEED_DB_NAME = -> { "pecorino_tests_%s" % Random.new(Minitest.seed).hex(4) }

  def self.establish_connection(**options)
    ActiveRecord::Base.establish_connection(
      adapter: "postgresql",
      connect_timeout: 2,
      **options
    )
  end

  def setup
    # Clean up any previous test artifacts
    @test_dir = File.join(Dir.tmpdir, "pecorino_test_#{Process.pid}")
    FileUtils.rm_rf(@test_dir)
    FileUtils.mkdir_p(@test_dir)

    # Use real PostgreSQL adapter (not test adapter) to test actual race conditions
    # The test adapter uses in-memory state which won't show the race condition
    # Ensure we have a PostgreSQL connection set up
    @db_name = SEED_DB_NAME.call
    create_postgres_database_if_none
  end

  def teardown
    FileUtils.rm_rf(@test_dir) if File.exist?(@test_dir)
    # Clean up Pecorino buckets
    ActiveRecord::Base.connection.execute("DELETE FROM pecorino_leaky_buckets WHERE key = 'concurrency_test_bucket'")
  end

  def copy_csv_on_failure(log_file)
    if File.exist?(log_file)
      timestamp = Time.now.strftime("%Y%m%d_%H%M%S")
      tmp_dir = File.join(__dir__, "..", "tmp")
      FileUtils.mkdir_p(tmp_dir)
      dest_file = File.join(tmp_dir, "concurrency_test_failure_#{timestamp}.csv")
      FileUtils.cp(log_file, dest_file)
      puts "\n=== CSV file copied to #{dest_file} for analysis ==="
    end
  end

  def create_postgres_database_if_none
    self.class.establish_connection(encoding: "unicode", database: @db_name)
    adapter = Pecorino::Adapters::PostgresAdapter.new(ActiveRecord::Base)
    ActiveRecord::Base.connection_pool.with_connection { |connection| connection.execute("SELECT 1 FROM pecorino_leaky_buckets") }
  rescue ActiveRecord::NoDatabaseError, ActiveRecord::ConnectionNotEstablished
    create_postgres_database
    retry
  rescue ActiveRecord::StatementInvalid
    adapter = Pecorino::Adapters::PostgresAdapter.new(ActiveRecord::Base)
    ActiveRecord::Schema.define(version: 1) do |via_definer|
      adapter.create_tables(via_definer)
    end
    retry
  end

  def create_postgres_database
    ActiveRecord::Migration.verbose = false
    self.class.establish_connection(database: "postgres")
    ActiveRecord::Base.connection_pool.with_connection { |connection| connection.create_database(@db_name, charset: :unicode) }
    ActiveRecord::Base.connection.close
    self.class.establish_connection(encoding: "unicode", database: @db_name)
  end

  test "concurrent fillup_conditionally calls should not exceed bucket capacity" do
    # This test documents a bug in Pecorino's fillup_conditionally implementation.
    # Currently it fails due to a race condition where concurrent processes can all
    # read the same bucket state before any of them update it, allowing total tokens
    # to exceed capacity. Once Pecorino is fixed, this test should pass.

    # Configuration for testing
    bucket_capacity = 500 # Small capacity for testing
    over_time = 3.seconds
    tokens_per_call = 5

    # Calculate expected max calls
    max_calls = bucket_capacity / tokens_per_call # 100 calls

    # We'll try to make more calls than allowed, concurrently
    # fillup_conditionally should reject calls that would exceed capacity
    num_processes = 5
    calls_per_process = 200 # Total: 1000 calls attempted, but only 100 should succeed

    # Create a log file to track calls across processes
    log_file = File.join(@test_dir, "calls.csv")
    # Write CSV header
    CSV.open(log_file, "w") do |csv|
      csv << ["monotonic_time", "process_id", "call_id", "status", "level_before", "level_after"]
    end

    # Capture database name for forked processes
    db_name = @db_name

    # Fork multiple processes that will make concurrent requests
    pids = []

    num_processes.times do |process_id|
      pid = fork do
        # Reconnect to database in forked process
        # Use the same database name as the parent process
        self.class.establish_connection(encoding: "unicode", database: db_name)

        # Each process creates its own bucket instance
        # They all share the same database key, which is where the race condition occurs
        bucket = Pecorino::LeakyBucket.new(
          key: "concurrency_test_bucket",
          capacity: bucket_capacity,
          over_time: over_time,
          adapter: Pecorino.adapter
        )

        # Make many calls concurrently
        calls_per_process.times do |call_id|
          # Query the bucket state BEFORE the fillup to see what level it was at
          state_before = bucket.state

          # Try to add tokens conditionally
          result = bucket.fillup_conditionally(tokens_per_call)

          # Log the result with state before and after using CSV format
          monotonic_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          row = [
            monotonic_time,
            process_id,
            call_id,
            result.accepted? ? "accepted" : "rejected",
            state_before.level,
            result.level
          ]
          File.open(log_file, "a") do |f|
            f.flock(File::LOCK_EX)
            f << CSV.generate_line(row)
            f.flock(File::LOCK_UN)
          end
        rescue
          # Log errors
          monotonic_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
          row = [
            monotonic_time,
            process_id,
            call_id,
            "error",
            nil,
            nil
          ]
          File.open(log_file, "a") do |f|
            f.flock(File::LOCK_EX)
            f << CSV.generate_line(row)
            f.flock(File::LOCK_UN)
          end
        end

        # Close database connection in forked process
        ActiveRecord::Base.connection.close
      end

      pids << pid
    end

    # Wait for all processes to complete
    pids.each do |pid|
      Process.wait(pid)
    end

    # Read and analyze the log file using CSV
    calls = []

    CSV.foreach(log_file, headers: true, header_converters: :symbol) do |row|
      monotonic_time = row[:monotonic_time].to_f
      process_id = row[:process_id].to_i
      call_id = row[:call_id].to_i
      status = row[:status]

      if status == "error"
        calls << {
          timestamp: monotonic_time,
          process_id: process_id,
          call_id: call_id,
          status: status,
          level_before: nil,
          level_after: nil,
          level_or_error: nil
        }
      else
        level_before = row[:level_before]&.to_f
        level_after = row[:level_after]&.to_f

        calls << {
          timestamp: monotonic_time,
          process_id: process_id,
          call_id: call_id,
          status: status,
          level_before: level_before,
          level_after: level_after,
          level_or_error: level_after
        }
      end
    end

    # Sort calls by timestamp to see the progression
    calls.sort_by! { |c| c[:timestamp] }

    accepted_calls = calls.select { |c| c[:status] == "accepted" }

    rejected_calls = calls.select { |c| c[:status] == "rejected" }

    # Epsilon for floating point comparisons (accounting for precision and timing)
    # We use a small tolerance to account for floating point arithmetic precision,
    # database timestamp precision, and timing variations in concurrent operations.
    # This prevents false positives from floating point rounding errors.
    epsilon = 0.01

    # Check that NO accepted call resulted in a level exceeding capacity
    # Use epsilon tolerance to account for floating point precision
    violations = accepted_calls.select { |c| c[:level_after] && c[:level_after] > bucket_capacity + epsilon }

    # Also check if level_before + tokens_per_call would exceed capacity
    # This catches cases where multiple transactions saw the same level_before
    # Use epsilon tolerance for floating point comparisons
    would_exceed_violations = accepted_calls.select do |c|
      if c[:level_before] && c[:level_after]
        would_exceed = (c[:level_before] + tokens_per_call) > bucket_capacity + epsilon
        # But the actual level_after is less than capacity (due to leakage or rejection)
        # Use epsilon tolerance here too
        would_exceed && c[:level_after] <= bucket_capacity + epsilon
      else
        false
      end
    end

    # Calculate actual tokens added by tracking level changes
    # Note: This is approximate since tokens leak over time
    # But we can verify the level never exceeds capacity
    total_tokens_added = accepted_calls.count * tokens_per_call

    # Check the final bucket level
    final_bucket = Pecorino::LeakyBucket.new(
      key: "concurrency_test_bucket",
      capacity: bucket_capacity,
      over_time: over_time,
      adapter: Pecorino.adapter
    )
    final_state = final_bucket.state

    puts "\n=== Test Results ==="
    puts "Total accepted calls: #{accepted_calls.count}"
    puts "Total rejected calls: #{rejected_calls.count}"
    puts "Total tokens added (estimated): #{total_tokens_added} tokens"
    puts "Bucket capacity: #{bucket_capacity} tokens"
    puts "Expected max calls: #{max_calls}"
    puts "Final bucket level: #{final_state.level.round(2)}"
    puts "Final bucket full?: #{final_state.full?}"

    if violations.any?
      puts "\n=== VIOLATIONS DETECTED ==="
      puts "Found #{violations.count} accepted calls that resulted in level > capacity:"
      violations.first(10).each do |v|
        puts "  Process #{v[:process_id]}, Call #{v[:call_id]}: level_before=#{v[:level_before]&.round(4)}, level_after=#{v[:level_after].round(4)} (capacity = #{bucket_capacity})"
      end
      puts "  ... (showing first 10)" if violations.count > 10
    end

    if would_exceed_violations.any?
      puts "\n=== CALLS WHERE level_before + tokens WOULD EXCEED CAPACITY ==="
      puts "Found #{would_exceed_violations.count} calls where queried level_before + tokens would exceed capacity"
      puts "Note: These may be valid if tokens leaked between query and fillup execution"
      would_exceed_violations.first(10).each do |v|
        expected_level = v[:level_before] + tokens_per_call
        actual_added = v[:level_after] - v[:level_before]
        leaked = tokens_per_call - actual_added
        puts "  Process #{v[:process_id]}, Call #{v[:call_id]}:"
        puts "    Queried level_before: #{v[:level_before].round(4)}"
        puts "    Expected if no leak: #{expected_level.round(4)} (would exceed!)"
        puts "    Actual level_after: #{v[:level_after].round(4)} (valid: < #{bucket_capacity})"
        puts "    Actual tokens added: #{actual_added.round(4)} (leaked: #{leaked.round(4)} during transaction)"
      end
      puts "  ... (showing first 10)" if would_exceed_violations.count > 10
    end

    # Show level progression for first few accepted calls
    if accepted_calls.any?
      puts "\n=== Level Progression (first 20 accepted calls) ==="
      accepted_calls.first(20).each_with_index do |c, idx|
        puts "  #{idx + 1}. Level before: #{c[:level_before] ? c[:level_before].round(4) : "N/A"}, after: #{c[:level_after] ? c[:level_after].round(4) : "N/A"}"
      end

      # Show calls near capacity
      near_capacity = accepted_calls.select { |c| c[:level_after] && c[:level_after] > bucket_capacity * 0.9 }
      if near_capacity.any?
        puts "\n=== Calls Near Capacity (>90%) ==="
        near_capacity.first(20).each do |c|
          puts "  Process #{c[:process_id]}, Call #{c[:call_id]}: level_before=#{c[:level_before]&.round(4)}, level_after=#{c[:level_after].round(4)} (#{(c[:level_after] / bucket_capacity * 100).round(2)}% of capacity)"
        end
      end

      # Show the level before vs after for last 20 accepted calls
      puts "\n=== Level Before vs After (last 20 accepted calls) ==="
      accepted_calls.last(20).each do |c|
        if c[:level_before] && c[:level_after]
          would_exceed = (c[:level_before] + tokens_per_call) > bucket_capacity
          margin = bucket_capacity - c[:level_before]
          actual_added = c[:level_after] - c[:level_before]
          puts "  Level before: #{c[:level_before].round(4)}, after: #{c[:level_after].round(4)}, margin: #{margin.round(4)}, would_exceed: #{would_exceed}, actual_added: #{actual_added.round(4)}"
        end
      end

      # Find calls that were accepted when there was very little margin
      # These are suspicious - multiple concurrent transactions might have all seen
      # the same low margin and all accepted
      # Use epsilon tolerance for floating point comparison
      low_margin_calls = accepted_calls.select do |c|
        if c[:level_before]
          margin = bucket_capacity - c[:level_before]
          margin < tokens_per_call * 2 + epsilon # Less than 2x tokens_per_call margin (with epsilon tolerance)
        else
          false
        end
      end

      if low_margin_calls.any?
        puts "\n=== Low Margin Calls (potential race condition) ==="
        puts "Found #{low_margin_calls.count} calls accepted with margin < #{tokens_per_call * 2}"
        low_margin_calls.each do |c|
          margin = bucket_capacity - c[:level_before]
          time_offset = c[:timestamp] - calls.first[:timestamp]
          actual_added = c[:level_after] - c[:level_before]
          puts "  Time: #{time_offset.round(4)}s, Process #{c[:process_id]}, Call #{c[:call_id]}: level_before=#{c[:level_before].round(4)}, margin=#{margin.round(4)}, level_after=#{c[:level_after].round(4)}, actual_added=#{actual_added.round(4)}"
        end

        # Check if any low-margin calls happened at nearly the same time
        if low_margin_calls.count > 1
          timestamps = low_margin_calls.map { |c| c[:timestamp] }.sort
          time_diffs = timestamps.each_cons(2).map { |a, b| b - a }
          concurrent_pairs = time_diffs.count { |diff| diff < 0.01 } # Within 10ms
          puts "  Concurrent low-margin calls (within 10ms): #{concurrent_pairs} pairs"

          # Group by time windows to find concurrent calls
          time_windows = low_margin_calls.group_by { |c| (c[:timestamp] * 100).floor / 100.0 } # Group by 10ms windows
          concurrent_groups = time_windows.select { |_time, calls| calls.count > 1 }
          if concurrent_groups.any?
            puts "  Concurrent groups:"
            concurrent_groups.each do |time_window, group_calls|
              puts "    Time window #{time_window.round(4)}s: #{group_calls.count} calls"
              group_calls.each do |c|
                puts "      Process #{c[:process_id]}, Call #{c[:call_id]}: level_before=#{c[:level_before].round(4)}, level_after=#{c[:level_after].round(4)}"
              end
            end
          end
        end
      end
    end

    # KEY ASSERTION: fillup_conditionally should NEVER result in a level exceeding capacity
    # This is the most important check - the bucket level after ANY accepted fillup
    # must never exceed the capacity, regardless of concurrency.
    begin
      assert_empty violations,
        "fillup_conditionally should NEVER allow the bucket level to exceed capacity. " \
        "Found #{violations.count} violations where level_after > #{bucket_capacity} + #{epsilon} (epsilon tolerance). " \
        "This indicates a race condition in Pecorino's fillup_conditionally implementation."
    rescue Minitest::Assertion
      # On failure, copy CSV to tmp/ directory for analysis
      if File.exist?(log_file)
        timestamp = Time.now.strftime("%Y%m%d_%H%M%S")
        tmp_dir = File.join(__dir__, "..", "tmp")
        FileUtils.mkdir_p(tmp_dir)
        dest_file = File.join(tmp_dir, "concurrency_test_failure_#{timestamp}.csv")
        FileUtils.cp(log_file, dest_file)
        puts "\n=== CSV file copied to #{dest_file} for analysis ==="
      end
      raise
    end

    # Also check: if level_before + tokens_per_call would exceed capacity, the fillup should be rejected
    # But if it was accepted, the actual level_after should be <= capacity (due to leakage or proper rejection)
    # Use epsilon tolerance for floating point comparisons
    suspicious_calls = accepted_calls.select do |c|
      if c[:level_before] && c[:level_after]
        would_exceed = (c[:level_before] + tokens_per_call) > bucket_capacity + epsilon
        # If it would exceed but was accepted, check if level_after is still valid
        # Use epsilon tolerance to account for floating point precision
        would_exceed && c[:level_after] > bucket_capacity + epsilon
      else
        false
      end
    end

    begin
      assert_empty suspicious_calls,
        "Found #{suspicious_calls.count} calls that were accepted when level_before + tokens would exceed capacity, " \
        "AND level_after > capacity + #{epsilon} (epsilon tolerance). This indicates a race condition."
    rescue Minitest::Assertion
      # On failure, copy CSV to tmp/ directory for analysis
      if File.exist?(log_file)
        timestamp = Time.now.strftime("%Y%m%d_%H%M%S")
        tmp_dir = File.join(__dir__, "..", "tmp")
        FileUtils.mkdir_p(tmp_dir)
        dest_file = File.join(tmp_dir, "concurrency_test_failure_#{timestamp}.csv")
        FileUtils.cp(log_file, dest_file)
        puts "\n=== CSV file copied to #{dest_file} for analysis ==="
      end
      raise
    end

    # Note: We don't assert on total_tokens_added because in a concurrent test with a leaky bucket,
    # tokens leak continuously, allowing many more tokens to be added than capacity over time.
    # The important check is that level never exceeds capacity (checked above), not the total tokens added.
    # The final bucket level check below also verifies the bucket state is correct.

    # Additional assertion: the final bucket level should not exceed capacity
    # (accounting for some tokens that may have leaked during the test)
    # Use epsilon tolerance to account for floating point precision
    begin
      assert_operator final_state.level, :<=, bucket_capacity + epsilon,
        "Final bucket level should not exceed capacity. " \
        "Expected at most #{bucket_capacity}, but got #{final_state.level.round(2)}. " \
        "Using epsilon tolerance of #{epsilon} for floating point precision."
    rescue Minitest::Assertion
      copy_csv_on_failure(log_file)
      raise
    end

    # Also verify that calls happened concurrently (within a reasonable time window)
    if accepted_calls.any?
      timestamps = accepted_calls.map { |c| c[:timestamp] }
      time_span = timestamps.max - timestamps.min
      puts "Time span of accepted calls: #{time_span.round(2)} seconds"

      # Calls should complete within a reasonable time (not take minutes)
      assert_operator time_span, :<, 30.0, "Calls should happen concurrently, not sequentially"
    end
  end
end

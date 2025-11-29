# frozen_string_literal: true

require "fileutils"
require "csv"
require "active_support/core_ext/numeric/time"

module LeakyBucketConcurrencyTestShared
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

  def run_concurrency_test(adapter:, setup_forked_process:)
    # Configuration for testing
    bucket_capacity = 500 # Small capacity for testing
    over_time = 3.seconds
    tokens_per_call = 5

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

    # Fork multiple processes that will make concurrent requests
    pids = []

    num_processes.times do |process_id|
      pid = fork do
        # Set up adapter in forked process (provided by the test)
        forked_adapter = setup_forked_process.call

        # Each process creates its own bucket instance
        # They all share the same database/key, which is where the race condition occurs
        bucket = Pecorino::LeakyBucket.new(
          key: "concurrency_test_bucket",
          capacity: bucket_capacity,
          over_time: over_time,
          adapter: forked_adapter
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

        # Cleanup in forked process (if needed)
        cleanup_forked_process if respond_to?(:cleanup_forked_process, true)
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

    # Epsilon for floating point comparisons (accounting for precision and timing)
    # We use a small tolerance to account for floating point arithmetic precision,
    # database timestamp precision, and timing variations in concurrent operations.
    # This prevents false positives from floating point rounding errors.
    epsilon = 0.01

    # Check that NO accepted call resulted in a level exceeding capacity
    # Use epsilon tolerance to account for floating point precision
    violations = accepted_calls.select { |c| c[:level_after] && c[:level_after] > bucket_capacity + epsilon }

    # Check the final bucket level
    final_bucket = Pecorino::LeakyBucket.new(
      key: "concurrency_test_bucket",
      capacity: bucket_capacity,
      over_time: over_time,
      adapter: adapter
    )
    final_state = final_bucket.state

    # KEY ASSERTION: fillup_conditionally should NEVER result in a level exceeding capacity
    # This is the most important check - the bucket level after ANY accepted fillup
    # must never exceed the capacity, regardless of concurrency.
    begin
      assert_empty violations,
        "fillup_conditionally should NEVER allow the bucket level to exceed capacity. " \
        "Found #{violations.count} violations where level_after > #{bucket_capacity} + #{epsilon} (epsilon tolerance). " \
        "This indicates a race condition in Pecorino's fillup_conditionally implementation."
    rescue Minitest::Assertion
      copy_csv_on_failure(log_file)
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
      copy_csv_on_failure(log_file)
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

      # Calls should complete within a reasonable time (not take minutes)
      assert_operator time_span, :<, 30.0, "Calls should happen concurrently, not sequentially"
    end
  end
end

# frozen_string_literal: true

require "test_helper"
require_relative "leaky_bucket_concurrency_test_shared"

require "fileutils"
require "csv"

class PecorinoLeakyBucketConcurrencyPostgresTest < ActiveSupport::TestCase
  include LeakyBucketConcurrencyTestShared

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

    db_name = @db_name
    run_concurrency_test(
      adapter: Pecorino.adapter,
      setup_forked_process: -> {
        # Reconnect to database in forked process
        # Use the same database name as the parent process
        self.class.establish_connection(encoding: "unicode", database: db_name)
        Pecorino.adapter
      }
    )
  end

  private

  def cleanup_forked_process
    # Close database connection in forked process
    ActiveRecord::Base.connection.close
  end
end

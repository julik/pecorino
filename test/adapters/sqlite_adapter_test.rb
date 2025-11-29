# frozen_string_literal: true

require_relative "../test_helper"
require_relative "adapter_test_methods"
require "fileutils"

class SqliteAdapterTest < ActiveSupport::TestCase
  include AdapterTestMethods

  setup { create_sqlite_db }
  teardown { drop_sqlite_db }

  def create_sqlite_db
    ActiveRecord::Migration.verbose = false
    ActiveRecord::Base.establish_connection(adapter: "sqlite3", database: db_filename)

    # The adapter has to be in a variable as the schema definition is scoped to the migrator, not self
    retained_adapter = create_adapter # the schema define block is run via instance_exec so it does not retain scope
    ActiveRecord::Schema.define(version: 1) do |via_definer|
      retained_adapter.create_tables(via_definer)
    end
  end

  def drop_sqlite_db
    # Close all connections before deleting the database file
    if ActiveRecord::Base.connected?
      ActiveRecord::Base.connection_pool.disconnect!
    end
    ActiveRecord::Base.remove_connection if ActiveRecord::Base.connection_handler

    # Delete database files if they exist
    FileUtils.rm_rf(db_filename) if File.exist?(db_filename)
    FileUtils.rm_rf(db_filename + "-wal") if File.exist?(db_filename + "-wal")
    FileUtils.rm_rf(db_filename + "-shm") if File.exist?(db_filename + "-shm")
  rescue
    # Ignore errors during teardown - file might already be deleted or locked
  end

  def db_filename
    @db_filename ||= begin
      tmp_dir = File.expand_path(File.join(__dir__, "..", "..", "tmp"))
      FileUtils.mkdir_p(tmp_dir) unless File.directory?(tmp_dir)
      File.join(tmp_dir, "pecorino_tests_%s_%s.sqlite3" % [Random.new(Minitest.seed).hex(4), object_id])
    end
  end

  def create_adapter
    Pecorino::Adapters::SqliteAdapter.new(ActiveRecord::Base)
  end

  def test_create_tables
    ActiveRecord::Base.transaction do
      ActiveRecord::Base.connection_pool.with_connection do |connection|
        connection.execute("DROP TABLE pecorino_leaky_buckets")
        connection.execute("DROP TABLE pecorino_blocks")
      end
      # The adapter has to be in a variable as the schema definition is scoped to the migrator, not self
      retained_adapter = create_adapter # the schema define block is run via instance_exec so it does not retain scope
      ActiveRecord::Schema.define(version: 1) do |via_definer|
        retained_adapter.create_tables(via_definer)
      end
    end
    assert true
  end
end

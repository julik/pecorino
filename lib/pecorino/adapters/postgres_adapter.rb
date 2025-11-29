# frozen_string_literal: true

class Pecorino::Adapters::PostgresAdapter
  include Pecorino::Adapters::ConnectionShim

  def initialize(model_class)
    @model_class = model_class
  end

  def state(key:, capacity:, leak_rate:)
    query_params = {
      key: key.to_s,
      capacity: capacity.to_f,
      leak_rate: leak_rate.to_f
    }
    # The `level` of the bucket is what got stored at `last_touched_at` time, and we can
    # extrapolate from it to see how many tokens have leaked out since `last_touched_at` -
    # we don't need to UPDATE the value in the bucket here
    sql = sanitize_sql_array([<<~SQL, query_params])
      SELECT
        GREATEST(
          0.0, LEAST(
            :capacity,
            t.level - (EXTRACT(EPOCH FROM (clock_timestamp() - t.last_touched_at)) * :leak_rate)
          )
        )
      FROM 
        pecorino_leaky_buckets AS t
      WHERE
        key = :key
    SQL

    # If the return value of the query is a NULL it means no such bucket exists,
    # so we assume the bucket is empty
    current_level = with_connection { |c| c.select_value(sql) } || 0.0
    [current_level, capacity - current_level.abs < 0.01]
  end

  def add_tokens(key:, capacity:, leak_rate:, n_tokens:)
    # Take double the time it takes the bucket to empty under normal circumstances
    # until the bucket may be deleted.
    may_be_deleted_after_seconds = (capacity.to_f / leak_rate.to_f) * 2.0

    # Create the leaky bucket if it does not exist, and update
    # to the new level, taking the leak rate into account - if the bucket exists.
    query_params = {
      key: key.to_s,
      capacity: capacity.to_f,
      delete_after_s: may_be_deleted_after_seconds,
      leak_rate: leak_rate.to_f,
      fillup: n_tokens.to_f
    }

    sql = sanitize_sql_array([<<~SQL, query_params])
      INSERT INTO pecorino_leaky_buckets AS t
        (key, last_touched_at, may_be_deleted_after, level)
      VALUES
        (
          :key,
          clock_timestamp(),
          clock_timestamp() + ':delete_after_s second'::interval,
          GREATEST(0.0,
            LEAST(
              :capacity,
              :fillup
            )
          )
        )
      ON CONFLICT (key) DO UPDATE SET
        last_touched_at = EXCLUDED.last_touched_at,
        may_be_deleted_after = EXCLUDED.may_be_deleted_after,
        level = GREATEST(0.0,
          LEAST(
              :capacity,
              t.level + :fillup - (EXTRACT(EPOCH FROM (EXCLUDED.last_touched_at - t.last_touched_at)) * :leak_rate)
          )
        )
      RETURNING
        level,
        -- Compare level to the capacity inside the DB so that we won't have rounding issues
        level >= :capacity AS at_capacity
    SQL

    # Note the use of .uncached here. The AR query cache will actually see our
    # query as a repeat (since we use "select_one" for the RETURNING bit) and will not call into Postgres
    # correctly, thus the clock_timestamp() value would be frozen between calls. We don't want that here.
    # See https://stackoverflow.com/questions/73184531/why-would-postgres-clock-timestamp-freeze-inside-a-rails-unit-test
    upserted = with_connection { |c| c.select_one(sql) }
    capped_level_after_fillup, at_capacity = upserted.fetch("level"), upserted.fetch("at_capacity")
    [capped_level_after_fillup, at_capacity]
  end

  def add_tokens_conditionally(key:, capacity:, leak_rate:, n_tokens:)
    # Take double the time it takes the bucket to empty under normal circumstances
    # until the bucket may be deleted.
    may_be_deleted_after_seconds = (capacity.to_f / leak_rate.to_f) * 2.0

    # Create the leaky bucket if it does not exist, and update
    # to the new level, taking the leak rate into account - if the bucket exists.
    query_params = {
      key: key.to_s,
      capacity: capacity.to_f,
      delete_after_s: may_be_deleted_after_seconds,
      leak_rate: leak_rate.to_f,
      fillup: n_tokens.to_f
    }

    # Use explicit transaction with separate INSERT and UPDATE statements
    # This ensures proper locking and is more portable across RDBMSes
    # READ COMMITTED (PostgreSQL's default) combined with SELECT FOR UPDATE provides row-level locking
    # that prevents concurrent modifications, which is sufficient for our use case.
    # SELECT FOR UPDATE locks the row until the transaction commits, preventing race conditions.
    # This is more performant than SERIALIZABLE or REPEATABLE READ while still preventing race conditions.
    with_connection do |connection|
      connection.uncached do
        connection.transaction(isolation: :read_committed) do
          # Step 1: Ensure the row exists (this serializes concurrent inserts)
          insert_sql = @model_class.sanitize_sql_array([<<~SQL, query_params])
            INSERT INTO pecorino_leaky_buckets
              (key, last_touched_at, may_be_deleted_after, level)
            VALUES
              (
                :key,
                clock_timestamp(),
                clock_timestamp() + ':delete_after_s second'::interval,
                0.0
              )
            ON CONFLICT (key) DO NOTHING
          SQL
          connection.execute(insert_sql)

          # Step 2: Lock the row and read current state
          # The FOR UPDATE lock will be held until the transaction commits
          lock_sql = @model_class.sanitize_sql_array([<<~SQL, query_params])
            SELECT
              level,
              last_touched_at,
              GREATEST(0.0,
                level - (EXTRACT(EPOCH FROM (clock_timestamp() - last_touched_at)) * :leak_rate)
              ) AS level_after_leak
            FROM pecorino_leaky_buckets
            WHERE key = :key
            FOR UPDATE
          SQL
          current = connection.select_one(lock_sql)

          # If row doesn't exist (shouldn't happen after INSERT, but handle it)
          if current.nil?
            level_before = 0.0
            level_post_with_uncapped_fillup = query_params[:fillup]
          else
            level_before = current.fetch("level_after_leak").to_f
            level_post_with_uncapped_fillup = level_before + query_params[:fillup]
          end

          # Step 3: Calculate new level conditionally
          # Ensure level_before is non-negative (should be handled by GREATEST, but be safe)
          level_before = [level_before, 0.0].max

          new_level = if level_post_with_uncapped_fillup <= query_params[:capacity]
            [level_post_with_uncapped_fillup, 0.0].max
          else
            level_before
          end

          # Step 4: Update the row (lock is still held from Step 2)
          update_sql = @model_class.sanitize_sql_array([<<~SQL, query_params.merge(new_level: new_level)])
            UPDATE pecorino_leaky_buckets
            SET
              last_touched_at = clock_timestamp(),
              may_be_deleted_after = clock_timestamp() + ':delete_after_s second'::interval,
              level = GREATEST(0.0, :new_level)
            WHERE key = :key
            RETURNING level
          SQL

          updated = connection.select_one(update_sql)
          level_after = updated.fetch("level").to_f
          [level_after, level_after >= capacity, level_after != level_before]
        end
      end
    end
  end

  def set_block(key:, block_for:)
    raise ArgumentError, "block_for must be positive" unless block_for > 0
    query_params = {key: key.to_s, block_for: block_for.to_f}
    block_set_query = sanitize_sql_array([<<~SQL, query_params])
      INSERT INTO pecorino_blocks AS t
        (key, blocked_until)
      VALUES
        (:key, clock_timestamp() + ':block_for seconds'::interval)
      ON CONFLICT (key) DO UPDATE SET
        blocked_until = GREATEST(EXCLUDED.blocked_until, t.blocked_until)
      RETURNING blocked_until
    SQL
    with_connection { |c| c.select_value(block_set_query) }
  end

  def blocked_until(key:)
    block_check_query = sanitize_sql_array([<<~SQL, key])
      SELECT blocked_until FROM pecorino_blocks WHERE key = ? AND blocked_until >= clock_timestamp() LIMIT 1
    SQL
    with_connection { |c| c.select_value(block_check_query) }
  end

  def prune
    with_connection do |c|
      c.execute("DELETE FROM pecorino_blocks WHERE blocked_until < NOW()")
      c.execute("DELETE FROM pecorino_leaky_buckets WHERE may_be_deleted_after < NOW()")
    end
  end

  def create_tables(active_record_schema)
    active_record_schema.create_table :pecorino_leaky_buckets, id: :uuid do |t|
      t.string :key, null: false
      t.float :level, null: false
      t.datetime :last_touched_at, null: false
      t.datetime :may_be_deleted_after, null: false
    end
    active_record_schema.add_index :pecorino_leaky_buckets, [:key], unique: true
    active_record_schema.add_index :pecorino_leaky_buckets, [:may_be_deleted_after]

    active_record_schema.create_table :pecorino_blocks, id: :uuid do |t|
      t.string :key, null: false
      t.datetime :blocked_until, null: false
    end
    active_record_schema.add_index :pecorino_blocks, [:key], unique: true
    active_record_schema.add_index :pecorino_blocks, [:blocked_until]
  end
end

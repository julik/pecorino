# frozen_string_literal: true

class Pecorino::Adapters::SqliteAdapter
  include Pecorino::Adapters::ConnectionShim

  def initialize(model_class)
    @model_class = model_class
  end

  def state(key:, capacity:, leak_rate:)
    # With a server database, it is really important to use the clock of the database itself so
    # that concurrent requests will see consistent bucket level calculations. Since SQLite is
    # actually in-process, there is no point using DB functions - and besides, SQLite reduces
    # the time precision to the nearest millisecond - and the calculations with timestamps are
    # obtuse. Therefore we can use the current time inside the Ruby VM - it doesn't matter all that
    # much but saves us on writing some gnarly SQL to have SQLite produce consistent precise timestamps.
    query_params = {
      key: key.to_s,
      capacity: capacity.to_f,
      leak_rate: leak_rate.to_f,
      now_s: Time.now.to_f
    }
    # The `level` of the bucket is what got stored at `last_touched_at` time, and we can
    # extrapolate from it to see how many tokens have leaked out since `last_touched_at` -
    # we don't need to UPDATE the value in the bucket here
    sql = sanitize_sql_array([<<~SQL, query_params])
      SELECT
        MAX(
          0.0, MIN(
            :capacity,
            t.level - ((:now_s - t.last_touched_at) * :leak_rate)
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
      now_s: Time.now.to_f, # See above as to why we are using a time value passed in
      fillup: n_tokens.to_f
    }

    sql = sanitize_sql_array([<<~SQL, query_params])
      INSERT INTO pecorino_leaky_buckets AS t
        (key, last_touched_at, may_be_deleted_after, level)
      VALUES
        (
          :key,
          :now_s, -- Precision loss must be avoided here as it is used for calculations
          DATETIME('now', '+:delete_after_s seconds'), -- Precision loss is acceptable here
          MAX(0.0,
            MIN(
              :capacity,
              :fillup
            )
          )
        )
      ON CONFLICT (key) DO UPDATE SET
        last_touched_at = EXCLUDED.last_touched_at,
        may_be_deleted_after = EXCLUDED.may_be_deleted_after,
        level = MAX(0.0,
          MIN(
              :capacity,
              t.level + :fillup - ((:now_s - t.last_touched_at) * :leak_rate)
          )
        )
      RETURNING
        level,
        -- Compare level to the capacity inside the DB so that we won't have rounding issues
        level >= :capacity AS did_overflow
    SQL

    upserted = with_connection { |c| c.select_one(sql) }
    capped_level_after_fillup, one_if_did_overflow = upserted.fetch("level"), upserted.fetch("did_overflow")
    [capped_level_after_fillup, one_if_did_overflow == 1]
  end

  def add_tokens_conditionally(key:, capacity:, leak_rate:, n_tokens:)
    # Take double the time it takes the bucket to empty under normal circumstances
    # until the bucket may be deleted.
    may_be_deleted_after_seconds = (capacity.to_f / leak_rate.to_f) * 2.0

    # Use explicit transaction with separate INSERT and UPDATE statements
    # This ensures proper locking and is more portable across RDBMSes
    # SQLite uses file-level locking and SERIALIZABLE isolation by default,
    # so the transaction itself provides the necessary isolation to prevent race conditions.
    with_connection do |connection|
      connection.transaction do
        # Calculate now_s once at the start of the transaction
        now_s = Time.now.to_f

        query_params = {
          key: key.to_s,
          capacity: capacity.to_f,
          delete_after_s: may_be_deleted_after_seconds,
          leak_rate: leak_rate.to_f,
          now_s: now_s,
          fillup: n_tokens.to_f
        }

        # Step 1: Ensure the row exists (this serializes concurrent inserts)
        insert_sql = @model_class.sanitize_sql_array([<<~SQL, query_params])
          INSERT INTO pecorino_leaky_buckets
            (key, last_touched_at, may_be_deleted_after, level)
          VALUES
            (
              :key,
              :now_s,
              DATETIME('now', '+:delete_after_s seconds'),
              0.0
            )
          ON CONFLICT (key) DO NOTHING
        SQL
        connection.execute(insert_sql)

        # Step 2: Read current state
        # SQLite uses file-level locking and SERIALIZABLE isolation by default,
        # so the transaction itself provides the necessary isolation without FOR UPDATE
        lock_sql = @model_class.sanitize_sql_array([<<~SQL, query_params])
          SELECT
            level,
            last_touched_at,
            MAX(0.0,
              level - ((:now_s - last_touched_at) * :leak_rate)
            ) AS level_after_leak
          FROM pecorino_leaky_buckets
          WHERE key = :key
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
        # Ensure level_before is non-negative (should be handled by MAX, but be safe)
        level_before = [level_before, 0.0].max

        new_level = if level_post_with_uncapped_fillup <= query_params[:capacity]
          [level_post_with_uncapped_fillup, 0.0].max
        else
          level_before
        end

        # Step 4: Update the row (transaction isolation ensures no concurrent modifications)
        update_sql = @model_class.sanitize_sql_array([<<~SQL, query_params.merge(new_level: new_level)])
          UPDATE pecorino_leaky_buckets
          SET
            last_touched_at = :now_s,
            may_be_deleted_after = DATETIME('now', '+:delete_after_s seconds'),
            level = MAX(0.0, :new_level)
          WHERE key = :key
        SQL

        connection.execute(update_sql)

        # Step 5: Read the final level to return
        final_sql = @model_class.sanitize_sql_array([<<~SQL, query_params])
          SELECT level
          FROM pecorino_leaky_buckets
          WHERE key = :key
        SQL
        final = connection.select_one(final_sql)
        level_after = final.fetch("level").to_f
        [level_after, level_after >= capacity, level_after != level_before]
      end
    end
  end

  def set_block(key:, block_for:)
    raise ArgumentError, "block_for must be positive" unless block_for > 0
    query_params = {key: key.to_s, block_for: block_for.to_f, now_s: Time.now.to_f}
    block_set_query = sanitize_sql_array([<<~SQL, query_params])
      INSERT INTO pecorino_blocks AS t
        (key, blocked_until)
      VALUES
        (:key, :now_s + :block_for)
      ON CONFLICT (key) DO UPDATE SET
        blocked_until = MAX(EXCLUDED.blocked_until, t.blocked_until)
      RETURNING blocked_until;
    SQL
    blocked_until_s = with_connection { |c| c.select_value(block_set_query) }
    Time.at(blocked_until_s)
  end

  def blocked_until(key:)
    now_s = Time.now.to_f
    block_check_query = sanitize_sql_array([<<~SQL, {now_s: now_s, key: key}])
      SELECT
        blocked_until
      FROM
        pecorino_blocks
      WHERE
        key = :key AND blocked_until >= :now_s LIMIT 1
    SQL
    blocked_until_s = with_connection { |c| c.select_value(block_check_query) }
    blocked_until_s && Time.at(blocked_until_s)
  end

  def prune
    now_s = Time.now.to_f
    with_connection do |c|
      c.execute("DELETE FROM pecorino_blocks WHERE blocked_until < ?", now_s)
      c.execute("DELETE FROM pecorino_leaky_buckets WHERE may_be_deleted_after < ?", now_s)
    end
  end

  def create_tables(active_record_schema)
    active_record_schema.create_table :pecorino_leaky_buckets do |t|
      t.string :key, null: false
      t.float :level, null: false
      t.datetime :last_touched_at, null: false
      t.datetime :may_be_deleted_after, null: false
    end
    active_record_schema.add_index :pecorino_leaky_buckets, [:key], unique: true
    active_record_schema.add_index :pecorino_leaky_buckets, [:may_be_deleted_after]

    active_record_schema.create_table :pecorino_blocks do |t|
      t.string :key, null: false
      t.datetime :blocked_until, null: false
    end
    active_record_schema.add_index :pecorino_blocks, [:key], unique: true
    active_record_schema.add_index :pecorino_blocks, [:blocked_until]
  end
end

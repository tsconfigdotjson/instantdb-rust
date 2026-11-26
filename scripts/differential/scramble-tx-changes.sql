-- Adversarial physical order for the tx change log (issue #129).
--
-- `rust_tx_changes` is replayed in order by the webhook payload builder
-- (webhooks.rs load_tx_changes, walked in reverse to rebuild `before`) and by
-- sync-table pushes (sync_table.rs changes_between). Postgres does not keep
-- rows in insertion order: once the table has had deletes and vacuums, the
-- free-space map can put a tx's later row on an earlier page. The readers
-- must order by `seq`, never by ctid or by scan order.
--
-- This trigger makes the bad case happen on every tx instead of only on a
-- busy table: after each insert into the log it moves every `removed` row of
-- the inserting tx physically past the tx's last `added` row (deleted and
-- re-inserted with the same seq, so both the heap slot and the index entry
-- are new), so ctid, heap-scan and index-scan order all show an update's new
-- value before its old one. A reader that relies on physical
-- order then rebuilds a webhook `before` without the attr, or hands a sync
-- client `added` then `removed`.
--
-- Test-only, and this file only defines the function. CI's differential legs
-- attach it for every app once the rust server has created its tables:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
--     -f scripts/differential/scramble-tx-changes.sql \
--     -c "CREATE TRIGGER rust_test_scramble_tx_changes AFTER INSERT ON rust_tx_changes
--         REFERENCING NEW TABLE AS scrambled_new_rows
--         FOR EACH STATEMENT EXECUTE FUNCTION rust_test_scramble_tx_changes()"
-- The instant-server unit tests attach it to their own app only, by passing
-- the app id as the trigger's argument (no argument: every app). The
-- transition table must be named scrambled_new_rows.

CREATE OR REPLACE FUNCTION rust_test_scramble_tx_changes() RETURNS trigger AS $fn$
DECLARE
  passes int := 0;
BEGIN
  -- the re-insert below fires this trigger again; that nested call only
  -- moves rows that still landed too early, and stops at once otherwise
  IF pg_trigger_depth() > 8 THEN
    RETURN NULL;
  END IF;
  LOOP
    PERFORM 1
      FROM rust_tx_changes r
      JOIN (SELECT c.app_id, c.tx_id, max(c.ctid) AS last_added
              FROM rust_tx_changes c
              JOIN (SELECT DISTINCT app_id, tx_id FROM scrambled_new_rows) t
                ON t.app_id = c.app_id AND t.tx_id = c.tx_id
             WHERE c.action = 'added'
               AND (TG_NARGS = 0 OR c.app_id = TG_ARGV[0]::uuid)
             GROUP BY c.app_id, c.tx_id) m
        ON r.app_id = m.app_id AND r.tx_id = m.tx_id
     WHERE r.action = 'removed' AND r.ctid < m.last_added
     LIMIT 1;
    EXIT WHEN NOT FOUND;
    -- delete + re-insert (same seq) rather than UPDATE: a HOT update keeps
    -- the index entry on the old slot, so an index scan would still return
    -- insertion order. A fresh tuple gets a new heap slot and index entry.
    WITH m AS (
      SELECT c.app_id, c.tx_id, max(c.ctid) AS last_added
        FROM rust_tx_changes c
        JOIN (SELECT DISTINCT app_id, tx_id FROM scrambled_new_rows) t
          ON t.app_id = c.app_id AND t.tx_id = c.tx_id
       WHERE c.action = 'added'
         AND (TG_NARGS = 0 OR c.app_id = TG_ARGV[0]::uuid)
       GROUP BY c.app_id, c.tx_id
    ), moved AS (
      DELETE FROM rust_tx_changes r USING m
       WHERE r.app_id = m.app_id AND r.tx_id = m.tx_id
         AND r.action = 'removed' AND r.ctid < m.last_added
      RETURNING r.tx_id, r.app_id, r.entity_id, r.attr_id, r.value, r.created_at,
                r.action, r.logged_at, r.seq
    )
    INSERT INTO rust_tx_changes
      (tx_id, app_id, entity_id, attr_id, value, created_at, action, logged_at, seq)
      OVERRIDING SYSTEM VALUE
    SELECT tx_id, app_id, entity_id, attr_id, value, created_at, action, logged_at, seq
      FROM moved;
    -- a new tuple can still land below the added row (a free line pointer
    -- on the same page, or an earlier page from the free-space map): retry
    passes := passes + 1;
    EXIT WHEN passes >= 16;
  END LOOP;
  RETURN NULL;
END $fn$ LANGUAGE plpgsql;

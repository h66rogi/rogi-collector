-- shared/migrations/004_metadata_category_code_lengthen.sql
--
-- Lengthen broadcast_metadata_history.category_code from VARCHAR(50) to TEXT.
--
-- Why: chzzk liveCategory values can exceed 50 chars for some games/categories,
-- causing INSERT to fail with SQLSTATE 22001 ("value too long for type
-- character varying(50)"). live_channels.category_code is already TEXT
-- (migration 003), so this aligns broadcast_metadata_history with it.
--
-- Lock behavior:
--   ALTER TABLE ... TYPE TEXT on a VARCHAR(N) column is metadata-only in
--   PG 11+ (no table rewrite, no index rebuild — category_code is not in
--   any index). Postgres 11+ propagates the change to all attached
--   partitions automatically.
--
--   The migration runner supplies the transaction and checksum ledger. A
--   manual invocation must use --single-transaction so SET LOCAL remains
--   scoped and the ALTER is atomic.
--
-- Apply (manual; chat-service has no automated migration runner):
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 --single-transaction \
--     -f shared/migrations/004_metadata_category_code_lengthen.sql
--
-- Verify:
--   SELECT column_name, data_type, character_maximum_length
--   FROM information_schema.columns
--   WHERE table_name LIKE 'broadcast_metadata_history%'
--     AND column_name = 'category_code';
--   -- Expect data_type=text, character_maximum_length=NULL on parent + all children.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE broadcast_metadata_history
    ALTER COLUMN category_code TYPE TEXT;

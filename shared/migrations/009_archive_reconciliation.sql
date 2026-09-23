ALTER TABLE archive_unassigned_chat
    ADD COLUMN next_retry_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

CREATE INDEX archive_unassigned_retry_idx
    ON archive_unassigned_chat (platform, channel_id, next_retry_at, id);

CREATE UNIQUE INDEX archive_quality_gaps_minute_idx
    ON archive_quality_gaps (platform, channel_id, started_at, reason);

DO $grants$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'collector_worker') THEN
        GRANT SELECT, UPDATE, DELETE ON archive_unassigned_chat TO collector_worker;
        GRANT UPDATE ON archive_quality_gaps TO collector_worker;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'collector_public_api') THEN
        GRANT SELECT ON archive_quality_gaps TO collector_public_api;
    END IF;
END
$grants$;

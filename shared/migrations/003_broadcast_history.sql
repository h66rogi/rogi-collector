-- shared/migrations/003_broadcast_history.sql

-- 1. live_channels 확장
ALTER TABLE live_channels
    ADD COLUMN IF NOT EXISTS current_session_seq BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS title TEXT,
    ADD COLUMN IF NOT EXISTS category TEXT,
    ADD COLUMN IF NOT EXISTS category_code TEXT,
    ADD COLUMN IF NOT EXISTS tags TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS thumbnail_url TEXT,
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS metadata_hash BYTEA;

-- 2. broadcast_sessions
CREATE TABLE IF NOT EXISTS broadcast_sessions (
    started_at           TIMESTAMPTZ NOT NULL,
    platform             VARCHAR(10) NOT NULL,
    channel_id           VARCHAR(100) NOT NULL,
    session_seq          BIGINT NOT NULL,
    streamer_name        VARCHAR(200) NOT NULL,
    title                TEXT,
    category             TEXT,
    started_observed_at  TIMESTAMPTZ NOT NULL,
    ended_at             TIMESTAMPTZ,
    ended_observed_at    TIMESTAMPTZ,
    first_seen_at        TIMESTAMPTZ NOT NULL,
    last_seen_at         TIMESTAMPTZ NOT NULL,
    peak_viewer_count    INT NOT NULL DEFAULT 0,
    instance_id          VARCHAR(50) NOT NULL,
    close_reason         VARCHAR(20),
    gap_detected         BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (started_at, platform, channel_id, session_seq),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
) PARTITION BY RANGE (started_at);

CREATE INDEX IF NOT EXISTS broadcast_sessions_lookup_idx
    ON broadcast_sessions (platform, channel_id, session_seq, started_at DESC);
CREATE INDEX IF NOT EXISTS broadcast_sessions_open_idx
    ON broadcast_sessions (platform, channel_id, started_at DESC)
    WHERE ended_at IS NULL;

-- 3. broadcast_metadata_history
CREATE TABLE IF NOT EXISTS broadcast_metadata_history (
    valid_from           TIMESTAMPTZ NOT NULL,
    platform             VARCHAR(10) NOT NULL,
    channel_id           VARCHAR(100) NOT NULL,
    session_seq          BIGINT NOT NULL,
    title                TEXT NOT NULL,
    category             TEXT,
    category_code        VARCHAR(50),
    tags                 TEXT[] NOT NULL DEFAULT '{}',
    thumbnail_url        TEXT,
    metadata_hash        BYTEA NOT NULL,
    valid_to             TIMESTAMPTZ,
    observed_at          TIMESTAMPTZ NOT NULL,
    instance_id          VARCHAR(50) NOT NULL,
    PRIMARY KEY (valid_from, platform, channel_id, session_seq),
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
) PARTITION BY RANGE (valid_from);

CREATE INDEX IF NOT EXISTS broadcast_metadata_lookup_idx
    ON broadcast_metadata_history (platform, channel_id, session_seq, valid_from DESC);
CREATE INDEX IF NOT EXISTS broadcast_metadata_current_idx
    ON broadcast_metadata_history (platform, channel_id, session_seq)
    WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS broadcast_metadata_hash_idx
    ON broadcast_metadata_history (platform, channel_id, session_seq, metadata_hash)
    WHERE valid_to IS NULL;

-- 4. Product bootstrap uses ordinary PostgreSQL; private source roles and
-- pg_partman provisioning are not part of the imported application schema.
-- History tracking is optional in the first single-channel runtime.
CREATE TABLE IF NOT EXISTS broadcast_sessions_default PARTITION OF broadcast_sessions DEFAULT;
CREATE TABLE IF NOT EXISTS broadcast_metadata_history_default PARTITION OF broadcast_metadata_history DEFAULT;

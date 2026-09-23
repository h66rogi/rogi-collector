-- Public chat archive foundation. No writer or retention job is enabled by
-- this additive migration. The application must not publish history endpoints
-- until the spool, S3 exporter, and recovery checks have passed.
CREATE TABLE archive_sessions (
    session_id UUID PRIMARY KEY,
    platform VARCHAR(10) NOT NULL,
    channel_id VARCHAR(100) NOT NULL,
    source_started_at TIMESTAMPTZ NOT NULL,
    source_session_seq BIGINT NOT NULL,
    source_broadcast_id TEXT,
    recording_started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (platform, channel_id, source_started_at, source_session_seq),
    FOREIGN KEY (source_started_at, platform, channel_id, source_session_seq)
        REFERENCES broadcast_sessions (started_at, platform, channel_id, session_seq)
);

-- This compact identity index remains after the message body is moved to S3.
-- It makes a replay idempotent even after hot rows have been pruned.
CREATE TABLE archive_event_ids (
    session_id UUID NOT NULL REFERENCES archive_sessions(session_id),
    event_id TEXT NOT NULL CHECK (length(event_id) BETWEEN 1 AND 255),
    position BIGINT GENERATED ALWAYS AS IDENTITY,
    PRIMARY KEY (session_id, event_id),
    UNIQUE (session_id, event_id, position),
    UNIQUE (position)
);
CREATE INDEX archive_event_ids_page_idx ON archive_event_ids (session_id, position);

CREATE TABLE archive_chat_hot (
    position BIGINT PRIMARY KEY REFERENCES archive_event_ids(position),
    session_id UUID NOT NULL,
    event_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    user_id_version SMALLINT NOT NULL CHECK (user_id_version > 0),
    public_user_id TEXT NOT NULL CHECK (length(public_user_id) = 64),
    display_name TEXT NOT NULL,
    message TEXT NOT NULL,
    FOREIGN KEY (session_id, event_id, position)
        REFERENCES archive_event_ids(session_id, event_id, position)
);
CREATE INDEX archive_chat_hot_page_idx ON archive_chat_hot (session_id, position);

-- Messages without an unambiguous open session are retained for explicit
-- reconciliation. They are never returned as part of another broadcast.
CREATE TABLE archive_unassigned_chat (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spool_id UUID NOT NULL UNIQUE,
    platform VARCHAR(10) NOT NULL,
    channel_id VARCHAR(100) NOT NULL,
    event_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    user_id_version SMALLINT NOT NULL CHECK (user_id_version > 0),
    public_user_id TEXT NOT NULL CHECK (length(public_user_id) = 64),
    display_name TEXT NOT NULL,
    message TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX archive_unassigned_review_idx ON archive_unassigned_chat (platform, channel_id, received_at);

CREATE TABLE archive_segments (
    segment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES archive_sessions(session_id),
    first_position BIGINT NOT NULL,
    last_position BIGINT NOT NULL,
    message_count INTEGER NOT NULL CHECK (message_count BETWEEN 1 AND 1000),
    object_bucket TEXT NOT NULL,
    object_key TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    byte_length BIGINT NOT NULL CHECK (byte_length > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (last_position >= first_position),
    UNIQUE (session_id, first_position, last_position)
);
CREATE INDEX archive_segments_page_idx ON archive_segments (session_id, first_position, last_position);

CREATE TABLE archive_quality_gaps (
    gap_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id UUID REFERENCES archive_sessions(session_id),
    platform VARCHAR(10) NOT NULL,
    channel_id VARCHAR(100) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    reason TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);
CREATE INDEX archive_quality_gaps_session_idx ON archive_quality_gaps (session_id, started_at);

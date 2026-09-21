CREATE TABLE IF NOT EXISTS collection_opt_out_batches (
    id              BIGSERIAL PRIMARY KEY,
    title           VARCHAR(160) NOT NULL,
    request_source  VARCHAR(120),
    reason          TEXT,
    requested_by    VARCHAR(320),
    created_by      VARCHAR(320),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collection_opt_outs (
    id                  BIGSERIAL PRIMARY KEY,
    batch_id            BIGINT REFERENCES collection_opt_out_batches(id) ON DELETE SET NULL,
    platform            VARCHAR(10) NOT NULL,
    channel_id          VARCHAR(100) NOT NULL,
    streamer_name       VARCHAR(200),
    requester_email     VARCHAR(320),
    reason              TEXT,
    active              BOOLEAN NOT NULL DEFAULT TRUE,
    created_by          VARCHAR(320),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at          TIMESTAMPTZ,
    revoked_by          VARCHAR(320),
    revocation_reason   TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_collection_opt_outs_active_channel
    ON collection_opt_outs (platform, channel_id)
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS idx_collection_opt_outs_platform_channel
    ON collection_opt_outs (platform, channel_id);

CREATE INDEX IF NOT EXISTS idx_collection_opt_outs_batch
    ON collection_opt_outs (batch_id);

CREATE INDEX IF NOT EXISTS idx_collection_opt_outs_active_created
    ON collection_opt_outs (active, created_at DESC);

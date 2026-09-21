CREATE TABLE IF NOT EXISTS workers (
    id            VARCHAR(50) PRIMARY KEY,
    status        VARCHAR(20) NOT NULL DEFAULT 'alive',
    max_capacity  INT NOT NULL DEFAULT 2000,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS live_channels (
    id            BIGSERIAL PRIMARY KEY,
    platform      VARCHAR(10) NOT NULL,
    channel_id    VARCHAR(100) NOT NULL,
    streamer_name VARCHAR(200),
    viewer_count  INT DEFAULT 0,
    status        VARCHAR(20) NOT NULL DEFAULT 'pending',
    worker_id     VARCHAR(50) REFERENCES workers(id) ON DELETE SET NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at      TIMESTAMPTZ,
    UNIQUE(platform, channel_id)
);

CREATE INDEX IF NOT EXISTS idx_live_channels_status ON live_channels(status);
CREATE INDEX IF NOT EXISTS idx_live_channels_worker ON live_channels(worker_id);
CREATE INDEX IF NOT EXISTS idx_live_channels_platform_status ON live_channels(platform, status);

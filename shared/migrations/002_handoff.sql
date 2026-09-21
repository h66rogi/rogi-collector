ALTER TABLE live_channels ADD COLUMN IF NOT EXISTS handoff_from VARCHAR(50) DEFAULT NULL;
ALTER TABLE live_channels ADD COLUMN IF NOT EXISTS handoff_status VARCHAR(20) DEFAULT NULL;
ALTER TABLE live_channels ADD COLUMN IF NOT EXISTS handoff_started_at TIMESTAMPTZ DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_live_channels_handoff_from_status
    ON live_channels (handoff_from, handoff_status)
    WHERE handoff_from IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_live_channels_worker_handoff
    ON live_channels (worker_id, handoff_status)
    WHERE handoff_status IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_live_channels_worker_platform_status
    ON live_channels (worker_id, platform, status);

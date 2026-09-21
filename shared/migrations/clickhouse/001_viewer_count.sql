CREATE TABLE IF NOT EXISTS viewer_count_history (
    valid_from     DateTime64(3, 'UTC'),
    valid_to       DateTime64(3, 'UTC') DEFAULT toDateTime64('2099-12-31 23:59:59.999', 3, 'UTC'),
    platform       Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id     LowCardinality(String),
    session_seq    UInt64,
    viewer_count   UInt32,
    min_count      UInt32,
    max_count      UInt32,
    sample_count   UInt16,
    instance_id    LowCardinality(String)
)
ENGINE = ReplacingMergeTree(valid_to)
PARTITION BY toYYYYMM(valid_from)
ORDER BY (platform, channel_id, session_seq, valid_from)
TTL toDateTime(valid_from) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS viewer_count_hourly (
    hour           DateTime,
    platform       Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id     LowCardinality(String),
    session_seq    UInt64,
    peak_viewers   SimpleAggregateFunction(max, UInt32),
    total_viewer_seconds SimpleAggregateFunction(sum, UInt64),
    sample_count   SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (platform, channel_id, session_seq, hour)
TTL hour + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS viewer_count_hourly_mv TO viewer_count_hourly
(hour DateTime, platform Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3), channel_id LowCardinality(String), session_seq UInt64, peak_viewers UInt32, total_viewer_seconds UInt64, sample_count UInt64)
AS
SELECT
    toStartOfHour(valid_from) AS hour,
    platform,
    channel_id,
    session_seq,
    max(max_count) AS peak_viewers,
    sum(toUInt64(viewer_count) * toUInt64(sample_count)) AS total_viewer_seconds,
    -- KNOWN BUG: alias `total_samples` does not match target column
    -- `sample_count`, so this value never reaches viewer_count_hourly
    -- (always 0). Kept as-is to match the legacy deployed schema. Do NOT
    -- read viewer_count_hourly.sample_count — it is unreliable.
    -- For accurate avg concurrent viewers, query viewer_count_history
    -- directly using (valid_to - valid_from) for duration.
    sum(toUInt64(sample_count)) AS total_samples
FROM viewer_count_history
GROUP BY hour, platform, channel_id, session_seq;

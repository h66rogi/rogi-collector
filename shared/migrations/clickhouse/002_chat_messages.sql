CREATE TABLE IF NOT EXISTS chat_messages (
    message_id       String,
    timestamp        DateTime64(3, 'UTC'),
    platform         Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id       LowCardinality(String),
    streamer_name    LowCardinality(String),
    user_id          String,
    nickname         String,
    message_type     Enum8('chat' = 1, 'donation' = 2, 'subscription' = 3, 'system' = 4),
    message_text     String,
    amount           Float32 DEFAULT 0,
    currency         LowCardinality(String) DEFAULT '',
    amount_krw       Int64 DEFAULT 0,
    worker_id        LowCardinality(String),
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (platform, channel_id, message_id)
TTL toDateTime(timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS chat_messages_hourly (
    hour             DateTime,
    platform         Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id       LowCardinality(String),
    message_type     Enum8('chat' = 1, 'donation' = 2, 'subscription' = 3, 'system' = 4),
    message_count    SimpleAggregateFunction(sum, UInt64),
    donation_count   SimpleAggregateFunction(sum, UInt64),
    total_krw        SimpleAggregateFunction(sum, Int64),
    unique_users     AggregateFunction(uniqExact, String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (platform, channel_id, message_type, hour)
TTL hour + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS chat_messages_hourly_mv TO chat_messages_hourly
(hour DateTime, platform Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3), channel_id LowCardinality(String), message_type Enum8('chat' = 1, 'donation' = 2, 'subscription' = 3, 'system' = 4), message_count UInt64, donation_count UInt64, total_krw Int64, unique_users AggregateFunction(uniqExact, String))
AS
SELECT
    toStartOfHour(timestamp) AS hour,
    platform,
    channel_id,
    message_type,
    toUInt64(1) AS message_count,
    toUInt64(max(toUInt8(amount > 0))) AS donation_count,
    sum(amount_krw) AS total_krw,
    uniqExactState(user_id) AS unique_users
FROM chat_messages
GROUP BY hour, platform, channel_id, message_type;

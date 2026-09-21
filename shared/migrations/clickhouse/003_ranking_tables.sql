-- 003_ranking_tables.sql
-- Ranking-oriented aggregation tables and materialized views.

-- ============================================================
-- 1. channel_daily_stats: 채널 일별 통계
-- ============================================================
CREATE TABLE IF NOT EXISTS channel_daily_stats (
    date             Date,
    platform         Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id       LowCardinality(String),
    chat_count       SimpleAggregateFunction(sum, UInt64),
    donation_count   SimpleAggregateFunction(sum, UInt64),
    subscription_count SimpleAggregateFunction(sum, UInt64),
    total_krw        SimpleAggregateFunction(sum, Int64),
    unique_chatters  AggregateFunction(uniqCombined64, String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, platform, channel_id)
TTL date + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS channel_daily_stats_mv TO channel_daily_stats
AS SELECT
    toDate(timestamp)                                                   AS date,
    platform,
    channel_id,
    countIf(message_type = 'chat')                                      AS chat_count,
    countIf(message_type = 'donation')                                  AS donation_count,
    countIf(message_type = 'subscription')                              AS subscription_count,
    sumIf(amount_krw, message_type = 'donation')                        AS total_krw,
    uniqCombined64StateIf(user_id, message_type != 'system'
                                   AND user_id != '')                   AS unique_chatters
FROM chat_messages
GROUP BY date, platform, channel_id;

-- ============================================================
-- 2. user_daily_stats: 유저 일별 통계
-- ============================================================
CREATE TABLE IF NOT EXISTS user_daily_stats (
    date             Date,
    platform         Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    user_id          String,
    nickname         SimpleAggregateFunction(anyLast, String),
    chat_count       SimpleAggregateFunction(sum, UInt64),
    donation_count   SimpleAggregateFunction(sum, UInt64),
    total_krw        SimpleAggregateFunction(sum, Int64),
    active_channels  AggregateFunction(uniqCombined64, String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, platform, user_id)
TTL date + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_daily_stats_mv TO user_daily_stats
AS SELECT
    toDate(timestamp)                                                   AS date,
    platform,
    user_id,
    anyLast(nickname)                                                   AS nickname,
    countIf(message_type = 'chat')                                      AS chat_count,
    countIf(message_type = 'donation')                                  AS donation_count,
    sumIf(amount_krw, message_type = 'donation')                        AS total_krw,
    uniqCombined64StateIf(channel_id, message_type != 'system')         AS active_channels
FROM chat_messages
WHERE user_id != ''
GROUP BY date, platform, user_id;

-- ============================================================
-- 3. user_channel_daily_stats: 유저x채널 일별 통계
-- ============================================================
CREATE TABLE IF NOT EXISTS user_channel_daily_stats (
    date             Date,
    platform         Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id       LowCardinality(String),
    user_id          String,
    nickname         SimpleAggregateFunction(anyLast, String),
    chat_count       SimpleAggregateFunction(sum, UInt64),
    donation_count   SimpleAggregateFunction(sum, UInt64),
    total_krw        SimpleAggregateFunction(sum, Int64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (platform, channel_id, date, user_id)
TTL date + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS user_channel_daily_stats_mv TO user_channel_daily_stats
AS SELECT
    toDate(timestamp)                                                   AS date,
    platform,
    channel_id,
    user_id,
    anyLast(nickname)                                                   AS nickname,
    countIf(message_type = 'chat')                                      AS chat_count,
    countIf(message_type = 'donation')                                  AS donation_count,
    sumIf(amount_krw, message_type = 'donation')                        AS total_krw
FROM chat_messages
WHERE user_id != ''
GROUP BY date, platform, channel_id, user_id;

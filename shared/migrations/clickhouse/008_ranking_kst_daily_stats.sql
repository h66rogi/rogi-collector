-- 008_ranking_kst_daily_stats.sql
-- Canonical completed-day ranking aggregates in Asia/Seoul.
--
-- These tables are intentionally snapshot-driven instead of fed by a
-- materialized view. A scheduled reconciliation job builds each completed
-- KST day from chat_messages FINAL in temporary tables and atomically replaces
-- the corresponding partition. This prevents bootstrap/realtime overlap from
-- double-counting rows and also removes ReplacingMergeTree duplicates.

CREATE TABLE IF NOT EXISTS channel_daily_stats_kst (
    date               Date,
    platform           Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id         LowCardinality(String),
    chat_count         SimpleAggregateFunction(sum, UInt64),
    donation_count     SimpleAggregateFunction(sum, UInt64),
    subscription_count SimpleAggregateFunction(sum, UInt64),
    total_krw          SimpleAggregateFunction(sum, Int64),
    unique_chatters    AggregateFunction(uniqCombined64, String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY date
ORDER BY (date, platform, channel_id)
TTL date + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS user_daily_stats_kst (
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
PARTITION BY date
ORDER BY (date, platform, user_id)
TTL date + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS user_channel_daily_stats_kst (
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
PARTITION BY date
ORDER BY (platform, channel_id, date, user_id)
TTL date + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS channel_daily_stats_kst_stage AS channel_daily_stats_kst;
CREATE TABLE IF NOT EXISTS user_daily_stats_kst_stage AS user_daily_stats_kst;
CREATE TABLE IF NOT EXISTS user_channel_daily_stats_kst_stage AS user_channel_daily_stats_kst;

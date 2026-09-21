-- 005_user_word_frequencies.sql
-- 시청자(발화자) 단위 단어 빈도 테이블
-- An offline text-analysis job periodically aggregates per-user keyword frequencies.
-- This table can support per-user summary features.

CREATE TABLE IF NOT EXISTS user_word_frequencies (
    platform    Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    user_id     String,
    period      Enum8('24h' = 1, '7d' = 2, '30d' = 3),
    word        LowCardinality(String),
    frequency   UInt64,
    computed_at DateTime
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY period
ORDER BY (period, platform, user_id, word)
TTL computed_at + INTERVAL 7 DAY;

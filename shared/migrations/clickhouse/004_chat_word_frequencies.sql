-- 004_chat_word_frequencies.sql
-- 채팅 워드클라우드용 단어 빈도 테이블
-- An offline text-analysis job periodically aggregates per-channel keyword frequencies.

CREATE TABLE IF NOT EXISTS chat_word_frequencies (
    platform    Enum8('chzzk' = 1, 'soop' = 2, 'cime' = 3),
    channel_id  LowCardinality(String),
    period      Enum8('24h' = 1, '7d' = 2, '30d' = 3),
    word        LowCardinality(String),
    frequency   UInt64,
    computed_at DateTime
) ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY period
ORDER BY (period, platform, channel_id, word)
TTL computed_at + INTERVAL 45 DAY;

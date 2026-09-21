-- ClickHouse aliases are visible throughout a SELECT expression. Using the
-- unqualified source column name `sample_count` with the same output alias can
-- therefore be interpreted as a nested aggregate on ClickHouse 25.3 and reject
-- every insert into viewer_count_history. Qualifying the source column keeps the
-- materialized-view aggregation unambiguous.
ALTER TABLE viewer_count_hourly_mv MODIFY QUERY
SELECT
    toStartOfHour(valid_from) AS hour,
    platform,
    channel_id,
    session_seq,
    max(max_count) AS peak_viewers,
    sum(toUInt64(viewer_count) * toUInt64(src.sample_count)) AS total_viewer_seconds,
    sum(toUInt64(src.sample_count)) AS sample_count
FROM viewer_count_history AS src
GROUP BY hour, platform, channel_id, session_seq;

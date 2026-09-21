-- Keep multiple last-known-good wordcloud generations available while a
-- failed or delayed producer is repaired. This extends retention only; it does
-- not rewrite existing rows or change the table key.
ALTER TABLE chat_word_frequencies
MODIFY TTL computed_at + INTERVAL 45 DAY;

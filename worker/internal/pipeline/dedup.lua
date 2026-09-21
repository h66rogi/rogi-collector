-- Atomic dedup check + dual-stream XADD.
-- KEYS[1] = dedup key
-- KEYS[2] = channel stream key
-- KEYS[3] = firehose stream key
-- ARGV[1] = dedup TTL (seconds)
-- ARGV[2] = channel maxlen
-- ARGV[3] = firehose maxlen
-- ARGV[4] = channel stream TTL (seconds)
-- ARGV[5..] = message field pairs (key, value, ...)

if redis.call('SET', KEYS[1], '1', 'NX', 'EX', tonumber(ARGV[1])) == false then
    return 0
end

redis.call('XADD', KEYS[2], 'MAXLEN', '~', tonumber(ARGV[2]), '*', unpack(ARGV, 5))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[4]))
redis.call('XADD', KEYS[3], 'MAXLEN', '~', tonumber(ARGV[3]), '*', unpack(ARGV, 5))
return 1

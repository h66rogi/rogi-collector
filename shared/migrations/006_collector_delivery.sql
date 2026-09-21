-- Additive application migration; do not replace the deployed foundation marker.
CREATE TABLE collector_channels (
 channel_id text PRIMARY KEY,
 generation text NOT NULL,
 next_offset numeric(20,0) NOT NULL DEFAULT 0 CHECK(next_offset BETWEEN 0 AND 18446744073709551615),
 expired_through numeric(20,0) NOT NULL DEFAULT 0,
 subscribed boolean NOT NULL DEFAULT true,
 owner_token text,
 runtime_state text NOT NULL DEFAULT 'waiting',
 state_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_received_at timestamptz
);
CREATE TABLE collector_owner_grants (
 token text PRIMARY KEY,
 channel_id text NOT NULL REFERENCES collector_channels(channel_id),
 worker_id text NOT NULL,
 generation text NOT NULL,
 granted_at timestamptz NOT NULL,
 valid_until timestamptz NOT NULL,
 closed_at timestamptz
);
CREATE TABLE collector_donations (
 event_id text PRIMARY KEY,
 channel_id text NOT NULL REFERENCES collector_channels(channel_id),
 generation text NOT NULL,
 channel_offset numeric(20,0) NOT NULL,
 source_key text,
 content_key text NOT NULL,
 connection_epoch text NOT NULL,
 observed_at timestamptz NOT NULL,
 payload jsonb,
 stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(channel_id,generation,channel_offset)
);
CREATE UNIQUE INDEX collector_source_identity ON collector_donations(channel_id,source_key) WHERE source_key IS NOT NULL;
CREATE INDEX collector_observation_match ON collector_donations(channel_id,content_key,observed_at DESC);
CREATE TABLE collector_outbox (
 event_id text PRIMARY KEY REFERENCES collector_donations(event_id),
 channel_id text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE collector_consumers (
 consumer_id text NOT NULL,
 channel_id text NOT NULL REFERENCES collector_channels(channel_id),
 generation text NOT NULL,
 baseline numeric(20,0) NOT NULL,
 ack_offset numeric(20,0),
 offered_offset numeric(20,0) NOT NULL,
 recovery_revision numeric(20,0) NOT NULL DEFAULT 0 CHECK(recovery_revision BETWEEN 0 AND 18446744073709551615),
 PRIMARY KEY(consumer_id,channel_id)
);
CREATE TABLE collector_requests (
 consumer_id text NOT NULL,
 channel_id text NOT NULL,
 operation text NOT NULL,
 request_key text NOT NULL,
 body_hash text NOT NULL,
 result jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(consumer_id,channel_id,operation,request_key)
);
CREATE TABLE collector_recovery_audit (
 id bigserial PRIMARY KEY,
 consumer_id text NOT NULL,
 channel_id text NOT NULL,
 request jsonb NOT NULL,
 result jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

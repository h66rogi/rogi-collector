# Architecture

## Why this system exists

Live-chat collection looks simple until it must cover many channels across
platforms with different discovery APIs, websocket protocols, rate limits, and
failure modes. A single process couples platform churn, connection count,
storage pressure, and query traffic into one failure domain. This design splits
those concerns while retaining a small operational core.

The main principles are:

1. PostgreSQL owns durable control-plane truth.
2. Redis carries short-lived coordination, commands, and recent messages.
3. Workers own connections; coordinators decide ownership.
4. Expensive historical writes are batched and optional.
5. Every retry queue, stream, request, and response has a bound.

## Components and flow

1. A discover leader periodically asks each platform for its live channels.
2. The discover service upserts channel lifecycle state in PostgreSQL.
3. A coordinator leader reads healthy worker heartbeats and pending channels.
4. The coordinator assigns each channel to the healthy worker with the lowest
   `connections / capacity` ratio below a configurable threshold.
5. It persists the assignment before publishing a connect command through
   Redis. If command delivery fails, worker reconciliation recovers from the
   durable assignment.
6. A worker connects to the platform, normalizes events, and publishes to a
   per-channel Redis Stream and a bounded firehose stream.
7. Optional batch writers append chat and viewer history to ClickHouse. Query
   services read recent data from Redis and durable state/history from
   PostgreSQL.

## Consistency model

PostgreSQL assignment is authoritative; Redis commands are an acceleration
path. This avoids requiring an atomic transaction across the database and
message bus. Reconciliation accepts temporary mismatch and converges toward the
database state.

Recent Redis streams are deliberately lossy and bounded. Optional ClickHouse
writes favor retaining failed batches for retry, with a separately bounded
secondary buffer where availability is more important than completeness.

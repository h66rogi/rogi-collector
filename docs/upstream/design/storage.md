# Storage model

The system uses different stores for different consistency and access needs.

## PostgreSQL: durable control plane

PostgreSQL stores workers, channel lifecycle and ownership, handoff state,
broadcast sessions, and collection opt-outs. Uniqueness on `(platform,
channel_id)` gives discovery an idempotent upsert target. Conditional updates
protect ownership transitions from stale coordinators and late workers.

Collection opt-outs are durable records. Workers cache them for connection
checks, and administrative changes can disconnect an active owner.

## Redis: ephemeral data plane

Redis is used for leases, heartbeats, pub/sub commands, deduplication keys, and
recent chat streams. Per-channel streams have bounded approximate length and an
expiry; the firehose is also bounded. These limits prevent an unattended cache
from becoming an unbounded historical database.

Redis command delivery is not the source of truth. Workers periodically compare
their connections with PostgreSQL assignments, which lets the system recover
from missed pub/sub messages.

## ClickHouse: optional analytical history

Chat and viewer-history rows can be sent to ClickHouse through generic batch
writers. A writer swaps pending rows into an in-flight batch, retries failures
with capped exponential backoff, and only discards them according to an
explicit bounded-buffer policy.

The primary writer favors durability by retaining failures. An optional
secondary sink uses a maximum pending count so a prolonged outage cannot exhaust
worker memory. Drop, retry, pending, and batch-size metrics make the trade-off
observable.

## Retention and exports

The cleanup utility removes expired transient state according to operator
policy. The exporter can copy selected analytical data to S3-compatible object
storage.

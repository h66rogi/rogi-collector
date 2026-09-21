# Coordination and handoff

## Leader election

Discovery and assignment are singleton activities even when several instances
are running. Each role acquires a Redis lease with an instance-specific value,
renews it before expiry, and stops leader-only work when renewal fails. Discovery
also carries a monotonically increasing fencing token so a delayed former leader
cannot publish state as if it still owned the lease.

Leases solve crash recovery without making process identity durable. They do
not make external requests idempotent, so state transitions are written using
conditional updates and uniqueness constraints where appropriate.

## Load-aware assignment

Workers publish heartbeat state including active connection count and capacity.
For every pending channel, the coordinator chooses the healthy worker with the
lowest load ratio under the configured threshold. Its in-memory view is updated
after each decision so a batch is spread rather than sent to one initially empty
worker.

Assignment follows this order:

1. Persist channel ownership in PostgreSQL.
2. Publish a connect command to the selected worker.
3. Let reconciliation repair missed command delivery from durable state.

That ordering can delay a connection after a Redis failure, but it avoids an
untracked worker connection becoming the only record of ownership.

## Graceful drain

A worker entering drain stops receiving new assignments. Each owned channel is
conditionally reassigned with `handoff_from`, `handoff_status`, and a start
timestamp. The destination connects before the old worker is retired, then
acknowledges and clears the handoff state.

The overlap reduces message gaps but may produce duplicate frames. During that
window the publisher hashes the raw event and uses an atomic Redis script with a
short TTL to deduplicate writes. If deduplication itself fails, the publisher
prefers a possible duplicate over silently losing the event.

Stale pending or acknowledged handoffs are reconciled. Conditional database
updates make repeated recovery attempts safe when a channel has already ended
or moved again.

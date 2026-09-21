# Reliability principles

## Bound every resource

- HTTP clients have deadlines and cap response bodies.
- HTTP servers set header, read, write, and idle timeouts.
- Redis streams have maximum lengths and per-channel expiry.
- Connector channels and optional secondary storage queues are bounded.
- Admin batches, request bodies, list limits, and field lengths are capped.
- Batch retries use capped backoff and expose pending/drop metrics.

Bounds turn overload into an explicit policy decision instead of an accidental
out-of-memory failure.

## Reconcile durable intent

Pub/sub makes normal operation responsive but does not guarantee delivery.
Workers therefore reconcile against PostgreSQL assignments. A command can be
retried or missed without permanently losing the desired connection state.

The same approach handles handoffs: state is recorded durably, workers
acknowledge successful connection, and coordinators retry transitions that stop
making progress.

## Isolate platform failures

Each platform has a separate connector and discovery implementation. Connector
startup is concurrent so a slow or failing platform does not serialize all
others. Retry behavior uses context cancellation and backoff. Protocol parsing
keeps raw payloads available for deterministic deduplication while normalizing
the fields used by storage and queries.

## Degrade intentionally

The recent-message path and durable analytical path have different goals. If an
atomic dedup operation fails during handoff, publishing proceeds because a
duplicate is easier to repair than an invisible gap. If an optional secondary
analytical sink remains unavailable, its bounded queue may drop new rows rather
than endanger connection ownership and the primary path.

## Observe transitions, not only uptime

Metrics cover leader state, pending assignments, handoffs, connector buffers,
publish errors, Redis stream length, batch retries, drops, and pending rows.
Health endpoints distinguish process liveness from readiness. Operators should
alert on stuck transitions and sustained queue growth before a process becomes
unavailable.

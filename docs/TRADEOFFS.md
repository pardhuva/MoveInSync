# Trade-offs & What Was Deliberately Left Out

The case study asks to "clearly define and document the trade-offs, especially consistency vs
availability vs latency, and explain what you deliberately left out." Here it is, honestly.

## 1. Consistency vs Availability vs Latency

This is a **real-time delivery** system, so when they conflict, the order of preference is:

> **Availability & latency first, strict global consistency last.**

Concretely:

- **At-least-once + client dedup, not exactly-once.** True exactly-once across a network and a
  crash needs distributed transactions (slow, fragile). We instead deliver at-least-once and let
  the client drop duplicates by sequence. Same user experience, a fraction of the cost. *Trade:* the
  client must keep a tiny bit of state (last seq per topic). Worth it.

- **Per-key ordering, not global ordering.** We guarantee order *within a trip*, not a single total
  order across all trips. Global ordering would need a single global sequencer — a bottleneck and a
  single point of failure. Nobody watching trip-100 cares how its events interleave with trip-200.
  *Trade:* no cross-trip ordering guarantee. Correct for the domain.

- **Pub/sub is fire-and-forget; the durable log is the truth.** Live delivery (Redis pub/sub) is
  allowed to drop a message under pressure, because the client notices the sequence gap and resumes
  from the durable log. *Trade:* a client may occasionally do a resume it "shouldn't have needed."
  Cheap and self-healing; keeps the fast path fast.

- **SOS latency beats SOS ordering-with-locations.** An SOS may be delivered *ahead* of queued
  locations for the same trip. It still carries its true sequence, so nothing is lost or
  mis-ordered by sequence — but on the wire the SOS can arrive before an older location. We chose
  the 2-second SOS guarantee over strict wire-order with low-value location frames. Documented and
  intentional.

- **Bounded staleness in the auth cache.** Ownership lookups are cached with a TTL. An ownership
  *revocation* can be served stale for up to the TTL (default 30s) unless explicitly invalidated
  (we do invalidate on the write path). *Trade:* a revoked user could linger briefly on a node that
  cached the old answer — but a stale entry can **never** grant cross-tenant access (it's only ever
  filled from the authoritative record for that same key). Availability/latency over instant global
  consistency, with a hard safety floor.

## 2. Design choices and their trade-offs

| Choice | Why | Trade-off accepted |
|---|---|---|
| **Raw WebSocket, not STOMP** | full control over the bounded queue, conflation, priority, resume | we implement a small protocol ourselves (frames in `FrameCodec`) |
| **Cabs publish over REST, not WS** | producers are fire-and-forget; no subscription machinery needed | two transports instead of one; simpler and cheaper overall |
| **Redis as the backbone** | gives sequencing (atomic Lua), replay (lists), snapshots, pub/sub in one dependency | Redis is a scaling ceiling (single-threaded) and a dependency; mitigated by per-topic sharding |
| **One event written to 3 topics** | trip/cab/office are all independently subscribable with their own sequence | ~3× write amplification; office topic is the costly one, and is toggleable |
| **In-process mode as the default** | the entire system runs and tests with zero infra | the default isn't horizontally scalable — it's for dev/test/demo; prod uses the `redis` profile |
| **Drain-scheduling on a shared pool** | avoids a thread per connection (won't survive 200k) | a connection's writes are slightly less "immediate" than a dedicated thread; bounded by batch size |
| **Snapshot built from latest-state, not a DB read** | one O(K) frame instead of unbounded history or a DB round-trip | snapshot reflects in-memory/Redis latest-state, which is exactly what a resuming client needs |

| **Ordered dispatch via partitioned serial executors** | strict per-topic live order without holding a lock across the fan-out | topics sharing a partition are serialised with each other; a saturated partition drops live dispatches (recoverable via resume) |
| **SOS bypasses ordered dispatch** | SOS must skip ahead everywhere, including the dispatch stage | an SOS can be distributed ahead of an earlier same-topic location; the sequence number still records true order |
| **Dedicated SOS express writer pool** | a saturated writer pool must not starve SOS (measured: shared pool p99 126–140 ms vs dedicated 6–60 ms; under full saturation a shared pool starves SOS entirely) | one more small pool; express flush uses `tryLock` and defers to the normal drain rather than queueing behind it |
| **Critical (STATUS/SOS) history kept separately from the replay window** | a location flood must not evict the events that matter most | extra bounded memory per topic; beyond its horizon the resume reports `criticalTruncated` |
| **Resume frames marked `replay: true`** | a recovered SOS must not be treated (or measured) as a live alert | one extra field on resume frames; clients should honour it |

## 3. What was deliberately left out (and why it's fine)

- **A real identity provider.** Tokens are minted by a dev endpoint gated behind the demo flag.
  Production would authenticate against an IdP and issue tokens there. The *verification* and
  *authorisation* paths are real and tested; only the issuance is a demo shortcut.
- **Persistent trip/ownership database.** `TripRepository` is in-memory (seeded). It's behind an
  interface with a caching decorator, so swapping in JPA/Postgres is a repository change only — the
  authorizers and services don't move.
- **Message payload schema/versioning.** The wire format is a hand-written JSON codec
  (`TripEventCodec`) — deliberate so the contract is explicit, but there's no schema registry or
  version negotiation. Fine at this scope; a real system would add a version field (the envelope
  already carries `eventId`, which gives a natural hook).
- **TLS / WSS termination.** Handled at the load balancer / ingress in a real deployment, not in the
  app. The nginx config shows where it would go.
- **Exactly-once producer idempotency.** A cab that retries a POST could create two events. The
  `eventId` gives a dedup hook, but server-side producer idempotency isn't implemented — locations
  conflate anyway, and duplicate statuses are rare and low-harm. Called out, not hidden.
- **Backfill from cold storage.** Beyond the replay window we send a snapshot, not full history.
  Full historical replay (e.g. for audit) would be a separate cold-path concern.
- **Strict cross-node live ordering.** Ordered dispatch is per node. Two gateway nodes publishing
  to the same topic are not mutually ordered by Redis pub/sub; clients reconcile by sequence. Making
  this strict would need a consume-side reorder buffer with an undecidable "is this gap permanent?"
  timeout. Not implemented, not claimed.
- **An SOS-acknowledgement flow.** `SnapshotStore.clearSos` is implemented but unused, so
  unresolved SOS accumulate per topic (unbounded in single-node mode; bounded by the 24h topic TTL
  in Redis). Adding blind eviction would risk dropping a genuinely unresolved SOS, so the
  limitation is documented rather than patched. See [COMPLEXITY.md](COMPLEXITY.md).
- **Autoscaling / service discovery.** The compose file is a fixed 2-node topology. Real scale-out
  (k8s, HPA, headless service for Redis) is deployment plumbing, out of scope for the case study.

## 4. The single most important trade-off

If there is one line to defend in an interview:

> **We never trade a silent, permanent loss for a transient, visible one.** When a critical event
> (status/SOS) cannot be queued, we *close the connection* (visible, auto-recovered via resume)
> rather than *drop the event* (silent, permanent). Everything downstream — the durable log,
> register-then-replay, overflow-closes — follows from that principle.

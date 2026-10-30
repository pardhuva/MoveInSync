# Interview Guide

Likely questions, with answers grounded in this codebase. The goal is to defend every significant
decision. Pair this with [CONCEPTS.md](CONCEPTS.md) (the "what") and [TRADEOFFS.md](TRADEOFFS.md)
(the "why this and not that").

---

### Q1. Who assigns the sequence number, and why does it matter that it's atomic?

The server assigns it, at one point per topic, atomically with the append to the log
(`EventLog.append`). It matters because ordering, dedup and resume all rest on it. If you took a
number and *then* appended as two steps, two concurrent publishers to the same trip could take 5
and 6 but store 6 before 5 — the replay log would be out of order and every client that later
resumes would see corrupted history. In-memory we hold a per-topic lock across increment+append; in
Redis we use one Lua script (`INCR`+`RPUSH`+`LTRIM`) which Redis runs atomically on its single
thread. `EventLogConcurrencyTest` proves 16 threads × 5,000 appends yield exactly 1..80000, sorted.

### Q2. How does an SOS stay under 2 seconds when the system is flooded?

Three things. (1) The per-connection `OutboundQueue` has a dedicated **express lane** for SOS,
always polled before locations/status. (2) A slow consumer can't stall others because fan-out only
*offers* into bounded queues and never blocks on a socket. (3) In Redis mode SOS travels on a
**separate channel with its own listener container** (its own connection and threads), so a backlog
of location messages can't queue ahead of it at the network hop. Measured: SOS p99 ≈ 475 ms at 10k
connections while a gateway is killed mid-test (`LoadSimulation`), p50 sub-5 ms.

### Q3. A client's phone was in a lift for 40 seconds. Walk me through the reconnect.

It reconnects, authenticates, and subscribes with `lastSeq` = the highest sequence it applied.
`SubscriptionService.subscribe` holds the connection's write lock, **registers the subscription
first** (so live events queue, not lost), then calls `EventLog.replayFrom(topic, lastSeq)`. If the
missed events are still in the bounded window, it replays exactly them in order; if the client was
away past the window, it sends a **snapshot** (latest location per cab, latest status, active SOS)
then goes live. Releasing the lock flushes queued live events in order. The client dedups by
sequence, so any overlap between replay and live is harmless. `ResumeIntegrationTest`.

### Q4. Why register the subscription *before* reading the replay buffer? Isn't that a double-send?

It can double-send — and that's the point. If I read the replay window first and registered
after, an event arriving in the gap between the two would be in neither the replay nor the live
stream: a **silent gap**. Registering first means such an event is delivered live (possibly also in
the replay) — a **duplicate**, which the client's `ClientSequenceTracker` drops. Duplicates are
recoverable; gaps are not. We always choose the recoverable failure.

### Q5. What exactly is conflation, and what is never conflated?

When a consumer lags, older **location** updates for a cab are collapsed into the latest one
(replaced in place in the queue), so the client sees the current position, not 400 stale dots, and
its location backlog is bounded to one-per-cab. **Status and SOS are never conflated** — every one
must be seen. Implemented in `OutboundQueue` (the location lane is a keyed map; status/SOS are
separate lanes). `OutboundQueueTest.locationsConflateToLatestPerCab`, `statusSurvivesLocationFlood`.

### Q6. What happens when a per-connection queue overflows?

Depends what's overflowing. If it's full and a droppable **location** arrives, we drop the location
(stay healthy). If admitting a **critical** event (status/SOS) would require losing another critical
event, we return `REJECTED_OVERFLOW` and the gateway **closes the connection**. That sounds
drastic, but it loses nothing: the client reconnects and resumes from the durable replay log. We
never silently drop a status/SOS. `OutboundQueueTest.criticalOverflowIsSignalled`.

### Q7. How does an event published on node A reach a subscriber on node B?

Via the `EventDistributor` backbone. A node declares interest in a topic when its first local
client subscribes. In Redis mode that's a `SUBSCRIBE` to the topic's channel; publishing is a
`PUBLISH`; Redis routes to exactly the subscribed nodes. "Which nodes care" is just the set of
channel subscribers — no separate directory to keep consistent. In single-node mode it's a direct
in-process call, which is why everything is testable without a broker.

### Q8. A gateway node crashes. What do its users lose?

Nothing important. The sequence counter, replay log and snapshots live in **Redis, not the
gateway's heap**. The clients' sockets drop; they reconnect (LB routes to a survivor) and resume
with their last sequence from the shared log — getting any missed status/SOS. `NodeCrashRecovery
Test` proves no status/SOS is lost; `LoadSimulation` kills a node under load and still delivers
~3,600 SOS afterwards.

### Q9. How is tenant isolation enforced, and where?

On **subscribe** (and publish), not just at login. `SubscriptionAuthorizer` resolves every topic to
its owning tenant and denies on a tenant mismatch **before** evaluating role. So no role — not even
a legitimate operator — can cross a tenant boundary. There's a test named for the exact scenario
the brief calls out: `employeeCannotSubscribeToAnotherTenantsTrip`. Crucially it's denied even if
the token *claims* to own that trip — ownership can't cross tenants.

### Q10. Why raw WebSocket instead of STOMP or a message broker's client?

Because the case study is *about* the delivery pipeline: the bounded queue, conflation, the SOS
priority lane, resume-from-sequence. STOMP's broker relay would own exactly those decisions. Raw
WebSocket + a tiny frame protocol (`FrameCodec`) keeps them in our code, where they can be tested
and defended.

### Q11. Why do cabs use REST but screens use WebSocket?

A cab is a **producer**: fire-and-forget, no subscription state, no need for a long-lived socket. A
screen is a **consumer** that needs a persistent, ordered, resumable stream. Matching the transport
to the role is simpler and cheaper than forcing both into one.

### Q12. What's the memory cost per connection, and what bounds it?

In steady state, O(cabs watched) location entries (conflation keeps one per cab) plus a few rare
critical events — a few KB. The SOS express lane and the main lane are bounded *independently*, so
the hard worst case is **`2 × queue-capacity` events** (default 2 × 1000 = 2000), not `capacity`.
At the measured ~230 B wire size that is ~460 KB wire (order of ~1–2 MB heap, estimated) before the
critical-overflow path closes the connection. The bound is pinned by `OutboundQueueBoundTest`. See
[COMPLEXITY.md](COMPLEXITY.md).

### Q13. How big is the replay buffer and what happens when it's full?

`capacity` events per topic (default 500), so O(topics × 500) events overall — which is why in
multi-node mode it lives in Redis, shared and TTL'd, not in each gateway's heap. When full, the
oldest events are evicted; a client that needed them gets a **snapshot** instead of a replay.
`capacity` is the knob trading resume depth against memory.

### Q14. How do you avoid a thread per connection at 200k connections?

Drain-scheduling. A connection submits a drain task to a shared writer pool only when it has bytes
to write and no task already in flight; the task writes a batch then reschedules if more remains. So
a connection holds a pool thread only while actively writing, at most one at a time. Writes take a
per-connection lock (to keep subscribe-ack-before-live-events), but the lock is per connection, so
one slow client never blocks another. `ClientConnection.drain`.

### Q15. What are the metrics you'd put on the 3 AM dashboard?

From `LiveTripsMetrics` (Prometheus): **SOS delivery p99** (the headline SLO), **queue depth** and
**conflation rate** (lag), **slow-consumer disconnects** and close reasons, **denied subscribes**
and auth failures (probing), **resume mode rate** (SNAPSHOT vs REPLAY — a rising snapshot rate means
the replay window is too small), publish/deliver throughput. Latency is recorded *after* a
successful socket write, not at enqueue — measuring at enqueue would hide the very back-pressure
this system manages. No metric carries a user/trip/cab tag (cardinality + privacy).

### Q16. How do you keep PII out of logs?

The authenticated principal and all log lines carry only opaque ids (userId, tripId, cabId) — never
name, phone, email or the location payload (a location says where a named person is *right now*).
Publish logs omit the payload. Error responses are generic ("not permitted", never "trip X exists
in tenant Y"), so trip ids can't be enumerated across tenants. `AuthenticatedPrincipal.toString`,
`PublishService` log lines, `Errors`.

### Q17. Where does this stop scaling?

First wall: connections per node (file descriptors + heap for queues) → add gateway nodes. Second:
Redis as the single sequencing/pub-sub point → shard by topic (sequence is per-topic, so sharding
is clean). Both are horizontal, not dead-ends. The costly stream is the **office topic** (every cab
in a site); conflation caps per-client cost and it's toggleable. See [COMPLEXITY.md §4](COMPLEXITY.md).

### Q18. What did you deliberately leave out?

A real IdP (dev token endpoint stands in, gated), a persistent ownership DB (in-memory behind an
interface + cache), schema versioning, TLS (LB concern), producer idempotency, cold-storage
backfill, autoscaling. All called out in [TRADEOFFS.md](TRADEOFFS.md) — none of them change the core
guarantees, which are all implemented and tested.

### Q19. If you had one more week, what would you add?

(1) A real-socket load test against the Docker topology to measure end-to-end (vs pipeline) latency.
(2) Redis Streams consumer groups instead of pub/sub, for stronger cross-node delivery guarantees.
(3) Per-topic Redis sharding for the sequencer. (4) A JPA-backed `TripRepository`. (5) Producer
idempotency via `eventId`. In priority order; the first is pure verification, the rest are scale.

### Q20. What's the single most important design principle here?

**Never trade a silent, permanent loss for a transient, visible one.** When a critical event can't
be queued, we close the connection (visible, auto-recovered by resume) rather than drop it (silent,
permanent). Register-then-replay, overflow-closes, and "the log is the truth, pub/sub is
best-effort" all follow from that one idea.

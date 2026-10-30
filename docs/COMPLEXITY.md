# Complexity & Cost Analysis

Time and space complexity of the core operations, the per-connection memory budget, the
replay-buffer cost, and where the system stops scaling. Backed by the measured benchmark in
[RESULTS.md](../benchmarks/RESULTS.md).

Notation: **S** = subscribers on a topic, **T** = active topics, **C** = replay capacity per topic
(default 500), **K** = distinct cabs a connection watches, **N** = events after a client's last seq.

## 1. Time complexity of core operations

| Operation | Complexity | Notes |
|---|---|---|
| `Sequencer.next` | **O(1)** | atomic increment |
| `EventLog.append` (in-memory) | **O(1)** amortised | increment + ring append; evict oldest when full |
| `EventLog.append` (Redis) | **O(1)** amortised | one Lua: `INCR` + `RPUSH` + `LTRIM` |
| `ReplayBuffer.replayFrom` | **O(N)** | N = missed events in window, N ≤ C |
| `OutboundQueue.offer` (location) | **O(1)** | hash lookup + in-place replace (conflation) |
| `OutboundQueue.offer` (status/SOS) | **O(1)** | append to a lane; O(1) oldest-location eviction if needed |
| `OutboundQueue.poll` | **O(1)** | express lane, else head of main lane |
| `SubscriptionAuthorizer.authorize` | **O(1)** | 1–2 cache/map lookups |
| `BoundedTtlCache.get/put` | **O(1)** | LinkedHashMap access-order LRU |
| **Fan-out one event** | **O(S)** | one `offer` per local subscriber; each offer is O(1) |
| **Publish one update** | **O(topics × 1)** = O(1) | writes to trip+cab+office (≤3) topics |

The whole hot path is O(1) per event per subscriber. There is no per-event sort, scan, or lock
over a shared structure beyond the single per-topic append point.

## 2. Space complexity & the per-connection memory budget

### Measured event size (not an estimate)
`EventWireSizeTest` serializes representative events through the real codec. **Measured JSON wire
size (UTF-8 bytes):**

| Event | Wire bytes (measured) |
|---|---|
| LOCATION (lat/lng/speed) | **242 B** |
| STATUS (status + delay) | **232 B** |
| SOS (reason code) | **214 B** |

So a frame is **~230 bytes on the wire**. The *in-heap* footprint of a `TripEvent` object is larger
(object headers, a boxed-value payload map, the 36-char UUID `eventId`, string overhead) — call it
**~0.5–1 KB per retained event as an estimate** (not measured; a true figure would need a tool such
as JOL). Figures below use the measured ~230 B wire size and flag heap separately.

### Per connection — exact bound
A connection holds its `OutboundQueue`. The **SOS express lane and the main lane are bounded
independently**, each by `queue-capacity`, so:

```
hard worst-case per-connection queue  =  2 × queue-capacity  events   (default 2 × 1000 = 2000)
steady-state                          ≈  O(K) location entries + a few critical events
```
The SOS lane is near-empty in normal operation, and location entries conflate to **one per cab
watched (K)**, so steady-state is a few KB. The hard ceiling is `2 × queue-capacity × ~230 B wire`
≈ **~460 KB wire (order of ~1–2 MB heap, estimated)** before the critical-overflow path closes the
connection. Plus small fixed bookkeeping (topic set, last-delivered map, locks). The `2 × capacity`
bound is pinned by `OutboundQueueBoundTest`.

**Answer to "state the memory cost per connection":** O(cabs watched) events in steady state; hard
worst case `2 × queue-capacity` events.

### Replay buffer
```
replay memory  =  O(T × C) events  ≈  active topics × 500 × ~230 B wire
```
*Worked example:* 20,000 cabs → ~20,000 trip topics + 20,000 cab topics + offices. At C=500 and
~230 B/event that is roughly `40,000 × 500 × 230 B ≈ 4.6 GB wire-equivalent` (more in heap) if every
topic is at full capacity simultaneously — which is why in Redis mode this lives in Redis (shared,
evictable, TTL'd), not in each gateway's heap, and why C is tunable. Lower C = cheaper but shorter
resume horizon.

**"What happens when either runs out":**
- Per-connection queue full of droppable locations → conflate / drop oldest location.
- Per-connection queue full of **critical** events → `REJECTED_OVERFLOW` → close → client resumes.
- Replay buffer full → oldest events evicted; a client needing them gets a **snapshot** instead.

### Known unbounded structure: unresolved SOS
`SnapshotStore` keeps unresolved SOS per topic keyed by eventId. `clearSos` exists as the
resolution hook but **has no production caller** (there is no SOS-acknowledged flow), so:

| Mode | Bound |
|---|---|
| Single-node (in-memory) | **unbounded** over process lifetime |
| Redis | bounded by the 24h `livetrips.replay.topic-ttl` on the topic's hash |

Practical size is small — SOS are rare, and only a long-lived `office:` topic accumulates.
Eviction is deliberately not implemented: discarding an unresolved SOS would break "SOS is never
dropped" for a client resuming far behind. Recent SOS are also carried by the bounded critical
history, so resume does not rely on this map alone. Fixing it properly means adding an
SOS-acknowledgement flow that calls `clearSos`.

## 3. Snapshot size
A snapshot is O(K + active statuses + active SOS) for a topic — the latest location per cab, the
latest status per trip, and any open SOS. Bounded and small; it replaces unbounded history with a
single current-state frame.

## 4. Where it stops scaling (and why)

| Dimension | Limit | Why / mitigation |
|---|---|---|
| Connections per node | ~tens of thousands per node | OS file descriptors + heap for queues; **scale horizontally** — add gateway nodes behind the LB |
| Total connections | 200k target | spread across N nodes; each node is independent; Redis pub/sub fans out |
| Hot topic (one trip, huge S) | fan-out is O(S) on one node | a single trip rarely has huge S; an office topic can — mitigate by conflation + optionally disabling office topics (`livetrips.publish.office-topics-enabled`) |
| Sequencer throughput | Redis single-threaded command loop | ~100k+ ops/s per Redis; **shard by topic** across Redis instances if exceeded (sequence is per-topic, so sharding is clean) |
| Replay memory | O(T × C) | lives in Redis; bound C; TTL idle topics |
| Office topic volume | every cab in a site on one stream | highest-amplification stream; conflation caps per-client cost; disable if unused |

The first wall you hit is **connections per node** (fd/heap), answered by adding nodes. The second
is **Redis as the single sequencing/pub-sub point**, answered by sharding per topic. Neither is a
design dead-end; both are horizontal.

## 5. Measured benchmark (summary)

On a 12-core laptop, JDK 19, `-Xmx3g`, in-process transport (see caveat in
[RESULTS.md](../benchmarks/RESULTS.md)):

- 10,000 connections, 2,000 cabs, 20 s, a gateway killed at T+10s
- **~21,600–22,600 frames/s** delivered
- **Live SOS p99 6–60 ms** (target < 2,000 ms), p50 ≈ 0 ms, mean < 2 ms
- 5,000 clients reconnected to the surviving node in 281–1,221 ms, **no SOS/status lost**

Latency is measured on **live** delivery only; SOS *recovered* via resume are counted separately,
because a recovered event's age includes the client's offline time, which the server cannot bound.
See [RESULTS.md](../benchmarks/RESULTS.md) for the regression that this distinction uncovered.

# Failure-Mode Table

What fails, what the user sees, how the system recovers, and where it's handled. This is the
"failure-mode table" the advanced track asks for.

| # | Failure | What the user sees | How the system recovers | Code / Test |
|---|---|---|---|---|
| 1 | **Client network blip** (lift, tunnel) | brief disconnect, then a catch-up | reconnect with last seq → replay missed events from the bounded buffer | `SubscriptionService.subscribe`; `ResumeIntegrationTest` |
| 2 | **Client away too long** (past replay window) | a one-shot state refresh, then live | `replayFrom` returns `SNAPSHOT_REQUIRED` → `SnapshotStore` snapshot, then live | `ReplayBuffer`; `ResumeIntegrationTest.outsideWindowGetsSnapshot` |
| 3 | **Slow / frozen consumer** | that client lags; its locations conflate; others unaffected | bounded queue absorbs it; locations conflate; if a critical event can't fit → close, client resumes on reconnect | `OutboundQueue`, `ClientConnection.closeSlowConsumer`; `SlowConsumerIsolationTest` |
| 4 | **Critical-event overflow** (queue full of status/SOS) | that one client is disconnected (and auto-reconnects) | `REJECTED_OVERFLOW` → close; nothing lost because replay log is the source of truth | `OutboundQueue.offer`; `OutboundQueueTest.criticalOverflowIsSignalled` |
| 5 | **Gateway node crash** | brief disconnect; reconnect to another node | LB routes to a surviving node; client resumes from shared Redis log/snapshot; no status/SOS lost | shared `EventLog`/`SnapshotStore`; `NodeCrashRecoveryTest`, `LoadSimulation` |
| 6 | **Duplicate delivery** (at-least-once, or redelivery on resume) | nothing — handled transparently | client drops any seq ≤ last applied | `ClientSequenceTracker`; `ClientSequenceTrackerTest` |
| 7 | **Out-of-order / gap** on the wire | nothing — client notices and resumes | `GAP` verdict signals a resume; sequences let the client reorder | `ClientSequenceTracker`; `ClientSequenceTrackerTest.detectsGap` |
| 8 | **Concurrent publish to one trip** | correct, ordered stream | atomic sequence assignment (lock in-memory / Lua in Redis) | `EventLog`; `EventLogConcurrencyTest` |
| 9 | **Unauthorised subscribe** (wrong tenant/role) | generic `FORBIDDEN`, connection stays open | denied before registration; security log line (opaque ids) + metric | `SubscriptionAuthorizer`; `SubscriptionAuthorizerTest` |
| 10 | **Forged / expired / garbage token** | `UNAUTHENTICATED`, socket closed | signature & expiry verified; invalid tokens not retried | `JwtService.verify`; `JwtServiceTest` |
| 11 | **Spoofed producer** (device publishing another cab) | `FORBIDDEN` | `PublishAuthorizer` restricts a DEVICE to its own cab; fields resolved from the trip record, not the request | `PublishAuthorizer`, `PublishService`; verified in live demo (403) |
| 12 | **Malformed client frame** | `BAD_FRAME` error, connection stays open | frame parse guarded; never crashes the handler | `LiveTripWebSocketHandler.handleTextMessage` |
| 13 | **Undecodable event on the backbone** | nothing — dropped, not fatal | Redis listener catches, logs, drops; thread survives | `RedisEventDistributor` message listener |
| 14 | **Unauthenticated socket left open** | closed after a timeout | handshake timeout closes sockets that never authenticate | `LiveTripWebSocketHandler.scheduleHandshakeTimeout` |
| 15 | **Blocked socket write** (peer stopped reading) | that client disconnected | container `asyncSendTimeout` + write-failure close; writer thread reclaimed | `WebSocketConfig`, `ClientConnection.drain` |
| 16 | **Redis unavailable (multi-node)** | degraded: no cross-node fan-out / resume | health reports DOWN; single-node mode is unaffected (no Redis). See Limitations. | `RedisDistributionConfig`; health indicator |
| 17 | **Trip ends mid-publish** (raced) | publisher gets `unknown trip`; subscribers keep final state | `PublishService` re-checks trip existence; topic state kept for late resumers | `PublishService.publish` |

| 18 | **Ordered-dispatch queue saturation** | brief live gap for that partition; client resumes | dispatch dropped + counted (`livetrips.dispatch.dropped`), never blocks the publisher; event is already in the replay log so resume recovers it | `OrderedDispatcher`; `OrderedDispatcherTest.saturationDropsAreCountedAndRecoverable` |
| 19 | **Critical-history overflow** (more STATUS/SOS missed than the horizon) | resume ack carries `criticalTruncated: true` | retained criticals still replayed; truncation reported + metric, never a silent loss | `SnapshotStore.criticalEventsAfter`; `SnapshotPreservesCriticalHistoryTest.criticalHistoryOverflowIsFlagged` |
| 20 | **Writer pool fully saturated by frozen consumers** | healthy clients still get SOS | dedicated express pool + `tryLock` express flush | `SosPriorityUnderPoolSaturationTest` (and its shared-pool starvation counter-example) |
| 21 | **Out-of-order snapshot record** | none — stale state can't win | `record` is sequence-monotonic (only a newer sequence overwrites) | `SnapshotRecordMonotonicTest` |

## Deliberate "fail-safe" choices

- **Overflow closes rather than drops.** When a critical event cannot be queued, we sever the
  connection instead of dropping the event. A disconnect is visible and recoverable (resume); a
  dropped status is silent and permanent. We never trade a silent permanent loss for a transient
  visible one.
- **Register-then-replay, never replay-then-register.** On subscribe we register for live events
  before reading the replay window, accepting possible duplicates (removed by dedup) to make gaps
  impossible.
- **Pub/sub is fire-and-forget; the log is the truth.** A missed live message is not a disaster:
  the client sees the sequence gap and resumes from the durable log. The fast path is allowed to be
  lossy precisely because the durable path backstops it.

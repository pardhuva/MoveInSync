# Requirements → Code → Test

Every rule in the case study, mapped to the code that implements it and the automated test that
proves it. 40 tests total, all green (`./mvnw test`).

## I. Topics & Access

| Requirement | Code | Test |
|---|---|---|
| Topics per cab, per trip, per office | `model/TopicType`, `model/Topic`, fan-out in `PublishService.topicsFor` | `SubscriptionAuthorizerTest` (all topic types) |
| Authorisation **per subscription** (not just login) | `SubscriptionService.subscribe` calls `SubscriptionAuthorizer.authorize` on every subscribe | `SubscriptionAuthorizerTest.employeeCanWatchOwnTrip` etc. |
| Employee → only their own trip | `SubscriptionAuthorizer.decideForTrip` (EMPLOYEE branch) | `employeeCanWatchOwnTrip`, `employeeCannotWatchColleaguesTrip` |
| Office admin → only their office | `SubscriptionAuthorizer` (OFFICE_ADMIN branch) | `officeAdminScopedToOwnOffice` |
| **Nobody → another client's data (tenant isolation)** | tenant check runs **before** role in `decideForTrip`/`authorizeOffice` | **`employeeCannotSubscribeToAnotherTenantsTrip`**, `operatorCannotCrossTenantBoundary` |

## II. Delivery Guarantees

| Requirement | Code | Test |
|---|---|---|
| Per-key (per-trip) ordering | sequence stored in order by `EventLog.append`; **ordered live dispatch** via `OrderedDispatcher` (single-node strict) | `EventLogConcurrencyTest`, `ReplayBufferTest`, **`OrderedDispatcherTest`** (3), **`ConcurrentSameTopicOrderingTest`** (2) |
| Per-topic sequence number | `Sequencer` / `EventLog.append` assigns per topic | `EventLogConcurrencyTest.concurrentAppendsAreTotallyOrdered` |
| At-least-once + client dedup | `ClientSequenceTracker.offer` (DUPLICATE/ACCEPT/GAP) | `ClientSequenceTrackerTest` (4 tests) |
| Resume from last sequence via bounded replay | `SubscriptionService.subscribe` + `EventLog.replayFrom` | `ResumeIntegrationTest.reconnectReplaysMissedStatusesAndLatestLocation`, `ReplayBufferTest` |
| Snapshot when away too long | `ReplayResult.SNAPSHOT_REQUIRED` → missed STATUS/SOS replayed from the bounded **critical history**, then `SnapshotStore.snapshot` | `ResumeIntegrationTest.outsideWindowGetsSnapshot`, `ReplayBufferTest.snapshotRequiredWhenOutsideWindow`, **`SnapshotPreservesCriticalHistoryTest`** (2) |

## III. Slow Consumers & Priority

| Requirement | Code | Test |
|---|---|---|
| Back-pressure: bounded per-connection queue | `OutboundQueue` (bounded) + `ClientConnection.offer` (never blocks) | `OutboundQueueTest.boundedByCapacity`, `SlowConsumerIsolationTest` |
| A slow client never slows others | drain-scheduling + offer-only fan-out in `DeliveryService` | **`SlowConsumerIsolationTest.frozenConsumerDoesNotBlockOthers`** |
| Conflation: latest location per cab | `OutboundQueue` location lane (in-place replace) | `OutboundQueueTest.locationsConflateToLatestPerCab`, `healthyConsumersConflateUnderFlood` |
| Status & SOS never dropped | critical/express lanes + overflow→close | `OutboundQueueTest.statusSurvivesLocationFlood`, `criticalOverflowIsSignalled` |
| Priority lane: SOS skips ahead | express lane polled first in `OutboundQueue.poll`; dedicated express writer pool; SOS bypasses ordered dispatch; separate Redis channel | `OutboundQueueTest.sosJumpsAheadOfLocations`, `multipleSosBeforeEverythingElse`, **`SosPriorityUnderPoolSaturationTest`** (3), `ConcurrentSameTopicOrderingTest.sosStillSkipsAhead` |

## IV. Scale-Out & Failure

| Requirement | Code | Test |
|---|---|---|
| Many gateway nodes; event reaches the right node | `EventDistributor` interest tracking; `RedisEventDistributor` pub/sub channels | `NodeCrashRecoveryTest.nodeBSnapshotReflectsEventsPublishedViaNodeA`; `LoadSimulation` (2 nodes) |
| Node crash: reconnect & resume, no status/SOS lost | shared `EventLog`+`SnapshotStore` (Redis); resume on reconnect | **`NodeCrashRecoveryTest.survivesNodeCrashWithoutLosingCriticalEvents`**; `LoadSimulation` (kills a node) |

## Example use cases (from the brief)

| Use case | Where it's proven |
|---|---|
| Phone in a lift 40s → 3 missed statuses in order + latest location | `ResumeIntegrationTest.reconnectReplaysMissedStatusesAndLatestLocation` |
| Frozen desk → conflated, while SOS p99 < 2s for others | `SlowConsumerIsolationTest.frozenConsumerDoesNotBlockOthers` + `LoadSimulation` |

## "Prove it" scale target

| Requirement | Evidence |
|---|---|
| ≥10,000 connections, ≥2,000 cabs | `LoadSimulation` default config (10k / 2k) — [RESULTS.md](../benchmarks/RESULTS.md) |
| Report SOS p99 latency | p99 ≈ 471–482 ms measured, 3 runs |
| Kill one gateway node during the test | `LoadSimulation` kills node-A at T+10s; 5,000 clients reconnect, ~3,600 SOS delivered after |

## Plus points (overall evaluation criteria)

| # | Criterion | Code | Test / Evidence |
|---|---|---|---|
| 1 | Authentication + RBAC + tenant isolation | `JwtService`, `SubscriptionAuthorizer`, `PublishAuthorizer`, roles incl. DEVICE | `JwtServiceTest`, `SubscriptionAuthorizerTest` |
| 2 | Time/space complexity + benchmark | [COMPLEXITY.md](COMPLEXITY.md) | `LoadSimulation`, [RESULTS.md](../benchmarks/RESULTS.md) |
| 3 | Failure handling + ≥1 failure-injection test | overflow→close, reconnect-resume, undecodable-message drop | **`SlowConsumerIsolationTest`**, `NodeCrashRecoveryTest` |
| 4 | OOP + clean controller/service/repository layering | `web/` → `service/` → `core/`+`repository/` | see [ARCHITECTURE.md §2](ARCHITECTURE.md) |
| 5 | Trade-offs documented | [TRADEOFFS.md](TRADEOFFS.md) | — |
| 6 | Monitoring: metrics + structured logging | `LiveTripsMetrics` (Prometheus), PII-safe structured logs | `/actuator/prometheus` (verified live) |
| 7 | Caching + eviction + bounded staleness | `CachingTripMetadataService`, `BoundedTtlCache` | `BoundedTtlCacheTest` |
| 2b | Stated memory bounds are exact and measured | `OutboundQueue` (2 × capacity), measured wire size | **`OutboundQueueBoundTest`** (2), **`EventWireSizeTest`** |
| 8 | Non-leaking error handling, no PII | `ApiException`, `Errors`, `GlobalExceptionHandler` | denials return generic messages; see [TRADEOFFS.md](TRADEOFFS.md) |

## Advanced-track deliverables

| Deliverable | Where |
|---|---|
| Automated test for every hard rule | this table |
| ≥1 concurrency test | `EventLogConcurrencyTest` (16 threads × 5,000) |
| ≥1 failure test | `SlowConsumerIsolationTest`, `NodeCrashRecoveryTest` |
| Load test w/ throughput + p50/p99 | [RESULTS.md](../benchmarks/RESULTS.md) |
| Design note: architecture + 1 sequence diagram + failure table | [ARCHITECTURE.md](ARCHITECTURE.md), [SEQUENCE-DIAGRAMS.md](SEQUENCE-DIAGRAMS.md), [FAILURE-MODES.md](FAILURE-MODES.md) |
| One-command run | `docker compose up --build` |
| Demo | [DEMO.md](DEMO.md), [demo/](../demo) |

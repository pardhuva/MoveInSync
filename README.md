# Live Trip Updates Fan-Out — Ordered, Resumable, Back-Pressured

MoveInSync Backend Case Study 4 · SDE Java Backend · Advanced Track

A WebSocket **publish-subscribe layer** that streams live cab location, trip-status, delay and **SOS** updates to security desks, transport admins and employees.

It is designed around the case study's peak requirements — **20,000 cabs** producing updates and **200,000 screens** consuming them — with a critical requirement: an SOS must reach the security desk within 2 seconds, even when the system is busy.

---

> **TL;DR for a reviewer in a hurry**
>
> - Single-node demo runs without Docker or Redis.
> - Full Maven test suite: **62 tests passed, 0 failures, 0 errors, 0 skipped** (Redis integration tests run against Memurai).
> - Load simulation: 10,000 connections, 2,000 publishing cabs and a simulated gateway failure.
> - Latest measured application-pipeline SOS p99: **38 ms**.
> - Latest measured throughput: **22,308 frames/second**.
> - Architecture, diagrams, failure analysis, complexity and interview guide: [docs/](docs/).
> - Docker Compose multi-node topology is provided, but a complete Docker deployment and real-socket load test have not yet been verified.

---

## 1. What this is (and what it is not)

**It is** the real-time delivery backend: topics and subscriptions, per-topic ordering, at-least-once delivery with deduplication, resume-from-sequence, bounded per-connection queues, back-pressure, location conflation, an SOS priority lane, multi-gateway support and node-crash recovery — plus authentication/RBAC, tenant isolation, metrics, caching and PII-safe error handling.

**It is not** a full ride-booking application or frontend. Cabs publish over authenticated REST; screens subscribe over WebSocket. A small JDK WebSocket demo client and curl examples stand in for the UI.

## 2. Architecture at a glance

```text
                         ┌──────────────────────────────────────────────┐
  cabs (REST POST) ─────▶│  Gateway node (Spring Boot)                   │
  screens (WebSocket) ◀─▶│  WS handler → Subscription/Delivery services  │
                         │  core: Sequencer · ReplayBuffer · OutboundQ   │
                         └───────────────┬──────────────────────────────┘
                                         │ (EventDistributor interface)
             single node: in-process ◀───┤
             multi node : Redis       ◀──┘ shared: event log + snapshots + pub/sub
                                         │
    ┌──────────────┐   ┌──────────────┐   │       ┌─────────┐
    │ gateway-1    │◀──┤  nginx LB    ├───┴──────▶│  Redis  │
    │ gateway-2    │   │ (ws + rest)  │           └─────────┘
    └──────────────┘   └──────────────┘
```

The same application code is designed to run in single-node mode (in-process backbone, the default) or multi-node mode (Redis backbone, the `redis` profile).

The gateway, service and core layers depend on three interfaces — `EventLog`, `SnapshotStore` and `EventDistributor` — whose implementations are selected by the Spring profile.

Full detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## 3. Quick start

### Option A — Multi-node Docker Compose topology (not yet verified end-to-end)

The repository includes a Docker Compose configuration intended to start Redis, two gateway nodes (`gateway-1`, `gateway-2`) using the `redis` profile, and an nginx WebSocket-aware load balancer.

The intended command is:

```bash
docker compose up --build
```

The intended entry point is:

```text
http://localhost:8080
```

To simulate a gateway failure after the topology is running:

```bash
docker compose kill gateway-1
```

Clients should reconnect through the load balancer to the remaining gateway and resume from their last acknowledged sequence.

**Verification status:** The complete Docker Compose deployment and real-TCP/WebSocket failure-recovery scenario have not yet been executed successfully in the authoring environment. Treat these commands as the provided deployment configuration, not as a verified end-to-end demonstration.

### Option B — Single node, no Docker, no Redis

This is the locally demonstrated setup.

Build the application:

```bash
./mvnw -DskipTests package
```

On Windows, use:

```powershell
.\mvnw.cmd -DskipTests package
```

Start the application with the demo profile:

Linux/macOS:

```bash
LIVETRIPS_JWT_SECRET=please-change-me-to-a-32-byte-min-secret \
SPRING_PROFILES_ACTIVE=demo \
java -jar target/live-trip-updates.jar
```

Windows PowerShell:

```powershell
$env:LIVETRIPS_JWT_SECRET="please-change-me-to-a-32-byte-min-secret"
$env:SPRING_PROFILES_ACTIVE="demo"
java -jar target/live-trip-updates.jar
```

Starts one gateway on **http://localhost:8080** with an in-process backbone.

> **Why `SPRING_PROFILES_ACTIVE=demo`?**
>
> The `demo` profile is the only configuration that seeds demo tenants and opens `POST /api/v1/dev/token`, which mints a signed token for a claimed identity.
>
> That endpoint bypasses normal authentication and is **disabled by default**. Without the demo profile, it is not registered and returns 404. Never enable it outside a local demo.

> **Prerequisites:** JDK 17+ (built and tested here on JDK 19). The Maven wrapper (`./mvnw`) downloads Maven itself, so a separate Maven installation is not needed. On Windows, use `mvnw.cmd`.

## 4. Try it (sample API calls)

The demo profile seeds two tenants (`acme`, `globex`) so tenant isolation can be demonstrated.

Full runnable script: [demo/demo.sh](demo/demo.sh).

A sample flow:

```bash
# 1. Mint a device token for cab-7 (DEVICE may publish only its own cab)
DEV=$(curl -s -X POST localhost:8080/api/v1/dev/token \
  -H 'Content-Type: application/json' \
  -d '{"userId":"dev-7","tenantId":"acme","role":"DEVICE","cabId":"cab-7"}' | jq -r .token)

# 2. Publish a location, a status and an SOS for trip-100
curl -s -X POST localhost:8080/api/v1/trips/trip-100/events \
  -H "Authorization: Bearer $DEV" \
  -H 'Content-Type: application/json' \
  -d '{"type":"LOCATION","lat":12.95,"lng":77.61}'

# Example response: per-topic sequences
# {"trip:trip-100":1,"cab:cab-7":1,"office:office-BLR-1":1}

# 3. Subscribe over WebSocket and watch events arrive
EMP=$(curl -s -X POST localhost:8080/api/v1/dev/token \
  -H 'Content-Type: application/json' \
  -d '{"userId":"emp-alice","tenantId":"acme","role":"EMPLOYEE","tripIds":["trip-100"]}' | jq -r .token)

javac demo/WebSocketDemoClient.java -d /tmp/wsdemo

java -cp /tmp/wsdemo WebSocketDemoClient \
  ws://localhost:8080/ws/live "$EMP" trip:trip-100 0
```

A captured run of this flow (welcome → authenticated → subscribed → snapshot → live LOCATION/STATUS/SOS) is documented in [docs/DEMO.md](docs/DEMO.md).

## 5. Running the tests

Run the complete test suite:

```bash
./mvnw test
```

On Windows:

```powershell
.\mvnw.cmd test
```

The full Maven test suite completed with:

| Result | Count |
|---|---:|
| Tests run | 62 |
| Failures | 0 |
| Errors | 0 |
| Skipped | 0 |
| Build | SUCCESS |

**Redis test environment:** Redis-backed integration tests passed against Memurai, a Redis-protocol-compatible Windows development server. This validates the Redis protocol interactions exercised by those tests, not a production Redis deployment.

Run the load simulation separately:

```bash
./mvnw -Dtest=LoadSimulation test
```

On Windows:

```powershell
.\mvnw.cmd "-Dtest=LoadSimulation" "-Dload.connections=10000" "-Dload.cabs=2000" "-Dload.durationSeconds=20" test
```

The core logic (sequencing, replay, outbound queues and authorization) is tested independently of the Spring context, web server and database.

See the full requirement-to-test mapping in [docs/REQUIREMENTS-MAPPING.md](docs/REQUIREMENTS-MAPPING.md).

## 6. Measured performance

The following results are from the latest recorded `LoadSimulation` run on Windows 11, Oracle JDK 19.0.1, with 12 logical CPU cores.

| Metric | Latest result |
|---|---:|
| Connections | 10,000 |
| Publishing cabs | 2,000 |
| Duration | 20 seconds |
| Locations published | 82,875 |
| SOS published | 1,050 |
| Frames delivered | 446,163 |
| Delivery throughput | 22,308 frames/s |
| **Live SOS latency p99** | **38 ms** |
| SOS latency p50 | 0 ms |
| SOS latency maximum | 248 ms |
| SOS latency mean | 0.9 ms |
| SOS p99 below 2 seconds | YES |
| Clients reconnected after Node A failure | 5,000 |
| SOS delivered after kill | 2,704 |
| SOS recovered through resume | 1,356 |

**Important measurement caveat:** The load generator uses in-process transports, not real TCP sockets.

These are **application-pipeline latency** measurements, covering sequencing, ordered dispatch, cross-node fan-out, bounded outbound queues, location conflation and JSON serialization.

They deliberately exclude real network round-trip time and operating-system socket scheduling. Therefore, the measured 38 ms SOS p99 must not be interpreted as end-to-end network latency.

The simulation exercises 10,000 virtual connections across two gateway harnesses, publishes from 2,000 simulated cabs, kills one gateway and reconnects its clients to the other gateway. It demonstrates application-level recovery behavior under the simulated failure scenario.

A real-socket load test against the Docker Compose topology remains future validation work.

Full benchmark methodology, historical experiments and caveats: [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

## 7. Documentation map

| Document | What's in it |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Components, responsibilities, key classes/methods, design decisions and technology choices |
| [docs/CONCEPTS.md](docs/CONCEPTS.md) | Beginner-friendly explanation of important concepts, with examples from this code |
| [docs/SEQUENCE-DIAGRAMS.md](docs/SEQUENCE-DIAGRAMS.md) | Sequence diagrams for resume, SOS and node-crash flows |
| [docs/REQUIREMENTS-MAPPING.md](docs/REQUIREMENTS-MAPPING.md) | Every case-study rule mapped to the code and its corresponding test |
| [docs/FAILURE-MODES.md](docs/FAILURE-MODES.md) | Failure-mode table: what fails, what the user sees and how it recovers |
| [docs/COMPLEXITY.md](docs/COMPLEXITY.md) | Time/space complexity, per-connection memory cost and scaling limitations |
| [docs/TRADEOFFS.md](docs/TRADEOFFS.md) | Consistency vs availability vs latency, and deliberate design trade-offs |
| [docs/INTERVIEW-GUIDE.md](docs/INTERVIEW-GUIDE.md) | Likely interview questions with answers grounded in the implementation |
| [docs/DEMO.md](docs/DEMO.md) | Captured demo transcripts and reproduction instructions |
| [benchmarks/RESULTS.md](benchmarks/RESULTS.md) | Detailed load-test results, environment, methodology and reproduction command |

## 8. Configuration knobs

All properties are under `livetrips.*` in [application.yml](src/main/resources/application.yml). The following are particularly relevant to the case study:

| Property | Default | Meaning |
|---|---:|---|
| `livetrips.replay.capacity` | 500 | Per-topic replay window (events). If a client misses more than this window, it falls back to a snapshot. |
| `livetrips.replay.critical-capacity` | 256 | Per-topic STATUS/SOS history, retained independently of the location window so the snapshot path can replay missed critical events. |
| `livetrips.publish.dispatch-partitions` | 16 | Single-thread partitions that provide strict per-topic live ordering. |
| `livetrips.gateway.queue-capacity` | 1000 | Per-connection outbound queue capacity. SOS and main lanes are bounded independently, so the hard worst case is 2 × this value. |

## 9. Security & privacy notes

- JWT signing key is read from `LIVETRIPS_JWT_SECRET`. If absent, an **ephemeral** key is generated and a warning is logged. Tokens then become invalid when the process restarts. No signing key is committed.
- The developer token endpoint (`POST /api/v1/dev/token`) and demo seeding are **disabled by default** and exist only under the `demo` profile.
- A cross-tenant trip ID and a non-existent trip ID return **identical** responses, so trip IDs cannot be enumerated across tenants (`TenantEnumerationTest`).
- Logs and error responses carry **no PII** — no name, phone, email or location payload, only opaque IDs. A caller who may not see a trip is told only "not permitted", never whether it exists.
- `.env` files, keys and local overrides are git-ignored.

# Sequence Diagrams

Mermaid diagrams for the flows that matter. They render on GitHub.

## 1. Publish → fan-out (the write path)

A cab publishes an update; it is sequenced once per topic and fanned out to subscribers.

```mermaid
sequenceDiagram
    participant Cab as Cab device
    participant REST as PublishController
    participant PS as PublishService
    participant PA as PublishAuthorizer
    participant EL as EventLog
    participant SS as SnapshotStore
    participant D as EventDistributor
    participant DEL as DeliveryService
    participant Q as OutboundQueue(s)

    Cab->>REST: POST /trips/trip-100/events (Bearer token)
    REST->>PS: publish(principal, command)
    PS->>PA: authorize(principal, tripId)
    PA-->>PS: ALLOW / DENY
    Note over PS: resolve cab/office/tenant from the trip record,<br/>stamp origin timestamp (not the device clock)
    loop for each topic: trip, cab, office
        PS->>EL: append(event@topic)
        Note over EL: atomic INCR + append<br/>(Lua in Redis, lock in-memory)
        EL-->>PS: event with sequence
        PS->>SS: record(event)  %% update latest-state snapshot
        PS->>D: publish(event)
    end
    PS-->>REST: {topic: sequence, ...}
    D->>DEL: onEvent(event)  %% on every interested node
    DEL->>Q: offer(event) for each local subscriber
    Note over Q: SOS→express · STATUS→critical · LOCATION→conflated
```

## 2. Subscribe + resume (the hardest flow)

The "phone in a lift" case: a client reconnects and must get exactly what it missed, in order.

```mermaid
sequenceDiagram
    participant Cli as Client (screen)
    participant WS as WebSocketHandler
    participant SUB as SubscriptionService
    participant AUTH as SubscriptionAuthorizer
    participant REG as ConnectionRegistry
    participant EL as EventLog
    participant SS as SnapshotStore

    Cli->>WS: {action: authenticate, token}
    WS-->>Cli: {kind: authenticated}
    Cli->>WS: {action: subscribe, topic: trip:trip-100, lastSeq: 7}
    WS->>SUB: subscribe(conn, topic, 7)
    SUB->>AUTH: authorize(principal, topic)
    AUTH-->>SUB: ALLOW  %% else deny (tenant/role) and stop

    Note over SUB,REG: hold the connection write lock
    SUB->>REG: register(conn, topic)  %% FIRST: live events now queue, not lost
    SUB->>EL: replayFrom(topic, 7)
    alt within replay window
        EL-->>SUB: REPLAY [8,9,10], latest=10
        SUB-->>Cli: {kind: subscribed, mode: REPLAY, latestSeq: 10}
        SUB-->>Cli: events 8, 9, 10 (in order)
    else away too long (evicted)
        EL-->>SUB: SNAPSHOT_REQUIRED
        SUB->>SS: snapshot(topic)
        SUB-->>Cli: {kind: subscribed, mode: SNAPSHOT}
        SUB-->>Cli: {kind: snapshot, latest location per cab + active SOS}
    end
    Note over SUB: release write lock → queued live events flush in order
    SUB-->>Cli: live events 11, 12, ... (client dedups by sequence)
```

## 3. Slow consumer + SOS priority (back-pressure under failure)

One desk freezes; its queue fills and conflates, while SOS still reaches every healthy desk fast.

```mermaid
sequenceDiagram
    participant Cab as Cab (flooding locations)
    participant DEL as DeliveryService
    participant QF as Frozen desk queue
    participant QH as Healthy desk queue
    participant TF as Frozen transport (blocked)
    participant TH as Healthy transport

    loop 2000 location updates
        Cab->>DEL: location
        DEL->>QF: offer(location)
        Note over QF: conflated to latest-per-cab<br/>(bounded; never grows)
        DEL->>QH: offer(location)
        QH->>TH: drained to socket
    end
    Note over TF: frozen: its drain thread is blocked on send()
    Cab->>DEL: SOS
    DEL->>QF: offer(SOS) → express lane (bounded)
    DEL->>QH: offer(SOS) → express lane
    QH->>TH: SOS delivered FIRST (<2s), ahead of locations
    Note over QF,TF: frozen desk still stuck, but isolated;<br/>if a critical event can't fit → REJECTED_OVERFLOW → close,<br/>client reconnects and resumes losslessly
```

## 4. Node crash + recovery

A gateway node dies; its clients reconnect elsewhere and resume from the shared backbone.

```mermaid
sequenceDiagram
    participant Cli as Client
    participant GW1 as Gateway-1
    participant R as Redis (shared log + snapshot)
    participant GW2 as Gateway-2
    participant LB as Load Balancer

    Cli->>GW1: subscribed to trip-100, last applied seq=5
    GW1->>R: (events appended here, seq 6,7,8 by other producers)
    Note over GW1: 💥 Gateway-1 crashes
    GW1--xCli: socket closes
    Cli->>LB: reconnect
    LB->>GW2: route (Gateway-1 marked down)
    Cli->>GW2: subscribe trip-100, lastSeq=5
    GW2->>R: replayFrom(trip-100, 5)
    R-->>GW2: events 6,7,8 (incl. any STATUS/SOS)
    GW2-->>Cli: replay 6,7,8 then live
    Note over Cli: no status or SOS lost
```

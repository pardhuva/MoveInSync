# Demo

How to run the demo flows, plus a **real captured transcript** proving the end-to-end WebSocket
path works on the running server (not just in tests).

## Running it

1. Start the server (single node, no Docker needed):
   ```bash
   ./mvnw -DskipTests package
   LIVETRIPS_JWT_SECRET=please-change-me-32-bytes-minimum \
   SPRING_PROFILES_ACTIVE=demo java -jar target/live-trip-updates.jar
   ```
2. Run the scripted demo (mints tokens, publishes, shows tenant isolation):
   ```bash
   bash demo/demo.sh
   ```
3. Watch events live over WebSocket (JDK-only client, no dependencies):
   ```bash
   javac demo/WebSocketDemoClient.java -d /tmp/wsdemo
   TOKEN=$(curl -s -X POST localhost:8080/api/v1/dev/token -H 'Content-Type: application/json' \
     -d '{"userId":"emp-alice","tenantId":"acme","role":"EMPLOYEE","tripIds":["trip-100"]}' | jq -r .token)
   java -cp /tmp/wsdemo WebSocketDemoClient ws://localhost:8080/ws/live "$TOKEN" trip:trip-100 0
   ```

## Captured transcript (real run on the live server)

A subscriber (`emp-alice` → `trip:trip-100`) connects; three events are then published by the cab
device (`cab-7`). This is verbatim from the WebSocket demo client:

```
[open] connected to ws://localhost:8080/ws/live
[recv] {"nodeId":"gw-local","kind":"welcome"}
[recv] {"kind":"authenticated","userId":"emp-alice","role":"EMPLOYEE"}
[recv] {"kind":"subscribed","mode":"SNAPSHOT","latestSeq":2,"topic":"trip:trip-100"}
[recv] {"kind":"snapshot","topic":"trip:trip-100","atSeq":2,
        "locations":{"cab-7":{...,"seq":1,"type":"LOCATION","payload":{"lng":77.61,"lat":12.95}}},
        "statuses":{},
        "sos":[{...,"seq":2,"type":"SOS","payload":{"reasonCode":"PANIC"}}]}
[recv] {"kind":"event","seq":3,"type":"LOCATION","payload":{"lng":77.63,"lat":12.97}}
[recv] {"kind":"event","seq":4,"type":"STATUS","payload":{"delayMinutes":4,"status":"DELAYED"}}
[recv] {"kind":"event","seq":5,"type":"SOS","payload":{"reasonCode":"PANIC"}}
[info] done
```

**What this proves, live:**
- WebSocket handshake + **authenticate-via-frame** (browsers can't set handshake headers) ✓
- **subscribe** returns mode `SNAPSHOT` for a fresh subscriber ✓
- the **snapshot** carries the latest location per cab and the active SOS (current state, not history) ✓
- **live fan-out** of LOCATION, STATUS and SOS with monotonic **per-topic sequence** (3, 4, 5) ✓

## REST flow (also captured, real server)

```
# device token for cab-7, then publish:
POST /api/v1/trips/trip-100/events  (LOCATION)  -> {"trip:trip-100":1,"cab:cab-7":1,"office:office-BLR-1":1}
POST /api/v1/trips/trip-100/events  (SOS)       -> {"trip:trip-100":2,"cab:cab-7":2,"office:office-BLR-1":2}

# TENANT ISOLATION: acme device -> globex trip-200
POST /api/v1/trips/trip-200/events              -> HTTP 403
```

The response to a publish is the **per-topic sequence** assigned on each of the trip, cab and
office topics — exactly the "per-topic sequence number" model.

## Demonstrating node-crash recovery (Docker)

```bash
docker compose up --build            # redis + gateway-1 + gateway-2 + nginx on :8080
# connect a WS client through :8080, subscribe, then:
docker compose kill gateway-1        # the node holding some clients dies
# clients reconnect through nginx to gateway-2 and resume from the shared Redis log — no SOS lost
```

The deterministic version of this (no Docker) is `NodeCrashRecoveryTest`; the under-load version is
`LoadSimulation`, which kills a gateway at T+10s and still delivers ~3,600 SOS afterwards.

## A note on the demo token endpoint

`POST /api/v1/dev/token` mints a token for any claimed identity. **This is a demo-only hole**, gated
behind `livetrips.demo.seed-enabled=true`. It exists so a reviewer can obtain a token without an
identity provider. Disable it (set the flag false) for anything real; the verification and
authorisation paths it feeds are production-grade and fully tested.

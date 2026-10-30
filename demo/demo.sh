#!/usr/bin/env bash
# End-to-end demo against a running server (default: http://localhost:8080).
# Requires: curl, jq. Start the server first (see README / docs/DEMO.md).
#
#   bash demo/demo.sh
#
set -euo pipefail
BASE="${BASE:-http://localhost:8080}"

note() { printf "\n\033[1;36m== %s ==\033[0m\n" "$1"; }

token() {
  curl -s -X POST "$BASE/api/v1/dev/token" -H 'Content-Type: application/json' -d "$1" | jq -r .token
}

note "1. Mint tokens (demo endpoint; gated behind demo flag)"
OP_ACME=$(token '{"userId":"op-1","tenantId":"acme","role":"OPERATOR"}')
DEV7=$(token '{"userId":"dev-7","tenantId":"acme","role":"DEVICE","cabId":"cab-7"}')
EMP_ALICE=$(token '{"userId":"emp-alice","tenantId":"acme","role":"EMPLOYEE","tripIds":["trip-100"]}')
echo "acme operator, cab-7 device, and emp-alice tokens obtained."

note "2. Publish a LOCATION, STATUS and SOS for trip-100 (as the cab-7 device)"
for body in \
  '{"type":"LOCATION","lat":12.95,"lng":77.61}' \
  '{"type":"STATUS","status":"STARTED"}' \
  '{"type":"SOS","reasonCode":"PANIC"}' ; do
  echo -n "  -> $body : "
  curl -s -X POST "$BASE/api/v1/trips/trip-100/events" \
    -H "Authorization: Bearer $DEV7" -H 'Content-Type: application/json' -d "$body"
  echo ""
done

note "3. TENANT ISOLATION: acme device tries to publish to globex trip-200 (expect 403)"
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/api/v1/trips/trip-200/events" \
  -H "Authorization: Bearer $DEV7" -H 'Content-Type: application/json' \
  -d '{"type":"LOCATION","lat":1,"lng":1}')
echo "  HTTP $code  (403 = correctly denied across tenants)"

note "4. AUTHORIZATION: a DEVICE may not subscribe at all (expect it cannot read streams)"
echo "  (devices are producers only; see docs/CONCEPTS.md). Use the WebSocket client as emp-alice:"
echo ""
echo "  javac demo/WebSocketDemoClient.java -d /tmp/wsdemo"
echo "  java -cp /tmp/wsdemo WebSocketDemoClient ws://localhost:8080/ws/live \"$EMP_ALICE\" trip:trip-100 0"
echo ""
note "5. Metrics an on-call engineer would watch"
echo "  curl -s $BASE/actuator/prometheus | grep livetrips_ | head"
echo ""
echo "Done."

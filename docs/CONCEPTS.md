# Concepts, explained simply

This explains every hard idea in the case study in plain language, each with a concrete example
from *this* code. If you are preparing to defend the project, read this once end to end.

---

## The scenario in one paragraph

Thousands of cabs send their GPS position, trip status and occasional SOS alerts. Hundreds of
thousands of screens — security desks, admins, employees watching their own ride — want to see
those updates live. Our job is the layer in the middle: take each update and deliver it to exactly
the right screens, in the right order, fast, without losing the important ones, and without one
slow screen hurting the others. An SOS must arrive within 2 seconds, always.

---

## 1. Topic

**A named channel you subscribe to.** We have three kinds: per cab (`cab:cab-7`), per trip
(`trip:trip-100`), per office (`office:BLR-1`).

*Example:* emp-alice watches `trip:trip-100` (her ride). A security desk watches
`office:BLR-1` (everything in that building's fleet).

One update belongs to several topics at once. A location for cab-7 on trip-100 in office BLR-1 is
published to all three, so whoever is watching any of them sees it.

## 2. Sequence number

**A counter per topic that stamps each event 1, 2, 3, …** It is the single thing ordering, dedup
and resume all depend on.

*Example:* on `trip:trip-100`, the first location is seq 1, an SOS is seq 2, the next location
seq 3. A client that has seen up to seq 7 knows that seq 8 is the next thing it needs.

**Who assigns it?** The server, atomically, the instant it accepts the event — see
[ARCHITECTURE.md §3](ARCHITECTURE.md). Why atomically? Imagine two cabs… no, two updates for one
trip arriving at the same millisecond on two threads. If one grabbed "5" and the other "6" but they
saved themselves in the order 6-then-5, the stored history would be out of order and every client
that later replays it would see nonsense. So "grab the number" and "store the event" are a single
indivisible step (`EventLog.append`).

## 3. Per-key ordering

**Events for one trip always arrive in order.** "Key" here = topic (effectively, the trip).

*Example:* if the cab goes ASSIGNED → STARTED → DELAYED → COMPLETED, no client ever sees COMPLETED
before STARTED. Because the replay log is stored in sequence order and live events go out in
sequence order.

(The one subtlety: an SOS can *jump the queue* to be delivered faster — see Priority Lane — but it
still carries its true sequence number, so the client can always sort by sequence.)

## 4. At-least-once delivery + dedup

**"At-least-once"** = we would rather send something twice than risk sending it zero times.
**Dedup** = the client throws away anything it has already seen, so twice becomes once-in-effect.

*Example:* a client reconnects and we replay seq 8-10. But seq 10 was also already on its way live.
It receives 10 twice. `ClientSequenceTracker` remembers "last applied = 10" and silently drops the
duplicate. Net effect: exactly once.

Why not just do exactly-once on the server? Because true exactly-once across a network and a crash
is extremely expensive (distributed transactions). At-least-once + a tiny bit of client memory
gives the same user experience for almost nothing. This is a classic, deliberate trade.

## 5. Resume

**Reconnect and pick up exactly where you left off.**

*Example (the case study's own):* your phone goes into a lift for 40 seconds. Three status changes
and several locations happen while you're gone. You come back, reconnect, and say "I last saw seq
7." The server replays 8, 9, 10… — the three statuses and the latest location — and you're current.
No full reload, no missed status.

**But the replay buffer is bounded** (we keep only the last N events per topic — default 500). If
you were gone *so* long that the events you need were already thrown away, we can't replay them.
Instead we send a **snapshot**: "here is the current state — latest location of each cab, latest
status, any active SOS" — and then go live. You're current in one frame instead of a huge history.

## 6. Back-pressure

**A slow client must never slow down the others.**

*Example:* one security desk's browser freezes. Its network connection stops accepting data. If we
tried to push to it and waited, that wait could back up and stall everyone. Instead, each
connection has its own **bounded queue**. We only ever *drop events into* the queue (never wait on
the socket). The frozen desk's queue fills up to its bound and then stops accepting — the problem
stays contained to that one desk. Everyone else is unaffected.

Think of it like a mailbox with limited slots. The postman (our fan-out) drops mail in and walks
on; he never stands waiting for you to come read it.

## 7. Conflation

**For locations, keep only the latest — older positions are worthless.**

*Example:* the frozen desk missed 400 location updates for cab-7 while stuck. When it recovers, it
does not want 400 stale dots on a map; it wants cab-7's *current* position. So in the queue, a newer
location for a cab **replaces** the older one in place. 400 updates collapse to 1.

Crucially, **status changes and SOS are never conflated** — those you must see every one of. Only
locations, because for a dot on a map only the latest matters.

This also bounds memory beautifully: a lagging client's location backlog can never exceed "one per
cab it watches," no matter how far behind it falls.

## 8. Priority lane (SOS)

**SOS skips ahead of everything, everywhere.**

*Example:* the queue has 300 pending location updates and then an SOS arrives. We do not make the
SOS wait behind 300 locations. The queue has a separate **express lane** for SOS that is always
drained first. In Redis mode, SOS even travels on a separate channel with its own listener threads,
so a flood of location traffic can't queue ahead of it at the network hop either. This is how we
keep SOS under 2 seconds even when the system is busy.

## 9. Fan-out across gateway nodes

**With 200k screens, no single server holds them all** — connections are spread across many gateway
nodes. So when an event is published on one node, how does it reach a screen connected to a
*different* node?

*Example:* emp-alice is connected to gateway-1. Her cab's device publishes to gateway-2. The event
must cross from gateway-2 to gateway-1. We use **Redis pub/sub**: gateway-1, because it has a local
subscriber for `trip:trip-100`, is subscribed to the Redis channel `trip:trip-100`. gateway-2
publishes the event to that channel. Redis delivers it to every node subscribed — including
gateway-1, which hands it to alice. "Which nodes care about this topic" is just the set of Redis
channel subscribers; there is no separate directory to maintain.

## 10. Node-crash recovery

**A server dies; its users must recover without losing anything important.**

*Example:* gateway-1 (holding alice) crashes. Her socket drops. She reconnects — the load balancer
sends her to gateway-2. She says "I last saw seq 5." gateway-2 reads the shared Redis log and
replays 6, 7, 8 (including any status or SOS she missed). Nothing lost.

The key design choice that makes this work: the **sequence counter, replay log and snapshots live
in Redis, not inside a gateway's memory.** A gateway is stateless-ish — it holds connections, not
truth. Kill it and the truth is still in Redis for the next node to serve.

## 11. Tenant isolation

**One client company can never see another's data.** This is a security requirement, checked on
*subscribe*, not just at login.

*Example:* acme and globex are two customer companies. An acme operator — even a legitimate,
fully-authenticated one — trying to subscribe to a globex trip is denied, on the tenant boundary,
before any role check. There is a test named exactly for this (`employeeCannotSubscribeToAnother
TenantsTrip`), as the case study requires.

## 12. Caching with bounded staleness

**Remember hot lookups, but say exactly how stale they can get.**

*Example:* every subscribe asks "who owns trip-100?" (to authorize). That answer rarely changes, so
we cache it. But a cache can go stale — what if ownership changed? We bound staleness two ways: by
**time** (a TTL — an entry is re-read from the source after, say, 30s) and by **memory** (LRU — only
the most-recently-used N entries are kept). And we invalidate explicitly the moment ownership
changes. A stale entry can at worst delay a revocation by the TTL; it can **never** grant
cross-tenant access, because an entry is only ever filled from the authoritative record for that
same key.

---

### The one-sentence summary of each guarantee

| Guarantee | One sentence |
|---|---|
| Ordering | Per trip, events arrive 1,2,3… because they're stored and sent in sequence order. |
| Dedup | The client drops anything with a sequence ≤ what it already applied. |
| Resume | Reconnect with your last sequence; get a replay, or a snapshot if you were away too long. |
| Back-pressure | Each connection has a bounded queue; fan-out never waits on a socket. |
| Conflation | Only the latest location per cab is kept when you lag; status/SOS never dropped. |
| Priority | SOS has an express lane drained first, everywhere. |
| Fan-out | Redis pub/sub channels route an event to exactly the nodes that have subscribers. |
| Crash recovery | State lives in Redis, so reconnect-and-resume on another node loses nothing. |
| Tenant isolation | A tenant mismatch denies on subscribe, before any role check. |

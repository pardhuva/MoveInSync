package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * The per-connection bounded outbound queue &mdash; the mechanism that makes "a slow
 * consumer on a bad network never slows down other clients" true.
 *
 * <p>Every connection owns one of these. Fan-out {@link #offer(TripEvent) offers} into it
 * and never blocks; a dedicated per-connection writer {@link #poll() polls} and writes to
 * the socket. Because the publisher never waits on any one socket, one frozen browser cannot
 * back up the pipeline for everyone else.
 *
 * <h2>Three behaviours, by event type</h2>
 * <ul>
 *   <li><b>SOS &mdash; express lane.</b> Polled strictly first, so SOS skips ahead of all
 *       location traffic. Never dropped.</li>
 *   <li><b>STATUS / SNAPSHOT &mdash; critical, in order.</b> Kept in the main ordered lane in
 *       arrival order and never silently dropped. If the only way to admit a critical event
 *       would be to drop another critical event, the connection is declared
 *       {@link #isOverflowed() overflowed} instead &mdash; the gateway then disconnects it and
 *       the client resumes losslessly from the replay buffer.</li>
 *   <li><b>LOCATION &mdash; conflated in place.</b> At most one pending location per cab: a newer
 *       position for a cab replaces the older one <i>in its existing position</i>, so ordering
 *       relative to status events is preserved while a lagging consumer only ever sees the
 *       latest location per cab.</li>
 * </ul>
 *
 * <h2>Ordering guarantee &mdash; precise statement</h2>
 * STATUS and LOCATION events are delivered in arrival (sequence) order, with location
 * latest-wins in place. The single, deliberate exception is the SOS express lane, which is
 * the behaviour the case study explicitly asks for ("SOS events skip ahead of location traffic
 * everywhere"). The authoritative, fully-ordered log is the replay buffer; clients reconcile
 * using the per-topic sequence number.
 *
 * <h2>Memory cost per connection</h2>
 * Bounded by {@code capacity} entries. Location entries collapse to at most one per cab, so the
 * steady-state size is O(distinct cabs the connection watches). At ~200 bytes/event a
 * capacity of 1,000 is ~200 KB worst case per connection.
 *
 * <p>Not thread-safe-free: all mutating methods are {@code synchronized}. Contention is low in
 * practice (one offering thread, one polling thread per connection).
 */
public class OutboundQueue {

    /** Outcome of an {@link #offer}. */
    public enum OfferOutcome {
        /** Added as a new pending entry. */
        ENQUEUED,
        /** Replaced an older pending location for the same cab (conflation). */
        CONFLATED,
        /** A location was dropped because the queue was full of critical events (locations are droppable). */
        DROPPED_LOCATION,
        /** Queue cannot admit this critical event without losing another; connection must be closed. */
        REJECTED_OVERFLOW
    }

    private final int capacity;

    /** Express lane for SOS. Polled first. */
    private final Deque<TripEvent> sos = new ArrayDeque<>();

    /** Main ordered lane: status + conflatable locations, keyed for in-place conflation. */
    private final LinkedHashMap<Object, TripEvent> main = new LinkedHashMap<>();

    /** Insertion-ordered subset of {@code main} keys that are LOCATION, for O(1) oldest-location eviction. */
    private final LinkedHashSet<Object> locationKeys = new LinkedHashSet<>();

    private long criticalKeySeq = 0;
    private boolean overflowed = false;

    // Observability counters (read by metrics).
    private long totalEnqueued = 0;
    private long totalConflated = 0;
    private long totalDroppedLocations = 0;
    private long totalDelivered = 0;

    public OutboundQueue(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
    }

    /**
     * Offer an event to this connection. Never blocks. Returns how the event was handled.
     * Once {@link #isOverflowed()} is true, every offer is rejected.
     */
    public synchronized OfferOutcome offer(TripEvent event) {
        if (overflowed) {
            return OfferOutcome.REJECTED_OVERFLOW;
        }
        EventType type = event.type();
        if (type == EventType.SOS) {
            if (sos.size() >= capacity) {
                // Cannot drop an SOS; the consumer is hopelessly stuck.
                overflowed = true;
                return OfferOutcome.REJECTED_OVERFLOW;
            }
            sos.addLast(event);
            totalEnqueued++;
            return OfferOutcome.ENQUEUED;
        }

        if (type.isConflatable()) { // LOCATION
            Object key = event.conflationKey();
            if (main.containsKey(key)) {
                main.put(key, event); // replace value, keep position
                totalConflated++;
                return OfferOutcome.CONFLATED;
            }
            if (main.size() >= capacity) {
                if (!evictOldestLocation()) {
                    // Main lane is entirely critical events; this new location is droppable.
                    totalDroppedLocations++;
                    return OfferOutcome.DROPPED_LOCATION;
                }
            }
            main.put(key, event);
            locationKeys.add(key);
            totalEnqueued++;
            return OfferOutcome.ENQUEUED;
        }

        // STATUS / SNAPSHOT: critical, never silently dropped.
        if (main.size() >= capacity) {
            if (!evictOldestLocation()) {
                // No droppable location to make room: declare overflow rather than lose a critical event.
                overflowed = true;
                return OfferOutcome.REJECTED_OVERFLOW;
            }
        }
        main.put("c:" + (criticalKeySeq++), event);
        totalEnqueued++;
        return OfferOutcome.ENQUEUED;
    }

    private boolean evictOldestLocation() {
        Iterator<Object> it = locationKeys.iterator();
        if (!it.hasNext()) {
            return false;
        }
        Object oldest = it.next();
        it.remove();
        main.remove(oldest);
        totalDroppedLocations++;
        return true;
    }

    /**
     * Remove and return the next event to send, honouring priority:
     * SOS first, then the main lane in order. Returns {@code null} if empty.
     */
    public synchronized TripEvent poll() {
        if (!sos.isEmpty()) {
            totalDelivered++;
            return sos.pollFirst();
        }
        Iterator<Map.Entry<Object, TripEvent>> it = main.entrySet().iterator();
        if (it.hasNext()) {
            Map.Entry<Object, TripEvent> e = it.next();
            it.remove();
            if (e.getValue().type().isConflatable()) {
                locationKeys.remove(e.getKey());
            }
            totalDelivered++;
            return e.getValue();
        }
        return null;
    }

    public synchronized int size() {
        return sos.size() + main.size();
    }

    public synchronized boolean isEmpty() {
        return sos.isEmpty() && main.isEmpty();
    }

    public synchronized boolean isOverflowed() {
        return overflowed;
    }

    public int capacity() {
        return capacity;
    }

    public synchronized long totalEnqueued() { return totalEnqueued; }
    public synchronized long totalConflated() { return totalConflated; }
    public synchronized long totalDroppedLocations() { return totalDroppedLocations; }
    public synchronized long totalDelivered() { return totalDelivered; }
}

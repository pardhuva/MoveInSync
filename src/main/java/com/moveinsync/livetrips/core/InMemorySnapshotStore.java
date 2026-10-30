package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node {@link SnapshotStore}.
 *
 * <p><b>Complexity.</b> {@code record} is O(1) amortised; {@code snapshot} is O(cabs + trips in
 * topic); {@code criticalEventsAfter} is O(missed criticals). All paid only on the resume path.
 *
 * <p><b>Space.</b> Per topic: one entry per cab (latest location) + one per trip (latest status)
 * + active SOS + a bounded ordered critical history ({@code criticalCapacity} STATUS/SOS events,
 * independent of the location-dominated replay window). This is the deliberate cost of making
 * cold resume cheap <i>and</i> lossless for criticals down to the critical-history horizon.
 */
public class InMemorySnapshotStore implements SnapshotStore {

    /** Default bound on the per-topic ordered STATUS/SOS history. */
    public static final int DEFAULT_CRITICAL_CAPACITY = 256;

    private final int criticalCapacity;
    private final ConcurrentHashMap<String, TopicState> states = new ConcurrentHashMap<>();

    public InMemorySnapshotStore() {
        this(DEFAULT_CRITICAL_CAPACITY);
    }

    public InMemorySnapshotStore(int criticalCapacity) {
        if (criticalCapacity < 1) {
            throw new IllegalArgumentException("criticalCapacity must be >= 1");
        }
        this.criticalCapacity = criticalCapacity;
    }

    @Override
    public void record(TripEvent event) {
        states.computeIfAbsent(event.topic(), k -> new TopicState(criticalCapacity)).record(event);
    }

    @Override
    public TopicSnapshot snapshot(String topic) {
        TopicState st = states.get(topic);
        if (st == null) {
            return new TopicSnapshot(topic, 0L, Map.of(), Map.of(), List.of());
        }
        return st.toSnapshot(topic);
    }

    @Override
    public CriticalHistory criticalEventsAfter(String topic, long lastSeq) {
        TopicState st = states.get(topic);
        if (st == null) {
            return CriticalHistory.empty();
        }
        return st.criticalEventsAfter(lastSeq);
    }

    @Override
    public void clearSos(String topic, String eventId) {
        TopicState st = states.get(topic);
        if (st != null) {
            st.clearSos(eventId);
        }
    }

    private static final class TopicState {
        private final int criticalCapacity;
        private final Map<String, TripEvent> latestLocationByCab = new LinkedHashMap<>();
        private final Map<String, TripEvent> latestStatusByTrip = new LinkedHashMap<>();
        /**
         * Unresolved SOS, keyed by eventId.
         *
         * <p><b>Known limitation: this map only grows.</b> {@link #clearSos} is the intended
         * resolution hook but nothing calls it yet (there is no "SOS acknowledged" flow), so in
         * single-node mode an entry lives for the life of the process. The practical size is small
         * (SOS are rare; a trip topic holds a handful) and an office topic is the only one that
         * accumulates. Redis mode bounds it via the 24h topic TTL. Eviction is deliberately NOT
         * implemented: discarding an unresolved SOS would break "SOS is never dropped" for a client
         * resuming far behind. Recent SOS are additionally carried by the bounded critical history,
         * so resume does not depend on this map alone.
         */
        private final Map<String, TripEvent> activeSos = new LinkedHashMap<>();
        /** Ordered STATUS/SOS history, keyed by sequence, bounded to {@code criticalCapacity}. */
        private final TreeMap<Long, TripEvent> criticalBySeq = new TreeMap<>();
        /** Highest sequence of a critical event that has been evicted (0 = none). */
        private long highestEvictedCriticalSeq = 0;

        TopicState(int criticalCapacity) {
            this.criticalCapacity = criticalCapacity;
        }
        private long latestSequence;

        synchronized void record(TripEvent e) {
            if (e.sequence() > latestSequence) {
                latestSequence = e.sequence();
            }
            switch (e.type()) {
                // LOCATION/STATUS keep the latest per key. Updates are made sequence-monotonic so
                // record() is commutative: if events arrive out of sequence order (which the
                // off-lock dispatch path allows), an older event can never overwrite a newer one.
                case LOCATION -> {
                    if (e.cabId() != null) {
                        putIfNewer(latestLocationByCab, e.cabId(), e);
                    }
                }
                case STATUS -> {
                    if (e.tripId() != null) {
                        putIfNewer(latestStatusByTrip, e.tripId(), e);
                    }
                }
                // SOS is keyed by its unique eventId, so there is no latest-wins race to guard.
                case SOS -> activeSos.put(e.eventId(), e);
                case SNAPSHOT -> { /* derived; never folded back in */ }
            }
            // STATUS and SOS also go into the ordered, bounded critical history so they can be
            // replayed on the snapshot path even after the location-dominated window has been evicted.
            if (e.type() == EventType.STATUS || e.type() == EventType.SOS) {
                recordCritical(e);
            }
        }

        private void recordCritical(TripEvent e) {
            // Ignore a late duplicate we have already evicted (its slot is gone for good).
            if (e.sequence() <= highestEvictedCriticalSeq) {
                return;
            }
            criticalBySeq.put(e.sequence(), e);
            while (criticalBySeq.size() > criticalCapacity) {
                Long evicted = criticalBySeq.firstKey();
                criticalBySeq.remove(evicted);
                if (evicted > highestEvictedCriticalSeq) {
                    highestEvictedCriticalSeq = evicted;
                }
            }
        }

        synchronized CriticalHistory criticalEventsAfter(long lastSeq) {
            List<TripEvent> missed = new ArrayList<>(criticalBySeq.tailMap(lastSeq, false).values());
            // Truncated if at least one critical with sequence > lastSeq was already evicted.
            boolean truncated = highestEvictedCriticalSeq > lastSeq;
            return CriticalHistory.of(missed, truncated);
        }

        private static void putIfNewer(Map<String, TripEvent> map, String key, TripEvent e) {
            TripEvent existing = map.get(key);
            if (existing == null || e.sequence() > existing.sequence()) {
                map.put(key, e);
            }
        }

        synchronized void clearSos(String eventId) {
            activeSos.remove(eventId);
        }

        synchronized TopicSnapshot toSnapshot(String topic) {
            return new TopicSnapshot(topic, latestSequence,
                    new LinkedHashMap<>(latestLocationByCab),
                    new LinkedHashMap<>(latestStatusByTrip),
                    new ArrayList<>(activeSos.values()));
        }
    }
}

package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node {@link SnapshotStore}.
 *
 * <p><b>Complexity.</b> {@code record} is O(1); {@code snapshot} is O(cabs + trips in topic),
 * paid only on the rare resume-after-eviction path.
 *
 * <p><b>Space.</b> Per topic: one entry per cab (latest location) + one per trip (latest status)
 * + active SOS alerts. For a {@code trip:} topic that is a couple of entries; for an
 * {@code office:} topic it is O(cabs in the office). This is the deliberate cost of making
 * cold resume cheap.
 */
public class InMemorySnapshotStore implements SnapshotStore {

    private final ConcurrentHashMap<String, TopicState> states = new ConcurrentHashMap<>();

    @Override
    public void record(TripEvent event) {
        states.computeIfAbsent(event.topic(), k -> new TopicState()).record(event);
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
    public void clearSos(String topic, String eventId) {
        TopicState st = states.get(topic);
        if (st != null) {
            st.clearSos(eventId);
        }
    }

    private static final class TopicState {
        private final Map<String, TripEvent> latestLocationByCab = new LinkedHashMap<>();
        private final Map<String, TripEvent> latestStatusByTrip = new LinkedHashMap<>();
        private final Map<String, TripEvent> activeSos = new LinkedHashMap<>();
        private long latestSequence;

        synchronized void record(TripEvent e) {
            if (e.sequence() > latestSequence) {
                latestSequence = e.sequence();
            }
            switch (e.type()) {
                case LOCATION -> {
                    if (e.cabId() != null) {
                        latestLocationByCab.put(e.cabId(), e);
                    }
                }
                case STATUS -> {
                    if (e.tripId() != null) {
                        latestStatusByTrip.put(e.tripId(), e);
                    }
                }
                case SOS -> activeSos.put(e.eventId(), e);
                case SNAPSHOT -> { /* derived; never folded back in */ }
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

package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node {@link ReplayBuffer}: a bounded ring (an {@link ArrayDeque} capped at
 * {@code capacity}) per topic.
 *
 * <p><b>Complexity.</b> {@code append} is amortised O(1). {@code replayFrom} is O(window)
 * where window = number of events after the client's last sequence (&le; capacity).
 * Space is O(topics &times; capacity) events.
 *
 * <p><b>Memory cost.</b> With capacity C per topic and T active topics, worst-case retained
 * events = C &times; T. At ~200 bytes per event that is the replay memory budget; it is
 * documented and configurable so an operator can trade resume depth against RAM.
 */
public class InMemoryReplayBuffer implements ReplayBuffer {

    private final int capacity;
    private final ConcurrentHashMap<String, TopicRing> rings = new ConcurrentHashMap<>();

    public InMemoryReplayBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
    }

    @Override
    public void append(TripEvent event) {
        rings.computeIfAbsent(event.topic(), k -> new TopicRing(capacity)).append(event);
    }

    @Override
    public ReplayResult replayFrom(String topic, long lastSequence) {
        TopicRing ring = rings.get(topic);
        if (ring == null) {
            // Nothing ever published on this topic: nothing to replay, caller is up to date.
            return ReplayResult.replay(List.of(), 0L);
        }
        return ring.replayFrom(lastSequence);
    }

    @Override
    public int capacity() {
        return capacity;
    }

    /** Per-topic bounded ring. All access guarded by the instance monitor. */
    private static final class TopicRing {
        private final int cap;
        private final Deque<TripEvent> events;
        private long latestSeq;

        TopicRing(int cap) {
            this.cap = cap;
            this.events = new ArrayDeque<>(Math.min(cap, 1024));
        }

        synchronized void append(TripEvent e) {
            events.addLast(e);
            latestSeq = e.sequence();
            if (events.size() > cap) {
                events.removeFirst();
            }
        }

        synchronized ReplayResult replayFrom(long lastSequence) {
            if (events.isEmpty()) {
                return ReplayResult.replay(List.of(), latestSeq);
            }
            long oldestRetained = events.peekFirst().sequence();

            // Client is already current (or ahead): nothing to send.
            if (lastSequence >= latestSeq) {
                return ReplayResult.replay(List.of(), latestSeq);
            }
            // The first event the client still needs is lastSequence + 1. If that is older
            // than what we retain, the client fell outside the window -> snapshot required.
            if (lastSequence + 1 < oldestRetained) {
                return ReplayResult.snapshotRequired(latestSeq);
            }
            List<TripEvent> missed = new ArrayList<>();
            for (TripEvent e : events) {
                if (e.sequence() > lastSequence) {
                    missed.add(e);
                }
            }
            return ReplayResult.replay(missed, latestSeq);
        }
    }
}

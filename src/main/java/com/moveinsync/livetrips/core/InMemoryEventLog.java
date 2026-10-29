package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node {@link EventLog}: a bounded ring per topic, with sequence assignment and append
 * performed under the same per-topic lock.
 *
 * <h2>Concurrency</h2>
 * Locking is per topic (the {@code TopicLog} instance monitor), not global, so publishers on
 * different trips never contend. Within a topic the critical section is a counter increment plus
 * a deque push &mdash; a few nanoseconds &mdash; so even 20,000 cabs publishing once per second
 * contend negligibly. The important property is that the sequence a publisher receives and the
 * position it occupies in the log are decided together, so the log is always sorted by sequence.
 *
 * <h2>Complexity</h2>
 * {@code append}: O(1) amortised. {@code replayFrom}: O(window), window &le; capacity.
 * Space: O(active topics &times; capacity).
 */
public class InMemoryEventLog implements EventLog {

    private final int capacity;
    private final ConcurrentHashMap<String, TopicLog> logs = new ConcurrentHashMap<>();

    public InMemoryEventLog(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
    }

    @Override
    public TripEvent append(TripEvent event) {
        return logs.computeIfAbsent(event.topic(), k -> new TopicLog(capacity)).append(event);
    }

    @Override
    public ReplayResult replayFrom(String topic, long lastSequence) {
        TopicLog log = logs.get(topic);
        if (log == null) {
            return ReplayResult.replay(List.of(), 0L);
        }
        return log.replayFrom(lastSequence);
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public long currentSequence(String topic) {
        TopicLog log = logs.get(topic);
        return log == null ? 0L : log.currentSequence();
    }

    @Override
    public void deleteTopic(String topic) {
        logs.remove(topic);
    }

    /** Number of topics currently holding state. Exposed for metrics and tests. */
    public int topicCount() {
        return logs.size();
    }

    private static final class TopicLog {
        private final int cap;
        private final Deque<TripEvent> events;
        private long nextSeq = 1;

        TopicLog(int cap) {
            this.cap = cap;
            this.events = new ArrayDeque<>(Math.min(cap, 1024));
        }

        /** Sequence assignment and append in one atomic step. */
        synchronized TripEvent append(TripEvent event) {
            TripEvent sequenced = event.withSequence(nextSeq++);
            events.addLast(sequenced);
            if (events.size() > cap) {
                events.removeFirst();
            }
            return sequenced;
        }

        synchronized long currentSequence() {
            return nextSeq - 1;
        }

        synchronized ReplayResult replayFrom(long lastSequence) {
            long latest = nextSeq - 1;
            // The client claims a sequence this log never issued: its node's state was lost or the
            // token is stale. Re-seed it from a snapshot rather than silently deliver nothing.
            if (lastSequence > latest) {
                return ReplayResult.snapshotRequired(latest);
            }
            if (events.isEmpty() || lastSequence == latest) {
                return ReplayResult.replay(List.of(), latest);
            }
            long oldestRetained = events.peekFirst().sequence();
            if (lastSequence + 1 < oldestRetained) {
                return ReplayResult.snapshotRequired(latest);
            }
            List<TripEvent> missed = new ArrayList<>();
            for (TripEvent e : events) {
                if (e.sequence() > lastSequence) {
                    missed.add(e);
                }
            }
            return ReplayResult.replay(missed, latest);
        }
    }
}

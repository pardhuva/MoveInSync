package com.moveinsync.livetrips.core;

import java.util.HashMap;
import java.util.Map;

/**
 * The client half of the at-least-once contract: tracks the last sequence seen per topic,
 * drops duplicates, and reports gaps.
 *
 * <p>The server guarantees <i>at-least-once</i> delivery (a redelivery after a reconnect may
 * repeat events the client already applied). Exactly-once <i>effect</i> is achieved here, on the
 * client, by discarding any event whose sequence is not greater than the last one applied.
 * That is why every event carries a per-topic sequence number.
 *
 * <p>This class lives in the server codebase because the reference client, the integration
 * tests and the load generator all use it &mdash; it is the canonical, testable statement of the
 * dedup rule.
 *
 * <p>Not thread-safe; intended to be owned by a single client connection.
 */
public class ClientSequenceTracker {

    public enum Verdict {
        /** In-order, not seen before: apply it. */
        ACCEPT,
        /** Sequence <= last applied: a redelivery. Drop it. */
        DUPLICATE,
        /** Sequence jumped forward: applied, but events were missed (ask for a resume). */
        GAP
    }

    private final Map<String, Long> lastSeenByTopic = new HashMap<>();

    /** Decide what to do with an incoming event, updating state when it is applied. */
    public Verdict offer(String topic, long sequence) {
        long last = lastSeenByTopic.getOrDefault(topic, 0L);
        if (sequence <= last) {
            return Verdict.DUPLICATE;
        }
        lastSeenByTopic.put(topic, sequence);
        return sequence == last + 1 ? Verdict.ACCEPT : Verdict.GAP;
    }

    /**
     * Called after applying a server snapshot: the client is now current as of
     * {@code atSequence}, so everything at or below it is a duplicate.
     */
    public void applySnapshot(String topic, long atSequence) {
        lastSeenByTopic.merge(topic, atSequence, Math::max);
    }

    /** The resume token for a topic: the highest sequence successfully applied (0 = nothing). */
    public long lastSeen(String topic) {
        return lastSeenByTopic.getOrDefault(topic, 0L);
    }

    public void forget(String topic) {
        lastSeenByTopic.remove(topic);
    }

    public Map<String, Long> resumeTokens() {
        return Map.copyOf(lastSeenByTopic);
    }
}

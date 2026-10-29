package com.moveinsync.livetrips.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-node {@link Sequencer}: one {@link AtomicLong} per topic.
 *
 * <p>Correctness under concurrency: {@code incrementAndGet()} is a lock-free atomic
 * read-modify-write, so N threads publishing to the same topic get N distinct,
 * strictly increasing numbers with no gaps and no duplicates.
 *
 * <p>Complexity: O(1) time per call; O(number of distinct topics) space.
 */
public class InMemorySequencer implements Sequencer {

    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    @Override
    public long next(String topic) {
        return counters.computeIfAbsent(topic, k -> new AtomicLong(0)).incrementAndGet();
    }

    @Override
    public long current(String topic) {
        AtomicLong c = counters.get(topic);
        return c == null ? 0L : c.get();
    }
}

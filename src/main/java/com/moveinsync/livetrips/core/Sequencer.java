package com.moveinsync.livetrips.core;

/**
 * Assigns the per-topic sequence number that everything else depends on
 * (ordering, dedup and resume).
 *
 * <p><b>Who assigns it and where (a key question in the case study):</b> the server
 * assigns it, at the single logical point where an event is accepted for a topic,
 * <i>before</i> fan-out. In single-node mode this is an {@code AtomicLong} per topic
 * ({@link InMemorySequencer}); in multi-node mode it is a Redis {@code INCR} executed
 * atomically with the append to the replay buffer (see {@code RedisSequencer}). In both
 * cases the assignment is a single atomic step, so concurrent publishers to the same
 * topic always receive distinct, gap-free, monotonically increasing numbers.
 *
 * <p>Sequences start at 1. A client's "last seen sequence" of 0 means "I have seen nothing".
 */
public interface Sequencer {

    /**
     * Atomically returns the next sequence number for {@code topic}.
     * Thread-safe; concurrent callers receive distinct increasing values.
     */
    long next(String topic);

    /** The current (last-assigned) sequence for a topic, or 0 if none assigned yet. Read-only. */
    long current(String topic);
}

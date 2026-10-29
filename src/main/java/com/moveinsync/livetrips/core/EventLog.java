package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;

/**
 * The append-only, per-topic, bounded log that ordering, dedup and resume are all built on.
 *
 * <h2>Why sequencing and appending are one operation</h2>
 * It is tempting to call a {@link Sequencer} and then a {@link ReplayBuffer} separately. That is
 * a race: two concurrent publishers on the same topic could take sequences 5 and 6 and then
 * append in the order 6, 5, leaving the replay buffer not sorted by sequence &mdash; which silently
 * breaks per-key ordering for every client that later resumes. So the two steps are exposed as a
 * single atomic {@link #append} and each implementation must make them atomic:
 *
 * <ul>
 *   <li>{@link InMemoryEventLog} holds a short per-topic lock around increment + append.</li>
 *   <li>{@code RedisEventLog} runs {@code INCR} + {@code RPUSH} + {@code LTRIM} inside one Lua
 *       script, which Redis executes atomically.</li>
 * </ul>
 */
public interface EventLog {

    /**
     * Atomically assign the next per-topic sequence to {@code event}, append it to the bounded
     * replay window, and return the sequenced event. The returned event is what gets fanned out.
     *
     * @param event an event with its sequence not yet assigned
     * @return the same event with {@link TripEvent#sequence()} set
     */
    TripEvent append(TripEvent event);

    /** See {@link ReplayBuffer#replayFrom}. */
    ReplayResult replayFrom(String topic, long lastSequence);

    /** The per-topic replay window size. */
    int capacity();

    /** The last sequence assigned for a topic (0 if none). */
    long currentSequence(String topic);

    /** Drop all state for a finished topic (e.g. when a trip completes), reclaiming memory. */
    void deleteTopic(String topic);
}

package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;

/**
 * A bounded, per-topic buffer of the most recent events, used to let a reconnecting
 * client resume from its last sequence number.
 *
 * <p><b>Bounded by design.</b> Each topic keeps at most {@code capacity} recent events.
 * When the buffer is full the oldest event is evicted. A client that asks to resume from
 * a sequence older than the oldest retained event cannot be served from the buffer and is
 * told a snapshot is required &mdash; this is the explicit "replay window overflow" behaviour.
 */
public interface ReplayBuffer {

    /** Store an already-sequenced event. Evicts the oldest if the topic is at capacity. */
    void append(TripEvent event);

    /**
     * Return everything after {@code lastSequence} for the topic, or signal that a snapshot
     * is required because the needed events have been evicted.
     *
     * @param topic        the topic key
     * @param lastSequence the highest sequence the client has already seen (0 = seen nothing)
     */
    ReplayResult replayFrom(String topic, long lastSequence);

    /** Per-topic retained-event capacity (the replay window size). */
    int capacity();
}

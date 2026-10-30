package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;

/**
 * Maintains the latest-state ("materialised view") per topic so a client that falls outside
 * the bounded replay window can be brought up to date with one snapshot instead of unbounded
 * history.
 *
 * <p>This is the second half of the resume contract: replay buffer for the recent past,
 * snapshot for everything older.
 */
public interface SnapshotStore {

    /** Fold an event into the topic's latest state. Called once per event, after sequencing. */
    void record(TripEvent event);

    /** Current state of the topic. Never null; an unknown topic yields an empty snapshot at sequence 0. */
    TopicSnapshot snapshot(String topic);

    /**
     * The STATUS/SOS events after {@code lastSeq} that are still retained in the bounded critical
     * history, in ascending sequence order, plus a {@code truncated} flag when older criticals the
     * client needed were already evicted.
     *
     * <p>This is independent of the location-dominated replay window: a location flood can evict
     * the replay window, but critical events live in their own bounded history, so the snapshot path
     * can still replay the STATUS/SOS a client missed rather than conveying only the latest status.
     */
    CriticalHistory criticalEventsAfter(String topic, long lastSeq);

    /** Mark an SOS as resolved so it stops appearing in snapshots. */
    void clearSos(String topic, String eventId);
}

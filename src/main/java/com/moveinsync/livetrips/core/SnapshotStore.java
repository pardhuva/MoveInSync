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

    /** Mark an SOS as resolved so it stops appearing in snapshots. */
    void clearSos(String topic, String eventId);
}

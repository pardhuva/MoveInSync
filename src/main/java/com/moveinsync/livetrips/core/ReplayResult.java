package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;

import java.util.Collections;
import java.util.List;

/**
 * The outcome of a resume request against the bounded replay buffer.
 *
 * <ul>
 *   <li>{@link Kind#REPLAY} &mdash; every missed event was still inside the bounded window;
 *       {@link #events()} holds them in ascending sequence order.</li>
 *   <li>{@link Kind#SNAPSHOT_REQUIRED} &mdash; the client was away too long and the events it
 *       needs have already been evicted. The caller must send a fresh snapshot followed by
 *       live events. This is exactly the "if it was away too long, it gets a fresh snapshot"
 *       branch from the case study.</li>
 * </ul>
 */
public final class ReplayResult {

    public enum Kind { REPLAY, SNAPSHOT_REQUIRED }

    private final Kind kind;
    private final List<TripEvent> events;
    private final long latestSequence;

    private ReplayResult(Kind kind, List<TripEvent> events, long latestSequence) {
        this.kind = kind;
        this.events = events;
        this.latestSequence = latestSequence;
    }

    public static ReplayResult replay(List<TripEvent> events, long latestSequence) {
        return new ReplayResult(Kind.REPLAY, List.copyOf(events), latestSequence);
    }

    public static ReplayResult snapshotRequired(long latestSequence) {
        return new ReplayResult(Kind.SNAPSHOT_REQUIRED, Collections.emptyList(), latestSequence);
    }

    public Kind kind() { return kind; }
    public List<TripEvent> events() { return events; }
    public long latestSequence() { return latestSequence; }

    public boolean isSnapshotRequired() { return kind == Kind.SNAPSHOT_REQUIRED; }
}

package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;

import java.util.List;

/**
 * The missed STATUS/SOS events a resuming client can still be given on the snapshot path, plus a
 * flag saying whether older critical events were already evicted (so the caller/client knows the
 * recovery of intermediate criticals is incomplete and only the snapshot's latest state is
 * authoritative for what is missing).
 *
 * <p>{@code events} are in ascending sequence order and all have {@code sequence > lastSeq}.
 * {@code truncated} is true when at least one critical event with {@code sequence > lastSeq} had
 * already been evicted from the bounded critical history &mdash; i.e. the critical history itself
 * overflowed for this client's horizon.
 */
public record CriticalHistory(List<TripEvent> events, boolean truncated) {

    public static CriticalHistory of(List<TripEvent> events, boolean truncated) {
        return new CriticalHistory(List.copyOf(events), truncated);
    }

    public static CriticalHistory empty() {
        return new CriticalHistory(List.of(), false);
    }
}

package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.moveinsync.livetrips.support.Events.rawLocation;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Resume semantics against the bounded replay window: in-window resume replays exactly the missed
 * events; out-of-window resume demands a snapshot; a current client gets nothing.
 */
class ReplayBufferTest {

    private static final String T = "trip:1";

    @Test
    @DisplayName("Reconnect within the window replays exactly the missed events, in order")
    void replaysMissedEventsInWindow() {
        InMemoryEventLog log = new InMemoryEventLog(100);
        for (int i = 0; i < 10; i++) {
            log.append(rawLocation(T, "cab-1"));
        }
        // Client last saw sequence 7; it should get 8, 9, 10.
        ReplayResult r = log.replayFrom(T, 7);
        assertThat(r.kind()).isEqualTo(ReplayResult.Kind.REPLAY);
        assertThat(r.events()).extracting(TripEvent::sequence).containsExactly(8L, 9L, 10L);
        assertThat(r.latestSequence()).isEqualTo(10L);
    }

    @Test
    @DisplayName("A client that fell outside the window is told a snapshot is required")
    void snapshotRequiredWhenOutsideWindow() {
        InMemoryEventLog log = new InMemoryEventLog(5); // only last 5 retained
        for (int i = 0; i < 20; i++) {
            log.append(rawLocation(T, "cab-1"));
        }
        // Client last saw 3, but only 16..20 are retained -> the gap (4..15) is lost.
        ReplayResult r = log.replayFrom(T, 3);
        assertThat(r.isSnapshotRequired()).isTrue();
        assertThat(r.latestSequence()).isEqualTo(20L);
    }

    @Test
    @DisplayName("Boundary: resume from exactly the oldest retained minus one still replays")
    void boundaryExactlyInWindow() {
        InMemoryEventLog log = new InMemoryEventLog(5);
        for (int i = 0; i < 20; i++) {
            log.append(rawLocation(T, "cab-1"));
        }
        // Retained 16..20; last-seen 15 means the next needed is 16, which is present.
        ReplayResult r = log.replayFrom(T, 15);
        assertThat(r.kind()).isEqualTo(ReplayResult.Kind.REPLAY);
        assertThat(r.events()).extracting(TripEvent::sequence).containsExactly(16L, 17L, 18L, 19L, 20L);
    }

    @Test
    @DisplayName("A fully caught-up client receives no replay")
    void caughtUpClientGetsNothing() {
        InMemoryEventLog log = new InMemoryEventLog(100);
        for (int i = 0; i < 10; i++) {
            log.append(rawLocation(T, "cab-1"));
        }
        ReplayResult r = log.replayFrom(T, 10);
        assertThat(r.kind()).isEqualTo(ReplayResult.Kind.REPLAY);
        assertThat(r.events()).isEmpty();
    }

    @Test
    @DisplayName("A never-seen topic yields an empty replay, not an error")
    void unknownTopicIsEmpty() {
        InMemoryEventLog log = new InMemoryEventLog(100);
        ReplayResult r = log.replayFrom("trip:never", 0);
        assertThat(r.events()).isEmpty();
        assertThat(r.isSnapshotRequired()).isFalse();
    }
}

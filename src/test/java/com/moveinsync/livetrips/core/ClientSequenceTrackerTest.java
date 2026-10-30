package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.core.ClientSequenceTracker.Verdict;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Client-side dedup: the server guarantees at-least-once, so the client turns that into
 * exactly-once effect by dropping anything it has already applied. This is the rule every real
 * client, the test suite and the load generator share.
 */
class ClientSequenceTrackerTest {

    private static final String T = "trip:1";

    @Test
    @DisplayName("In-order events are accepted; a redelivery of the same sequence is a duplicate")
    void acceptsInOrderAndDropsDuplicates() {
        ClientSequenceTracker tracker = new ClientSequenceTracker();
        assertThat(tracker.offer(T, 1)).isEqualTo(Verdict.ACCEPT);
        assertThat(tracker.offer(T, 2)).isEqualTo(Verdict.ACCEPT);
        // At-least-once redelivery of 2 and even an older 1:
        assertThat(tracker.offer(T, 2)).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.offer(T, 1)).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.lastSeen(T)).isEqualTo(2);
    }

    @Test
    @DisplayName("A forward jump is flagged as a gap so the client knows to resume")
    void detectsGap() {
        ClientSequenceTracker tracker = new ClientSequenceTracker();
        tracker.offer(T, 1);
        assertThat(tracker.offer(T, 5)).isEqualTo(Verdict.GAP);
        // State advances to 5 so subsequent in-order events continue normally.
        assertThat(tracker.offer(T, 6)).isEqualTo(Verdict.ACCEPT);
    }

    @Test
    @DisplayName("After a snapshot, everything up to the snapshot sequence is a duplicate")
    void snapshotResetsBaseline() {
        ClientSequenceTracker tracker = new ClientSequenceTracker();
        tracker.applySnapshot(T, 100);
        assertThat(tracker.offer(T, 50)).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.offer(T, 100)).isEqualTo(Verdict.DUPLICATE);
        assertThat(tracker.offer(T, 101)).isEqualTo(Verdict.ACCEPT);
    }

    @Test
    @DisplayName("Dedup state is per topic")
    void perTopicIndependence() {
        ClientSequenceTracker tracker = new ClientSequenceTracker();
        tracker.offer("trip:A", 10);
        assertThat(tracker.offer("trip:B", 1)).isEqualTo(Verdict.ACCEPT);
        assertThat(tracker.resumeTokens()).containsEntry("trip:A", 10L).containsEntry("trip:B", 1L);
    }
}

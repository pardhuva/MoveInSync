package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The snapshot store keeps the latest LOCATION per cab and latest STATUS per trip. Because
 * {@code record} runs off any ordering lock (and, in Redis mode, from more than one node), it must
 * be <b>sequence-monotonic</b>: an event delivered out of sequence order must never overwrite a
 * newer one. This test records events out of order and asserts the snapshot reflects the highest
 * sequence per key.
 */
class SnapshotRecordMonotonicTest {

    private static final String TOPIC = "trip:1";

    private TripEvent loc(long seq, String cabId, double lat) {
        return TripEvent.builder().eventId(UUID.randomUUID().toString())
                .topic(TOPIC).sequence(seq).type(EventType.LOCATION)
                .tripId("trip-1").cabId(cabId).officeId("office-1")
                .payload(Map.of("lat", lat, "lng", 77.6)).originTimestampMs(1).build();
    }

    private TripEvent status(long seq, String tripId, String s) {
        return TripEvent.builder().eventId(UUID.randomUUID().toString())
                .topic(TOPIC).sequence(seq).type(EventType.STATUS)
                .tripId(tripId).cabId("cab-1").officeId("office-1")
                .payload(Map.of("status", s)).originTimestampMs(1).build();
    }

    @Test
    @DisplayName("An out-of-order LOCATION cannot overwrite a newer one (latest-by-sequence wins)")
    void locationRecordIsMonotonic() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();

        // Record the NEWER event first, then an OLDER one for the same cab.
        store.record(loc(10, "cab-A", 12.99));
        store.record(loc(7, "cab-A", 12.50)); // older: must be ignored

        TopicSnapshot snap = store.snapshot(TOPIC);
        TripEvent kept = snap.latestLocationByCab().get("cab-A");
        assertThat(kept.sequence()).isEqualTo(10);
        assertThat(kept.payload().get("lat")).isEqualTo(12.99);
    }

    @Test
    @DisplayName("An out-of-order STATUS cannot overwrite a newer one")
    void statusRecordIsMonotonic() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        store.record(status(20, "trip-1", "COMPLETED"));
        store.record(status(15, "trip-1", "DELAYED")); // older: ignored

        TopicSnapshot snap = store.snapshot(TOPIC);
        assertThat(snap.latestStatusByTrip().get("trip-1").sequence()).isEqualTo(20);
        assertThat(snap.latestStatusByTrip().get("trip-1").payload().get("status")).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("In-order records still advance normally, and distinct cabs are independent")
    void inOrderAndMultiKey() {
        InMemorySnapshotStore store = new InMemorySnapshotStore();
        store.record(loc(1, "cab-A", 1.0));
        store.record(loc(2, "cab-A", 2.0)); // newer, applies
        store.record(loc(3, "cab-B", 3.0));

        TopicSnapshot snap = store.snapshot(TOPIC);
        assertThat(snap.latestLocationByCab().get("cab-A").sequence()).isEqualTo(2);
        assertThat(snap.latestLocationByCab().get("cab-B").sequence()).isEqualTo(3);
    }
}

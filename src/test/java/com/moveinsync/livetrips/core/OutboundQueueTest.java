package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.core.OutboundQueue.OfferOutcome;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.moveinsync.livetrips.support.Events.location;
import static com.moveinsync.livetrips.support.Events.sos;
import static com.moveinsync.livetrips.support.Events.status;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The per-connection outbound queue is the mechanism behind three hard requirements:
 * bounded back-pressure, location conflation, and the SOS priority lane. These tests pin each
 * behaviour, plus the explicit overflow rule for critical events.
 */
class OutboundQueueTest {

    private static final String T = "trip:1";

    @Test
    @DisplayName("SOS is always delivered before queued location traffic (priority lane)")
    void sosJumpsAheadOfLocations() {
        OutboundQueue q = new OutboundQueue(100);
        for (int i = 1; i <= 50; i++) {
            q.offer(location(T, i, "cab-" + i, 1, 1)); // 50 distinct cabs so nothing conflates
        }
        q.offer(sos(T, 51, "trip-1"));

        // Even though the SOS arrived last, it comes out first.
        assertThat(q.poll().type()).isEqualTo(EventType.SOS);
    }

    @Test
    @DisplayName("Multiple SOS keep FIFO order among themselves and all precede other traffic")
    void multipleSosBeforeEverythingElse() {
        OutboundQueue q = new OutboundQueue(100);
        q.offer(status(T, 1, "trip-1", "STARTED"));
        q.offer(location(T, 2, "cab-1", 1, 1));
        q.offer(sos(T, 3, "trip-1"));
        q.offer(sos(T, 4, "trip-1"));

        assertThat(q.poll().sequence()).isEqualTo(3);
        assertThat(q.poll().sequence()).isEqualTo(4);
        // Then the non-SOS lane, in arrival order.
        assertThat(q.poll().type()).isEqualTo(EventType.STATUS);
        assertThat(q.poll().type()).isEqualTo(EventType.LOCATION);
        assertThat(q.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("A lagging client keeps only the latest location per cab (conflation)")
    void locationsConflateToLatestPerCab() {
        OutboundQueue q = new OutboundQueue(100);
        q.offer(location(T, 1, "cab-A", 10, 10));
        q.offer(location(T, 2, "cab-B", 20, 20));
        OfferOutcome second = q.offer(location(T, 3, "cab-A", 11, 11)); // newer cab-A

        assertThat(second).isEqualTo(OfferOutcome.CONFLATED);
        // Two cabs -> two entries, each the newest for its cab.
        assertThat(q.size()).isEqualTo(2);

        TripEvent first = q.poll();  // cab-A kept its original position, newest value
        assertThat(first.cabId()).isEqualTo("cab-A");
        assertThat(first.sequence()).isEqualTo(3);
        assertThat(q.poll().cabId()).isEqualTo("cab-B");
    }

    @Test
    @DisplayName("Status changes are never dropped, even when the queue is saturated with locations")
    void statusSurvivesLocationFlood() {
        OutboundQueue q = new OutboundQueue(10);
        // Fill with 10 distinct-cab locations (no conflation possible).
        for (int i = 1; i <= 10; i++) {
            assertThat(q.offer(location(T, i, "cab-" + i, 1, 1))).isEqualTo(OfferOutcome.ENQUEUED);
        }
        // A status arrives at a full queue: a location is evicted to make room, status is kept.
        OfferOutcome outcome = q.offer(status(T, 11, "trip-1", "DELAYED"));
        assertThat(outcome).isEqualTo(OfferOutcome.ENQUEUED);
        assertThat(q.isOverflowed()).isFalse();

        // Drain and confirm the status is present and there are still exactly 10 entries.
        List<TripEvent> drained = drain(q);
        assertThat(drained).hasSize(10);
        assertThat(drained).anyMatch(e -> e.type() == EventType.STATUS);
    }

    @Test
    @DisplayName("Bounded: a flood of distinct-cab locations never exceeds capacity")
    void boundedByCapacity() {
        OutboundQueue q = new OutboundQueue(50);
        for (int i = 1; i <= 5000; i++) {
            q.offer(location(T, i, "cab-" + i, 1, 1));
        }
        assertThat(q.size()).isLessThanOrEqualTo(50);
    }

    @Test
    @DisplayName("Overflow of critical events closes the connection rather than dropping them")
    void criticalOverflowIsSignalled() {
        OutboundQueue q = new OutboundQueue(3);
        // Fill the main lane entirely with critical (status) events: nothing droppable.
        assertThat(q.offer(status(T, 1, "trip-1", "A"))).isEqualTo(OfferOutcome.ENQUEUED);
        assertThat(q.offer(status(T, 2, "trip-1", "B"))).isEqualTo(OfferOutcome.ENQUEUED);
        assertThat(q.offer(status(T, 3, "trip-1", "C"))).isEqualTo(OfferOutcome.ENQUEUED);

        // The fourth critical event cannot be admitted without losing one: overflow.
        assertThat(q.offer(status(T, 4, "trip-1", "D"))).isEqualTo(OfferOutcome.REJECTED_OVERFLOW);
        assertThat(q.isOverflowed()).isTrue();
        // Once overflowed, everything is rejected (the gateway will close and let the client resume).
        assertThat(q.offer(location(T, 5, "cab-1", 1, 1))).isEqualTo(OfferOutcome.REJECTED_OVERFLOW);
    }

    @Test
    @DisplayName("A location offered into an all-critical full queue is dropped, not an overflow")
    void droppableLocationIntoFullCriticalQueue() {
        OutboundQueue q = new OutboundQueue(2);
        q.offer(status(T, 1, "trip-1", "A"));
        q.offer(status(T, 2, "trip-1", "B"));
        // No room and nothing droppable already queued, but the *incoming* event is a droppable
        // location -> drop it, stay healthy (not overflow).
        assertThat(q.offer(location(T, 3, "cab-1", 1, 1))).isEqualTo(OfferOutcome.DROPPED_LOCATION);
        assertThat(q.isOverflowed()).isFalse();
    }

    private static List<TripEvent> drain(OutboundQueue q) {
        List<TripEvent> out = new ArrayList<>();
        TripEvent e;
        while ((e = q.poll()) != null) {
            out.add(e);
        }
        return out;
    }
}

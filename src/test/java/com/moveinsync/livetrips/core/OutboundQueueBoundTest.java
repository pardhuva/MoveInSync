package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.core.OutboundQueue.OfferOutcome;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.moveinsync.livetrips.support.Events.sos;
import static com.moveinsync.livetrips.support.Events.status;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the exact per-connection memory contract so the documentation cannot drift again.
 *
 * <p>The SOS express lane and the main lane are bounded <b>independently</b>, each by
 * {@code capacity}. Therefore the hard worst-case total is {@code 2 x capacity} entries &mdash; not
 * {@code capacity} &mdash; and, critically, filling both lanes drops neither an SOS nor a STATUS.
 */
class OutboundQueueBoundTest {

    private static final String T = "trip:1";

    @Test
    @DisplayName("Both lanes fill independently: worst-case size is 2 x capacity, no critical event dropped")
    void worstCaseIsTwiceCapacity() {
        int capacity = 100;
        OutboundQueue q = new OutboundQueue(capacity);

        // Fill the SOS express lane to capacity. None may be dropped.
        for (int i = 1; i <= capacity; i++) {
            assertThat(q.offer(sos(T, i, "trip-1")))
                    .as("SOS %d must be enqueued", i)
                    .isEqualTo(OfferOutcome.ENQUEUED);
        }
        // Fill the main lane to capacity with STATUS (critical, never dropped).
        for (int i = 1; i <= capacity; i++) {
            assertThat(q.offer(status(T, capacity + i, "trip-1", "S" + i)))
                    .as("STATUS %d must be enqueued", i)
                    .isEqualTo(OfferOutcome.ENQUEUED);
        }

        // Hard worst case reached: exactly 2 x capacity entries, nothing dropped, not overflowed.
        assertThat(q.size()).isEqualTo(2 * capacity);
        assertThat(q.isOverflowed()).isFalse();

        // One more SOS cannot be admitted (SOS lane already full) -> overflow, never a silent drop.
        assertThat(q.offer(sos(T, 9999, "trip-1"))).isEqualTo(OfferOutcome.REJECTED_OVERFLOW);
        assertThat(q.isOverflowed()).isTrue();
    }

    @Test
    @DisplayName("A main lane full of STATUS plus a full SOS lane still delivers every critical event")
    void allCriticalEventsSurviveToPoll() {
        int capacity = 50;
        OutboundQueue q = new OutboundQueue(capacity);
        for (int i = 1; i <= capacity; i++) {
            q.offer(sos(T, i, "trip-1"));
        }
        for (int i = 1; i <= capacity; i++) {
            q.offer(status(T, capacity + i, "trip-1", "S" + i));
        }

        int sosCount = 0;
        int statusCount = 0;
        for (int i = 0; i < 2 * capacity; i++) {
            var e = q.poll();
            assertThat(e).isNotNull();
            switch (e.type()) {
                case SOS -> sosCount++;
                case STATUS -> statusCount++;
                default -> { }
            }
        }
        assertThat(sosCount).isEqualTo(capacity);
        assertThat(statusCount).isEqualTo(capacity);
        assertThat(q.isEmpty()).isTrue();
    }
}

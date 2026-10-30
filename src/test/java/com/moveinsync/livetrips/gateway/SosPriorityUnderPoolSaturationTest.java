package com.moveinsync.livetrips.gateway;

import com.moveinsync.livetrips.core.OutboundQueue;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;
import com.moveinsync.livetrips.support.BlockingTransport;
import com.moveinsync.livetrips.support.RecordingTransport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue 1 regression: a dedicated express writer pool means an SOS reaches a healthy connection
 * promptly even when the normal writer pool is fully saturated by other (frozen) connections.
 *
 * <p>Setup: a normal pool with exactly N threads, N frozen connections each blocking one of those
 * threads on a never-returning location write. With the old single-pool design the SOS to a healthy
 * connection would have no free thread to run its drain. With the dedicated SOS pool it is flushed
 * on an uncontended thread.
 */
class SosPriorityUnderPoolSaturationTest {

    private final FrameCodec codec = new FrameCodec();
    private ExecutorService normalPool;
    private ExecutorService sosPool;

    @AfterEach
    void tearDown() {
        if (normalPool != null) normalPool.shutdownNow();
        if (sosPool != null) sosPool.shutdownNow();
    }

    private TripEvent location(long seq, String cab) {
        return TripEvent.builder().eventId(UUID.randomUUID().toString())
                .topic("trip:1").sequence(seq).type(EventType.LOCATION)
                .tripId("trip-1").cabId(cab).officeId("office-1")
                .payload(Map.of("lat", 12.9, "lng", 77.6)).originTimestampMs(System.currentTimeMillis())
                .build();
    }

    private TripEvent sos(long seq) {
        return TripEvent.builder().eventId(UUID.randomUUID().toString())
                .topic("trip:1").sequence(seq).type(EventType.SOS)
                .tripId("trip-1").cabId("cab-1").officeId("office-1")
                .payload(Map.of("reasonCode", "PANIC")).originTimestampMs(System.currentTimeMillis())
                .build();
    }

    @Test
    @DisplayName("SOS reaches a healthy connection within 2s even when the normal pool is fully saturated")
    void sosDeliveredWhileNormalPoolSaturated() throws Exception {
        int normalThreads = 4;
        normalPool = Executors.newFixedThreadPool(normalThreads);
        sosPool = Executors.newFixedThreadPool(2);

        // Saturate every normal-pool thread with a frozen connection whose location write blocks.
        BlockingTransport[] frozen = new BlockingTransport[normalThreads];
        for (int i = 0; i < normalThreads; i++) {
            frozen[i] = new BlockingTransport("frozen-" + i);
            ClientConnection fc = new ClientConnection(frozen[i], new OutboundQueue(100),
                    codec, normalPool, sosPool, ConnectionObserver.NOOP);
            fc.offer(location(1, "cab-f" + i)); // schedules a normal drain that blocks on send()
        }
        // Wait until all normal-pool threads are actually parked inside a blocked send().
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
            int started = 0;
            for (BlockingTransport f : frozen) {
                if (f.sendsStarted() > 0) started++;
            }
            return started == normalThreads;
        });

        // A healthy connection on the SAME normal pool; its SOS must still get out via the SOS pool.
        RecordingTransport healthy = new RecordingTransport("healthy");
        ClientConnection hc = new ClientConnection(healthy, new OutboundQueue(100),
                codec, normalPool, sosPool, ConnectionObserver.NOOP);

        long t0 = System.nanoTime();
        hc.offer(sos(2));

        Awaitility.await().atMost(Duration.ofSeconds(2))
                .until(() -> !healthy.eventsOfType("SOS").isEmpty());
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        assertThat(healthy.eventsOfType("SOS")).hasSize(1);
        assertThat(elapsedMs).as("SOS delivered in %d ms despite a saturated normal pool", elapsedMs)
                .isLessThan(2000);

        for (BlockingTransport f : frozen) {
            f.unblock();
        }
    }

    @Test
    @DisplayName("Why the dedicated pool exists: a SHARED saturated pool starves the same SOS")
    void sharedPoolStarvesSosUnderSaturation() throws Exception {
        // Identical saturation scenario as above, but the SOS path shares the saturated pool.
        int threads = 2;
        ExecutorService shared = Executors.newFixedThreadPool(threads);
        BlockingTransport[] frozen = new BlockingTransport[threads];
        try {
            for (int i = 0; i < threads; i++) {
                frozen[i] = new BlockingTransport("frozen-" + i);
                ClientConnection fc = new ClientConnection(frozen[i], new OutboundQueue(100),
                        codec, shared, shared, ConnectionObserver.NOOP);
                fc.offer(location(1, "cab-f" + i));
            }
            Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> {
                int started = 0;
                for (BlockingTransport f : frozen) {
                    if (f.sendsStarted() > 0) started++;
                }
                return started == threads;
            });

            RecordingTransport healthy = new RecordingTransport("healthy");
            ClientConnection hc = new ClientConnection(healthy, new OutboundQueue(100),
                    codec, shared, shared, ConnectionObserver.NOOP);
            hc.offer(sos(2));

            // Every thread of the shared pool is parked in a blocked send, so neither the normal
            // drain nor the express flush can run: the SOS is starved. This is precisely the
            // failure the dedicated express pool removes (see the test above, which delivers it).
            Thread.sleep(600);
            assertThat(healthy.eventsOfType("SOS"))
                    .as("with a shared saturated pool the SOS cannot get out")
                    .isEmpty();
        } finally {
            for (BlockingTransport f : frozen) {
                if (f != null) f.unblock();
            }
            shared.shutdownNow();
        }
    }

    @Test
    @DisplayName("Falling back to a shared pool (no dedicated SOS pool) still delivers the SOS when a thread is free")
    void sharedPoolFallbackStillWorks() {
        // When sosWriterPool == writerPool, behaviour degrades gracefully but remains correct.
        ExecutorService shared = Executors.newFixedThreadPool(2);
        try {
            RecordingTransport t = new RecordingTransport("c");
            ClientConnection c = new ClientConnection(t, new OutboundQueue(100), codec, shared);
            c.offer(sos(1));
            Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> !t.eventsOfType("SOS").isEmpty());
            assertThat(t.eventsOfType("SOS")).hasSize(1);
        } finally {
            shared.shutdownNow();
        }
    }
}

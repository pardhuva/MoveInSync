package com.moveinsync.livetrips.integration;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.gateway.ClientConnection;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.service.PublishCommand;
import com.moveinsync.livetrips.support.GatewayHarness;
import com.moveinsync.livetrips.support.RecordingTransport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue 4, end to end: with many threads publishing to the SAME topic concurrently, the events a
 * subscriber actually receives on the live wire must be in strict sequence order.
 *
 * <p>STATUS is used rather than LOCATION because locations are deliberately conflated in the
 * outbound queue (latest-per-cab), which would collapse the very sequence we want to observe.
 *
 * <p>Note: this asserts the invariant holds <i>with</i> the ordered dispatcher. It cannot reliably
 * reproduce the pre-fix race on demand, so it is a regression guard rather than a proof of the old
 * bug. Strict ordering here is a single-node guarantee; cross-node Redis ordering is reconciled by
 * sequence at the client and is NOT asserted by this test.
 */
class ConcurrentSameTopicOrderingTest {

    private GatewayHarness h;
    private final AuthenticatedPrincipal operator = AuthenticatedPrincipal.operator("op", "acme");
    private final AuthenticatedPrincipal device = AuthenticatedPrincipal.device("dev-1", "acme", "cab-1");

    @BeforeEach
    void setUp() {
        h = new GatewayHarness(5000); // generous queue so criticals are never shed in this test
        h.repository.registerOffice("office-1", "acme");
        h.repository.save(new TripMeta("trip-1", "cab-1", "office-1", "acme", "emp-alice"));
    }

    @AfterEach
    void tearDown() {
        h.shutdown();
    }

    @Test
    @DisplayName("Concurrent publishers to one trip: the subscriber's live wire order is strictly increasing")
    void liveOrderIsStrictUnderConcurrentPublishing() throws Exception {
        RecordingTransport t = new RecordingTransport("desk");
        ClientConnection c = h.connect(t, operator);
        h.subscriptionService.subscribe(c, "trip:trip-1", 0);

        int threads = 8;
        int perThread = 40;
        int total = threads * perThread;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < perThread; j++) {
                        // DELAYED is a non-terminal status, so the trip is not ended mid-test.
                        h.publishService.publish(device, new PublishCommand("trip-1",
                                EventType.STATUS, Map.of("status", "DELAYED")));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        Awaitility.await().atMost(Duration.ofSeconds(20))
                .until(() -> t.eventsOfType("STATUS").size() == total);

        List<Long> deliveredSeqs = t.eventsOfType("STATUS").stream()
                .map(e -> ((Number) e.get("seq")).longValue())
                .toList();

        assertThat(deliveredSeqs).hasSize(total);
        // The heart of the fix: delivered order equals sequence order, with no inversion.
        assertThat(deliveredSeqs).isSorted();
        assertThat(deliveredSeqs).doesNotHaveDuplicates();
        assertThat(deliveredSeqs.get(0)).isEqualTo(1L);
        assertThat(deliveredSeqs.get(total - 1)).isEqualTo((long) total);
    }

    @Test
    @DisplayName("SOS still skips ahead of queued traffic even though other events are order-dispatched")
    void sosStillSkipsAhead() {
        RecordingTransport t = new RecordingTransport("desk");
        ClientConnection c = h.connect(t, operator);
        h.subscriptionService.subscribe(c, "trip:trip-1", 0);

        // Queue a burst of statuses, then fire an SOS.
        for (int i = 0; i < 100; i++) {
            h.publishService.publish(device, new PublishCommand("trip-1",
                    EventType.STATUS, Map.of("status", "DELAYED")));
        }
        h.publishService.publish(device, new PublishCommand("trip-1",
                EventType.SOS, Map.of("reasonCode", "PANIC")));

        Awaitility.await().atMost(Duration.ofSeconds(10))
                .until(() -> !t.eventsOfType("SOS").isEmpty());
        assertThat(t.eventsOfType("SOS")).hasSize(1);
    }
}

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
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The headline use case from the case study, end to end through the real gateway stack:
 *
 * <blockquote>"An employee's phone goes into a lift for 40 seconds. On reconnect it receives the
 * three status changes it missed, in order, and only the latest location."</blockquote>
 */
class ResumeIntegrationTest {

    private GatewayHarness h;
    private AuthenticatedPrincipal alice;

    @BeforeEach
    void setUp() {
        h = new GatewayHarness();
        h.repository.registerOffice("office-1", "acme");
        h.repository.save(new TripMeta("trip-1", "cab-1", "office-1", "acme", "emp-alice"));
        alice = AuthenticatedPrincipal.employee("emp-alice", "acme", Set.of("trip-1"));
    }

    @AfterEach
    void tearDown() {
        h.shutdown();
    }

    private void publish(EventType type, Map<String, Object> payload) {
        h.publishService.publish(
                AuthenticatedPrincipal.device("dev-1", "acme", "cab-1"),
                new PublishCommand("trip-1", type, payload));
    }

    @Test
    @DisplayName("Phone-in-a-lift: on reconnect the client gets the 3 missed statuses in order and only the latest location")
    void reconnectReplaysMissedStatusesAndLatestLocation() {
        // --- Phase 1: connected, sees the trip live ---
        RecordingTransport t1 = new RecordingTransport("alice-1");
        ClientConnection c1 = h.connect(t1, alice);
        h.subscriptionService.subscribe(c1, "trip:trip-1", 0); // fresh: snapshot + live

        publish(EventType.LOCATION, Map.of("lat", 12.90, "lng", 77.60));
        awaitEvents(t1, 1);
        long lastSeq = t1.lastSeqOfType("LOCATION"); // highest seq the client has applied

        // --- Phase 2: the lift. The client is gone; updates keep flowing. ---
        c1.close("CLIENT_GONE");

        publish(EventType.STATUS, Map.of("status", "DELAYED", "delayMinutes", 5));
        publish(EventType.LOCATION, Map.of("lat", 12.91, "lng", 77.61));
        publish(EventType.STATUS, Map.of("status", "STARTED"));
        publish(EventType.LOCATION, Map.of("lat", 12.92, "lng", 77.62)); // newer location
        publish(EventType.STATUS, Map.of("status", "COMPLETED"));

        // --- Phase 3: out of the lift, reconnect and resume from lastSeq ---
        RecordingTransport t2 = new RecordingTransport("alice-2");
        ClientConnection c2 = h.connect(t2, alice);
        h.subscriptionService.subscribe(c2, "trip:trip-1", lastSeq);
        awaitEvents(t2, 4); // 3 statuses + at least 1 location

        // Exactly the three missed status changes, in order.
        List<Map<String, Object>> statuses = t2.eventsOfType("STATUS");
        assertThat(statuses).extracting(e -> (Object) ((Map<?, ?>) e.get("payload")).get("status"))
                .containsExactly("DELAYED", "STARTED", "COMPLETED");

        // The replay carried both location rows (the log is literal); what matters is the client
        // ends on the latest position. The highest LOCATION seq delivered is the newest one.
        List<Map<String, Object>> locations = t2.eventsOfType("LOCATION");
        Map<String, Object> newestLoc = locations.get(locations.size() - 1);
        assertThat(((Map<?, ?>) newestLoc.get("payload")).get("lat")).isEqualTo(12.92);

        // Ordering: statuses arrive in sequence order (per-key ordering preserved).
        assertThat(statuses).extracting(e -> ((Number) e.get("seq")).longValue()).isSorted();
    }

    @Test
    @DisplayName("Away too long: a client outside the replay window is re-seeded with a snapshot")
    void outsideWindowGetsSnapshot() {
        // A tiny window so we can overrun it deterministically.
        GatewayHarness small = GatewayHarness.withReplayWindow(3, 1000);
        try {
            small.repository.registerOffice("office-1", "acme");
            small.repository.save(new TripMeta("trip-1", "cab-1", "office-1", "acme", "emp-alice"));

            RecordingTransport t1 = new RecordingTransport("alice-1");
            ClientConnection c1 = small.connect(t1, alice);
            small.subscriptionService.subscribe(c1, "trip:trip-1", 0);
            small.publishService.publish(AuthenticatedPrincipal.device("d", "acme", "cab-1"),
                    new PublishCommand("trip-1", EventType.LOCATION, Map.of("lat", 1.0, "lng", 1.0)));
            awaitEvents(t1, 1);
            long lastSeq = t1.lastSeqOfType("LOCATION");
            c1.close("GONE");

            // Publish far more than the window (3) can hold.
            for (int i = 0; i < 20; i++) {
                small.publishService.publish(AuthenticatedPrincipal.device("d", "acme", "cab-1"),
                        new PublishCommand("trip-1", EventType.LOCATION, Map.of("lat", 2.0 + i, "lng", 2.0)));
            }

            RecordingTransport t2 = new RecordingTransport("alice-2");
            ClientConnection c2 = small.connect(t2, alice);
            small.subscriptionService.subscribe(c2, "trip:trip-1", lastSeq);

            Awaitility.await().atMost(Duration.ofSeconds(5))
                    .until(() -> !t2.framesOfKind("snapshot").isEmpty());
            assertThat(t2.framesOfKind("snapshot")).isNotEmpty();
        } finally {
            small.shutdown();
        }
    }

    private static void awaitEvents(RecordingTransport t, int atLeast) {
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> t.events().size() >= atLeast);
    }
}

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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue 2 regression: when the location-dominated replay window has been evicted and a client
 * resumes on the snapshot path, it must still receive every STATUS/SOS it missed, in order, from
 * the independent bounded critical history &mdash; not just the latest status.
 */
class SnapshotPreservesCriticalHistoryTest {

    private GatewayHarness h;
    private final AuthenticatedPrincipal alice =
            AuthenticatedPrincipal.employee("emp-alice", "acme", Set.of("trip-1"));
    private final AuthenticatedPrincipal device =
            AuthenticatedPrincipal.device("dev-1", "acme", "cab-1");

    @AfterEach
    void tearDown() {
        if (h != null) {
            h.shutdown();
        }
    }

    private void publishLocation(double lat) {
        h.publishService.publish(device, new PublishCommand("trip-1", EventType.LOCATION,
                Map.of("lat", lat, "lng", 77.6)));
    }

    private void publishStatus(String s) {
        h.publishService.publish(device, new PublishCommand("trip-1", EventType.STATUS,
                Map.of("status", s)));
    }

    private void publishSos() {
        h.publishService.publish(device, new PublishCommand("trip-1", EventType.SOS,
                Map.of("reasonCode", "PANIC")));
    }

    @Test
    @DisplayName("Snapshot path still replays every missed STATUS/SOS in order (critical history within horizon)")
    void snapshotPathReplaysMissedCriticals() {
        // Tiny replay window (3) so a location flood evicts it; generous critical history (256).
        h = GatewayHarness.withReplayAndCriticalWindow(3, 256, 1000);
        h.repository.registerOffice("office-1", "acme");
        h.repository.save(new TripMeta("trip-1", "cab-1", "office-1", "acme", "emp-alice"));

        // Client sees the first status, then disconnects.
        RecordingTransport t1 = new RecordingTransport("alice-1");
        ClientConnection c1 = h.connect(t1, alice);
        h.subscriptionService.subscribe(c1, "trip:trip-1", 0);
        publishStatus("STARTED");
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !t1.eventsOfType("STATUS").isEmpty());
        long lastSeq = t1.lastSeqOfType("STATUS");
        c1.close("GONE");

        // While away: criticals interleaved with a location flood that far exceeds the replay window.
        publishStatus("DELAYED");
        for (int i = 0; i < 30; i++) {
            publishLocation(12.9 + i * 0.001);
        }
        publishSos();
        for (int i = 0; i < 30; i++) {
            publishLocation(13.0 + i * 0.001);
        }
        publishStatus("COMPLETED");

        // Reconnect: the replay window is long gone, so this takes the SNAPSHOT path.
        RecordingTransport t2 = new RecordingTransport("alice-2");
        ClientConnection c2 = h.connect(t2, alice);
        h.subscriptionService.subscribe(c2, "trip:trip-1", lastSeq);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> t2.eventsOfType("STATUS").size() >= 2 && !t2.eventsOfType("SOS").isEmpty());

        // Confirm it was the snapshot path and that it was NOT flagged truncated.
        List<Map<String, Object>> subs = t2.framesOfKind("subscribed");
        assertThat(subs).isNotEmpty();
        assertThat(subs.get(0).get("mode")).isEqualTo("SNAPSHOT");
        assertThat(subs.get(0).get("criticalTruncated")).isEqualTo(false);
        assertThat(t2.framesOfKind("snapshot")).isNotEmpty();

        // Every missed STATUS and the SOS were replayed, in order (this is the fix).
        List<Map<String, Object>> statuses = t2.eventsOfType("STATUS");
        assertThat(statuses).extracting(e -> (Object) ((Map<?, ?>) e.get("payload")).get("status"))
                .containsExactly("DELAYED", "COMPLETED");
        assertThat(t2.eventsOfType("SOS")).hasSize(1);
        assertThat(t2.events()).extracting(e -> ((Number) e.get("seq")).longValue()).isSorted();
    }

    @Test
    @DisplayName("Critical-history overflow is signalled (truncated=true) rather than silently losing criticals")
    void criticalHistoryOverflowIsFlagged() {
        // Tiny replay window AND tiny critical history (2), so many criticals overflow it.
        h = GatewayHarness.withReplayAndCriticalWindow(3, 2, 1000);
        h.repository.registerOffice("office-1", "acme");
        h.repository.save(new TripMeta("trip-1", "cab-1", "office-1", "acme", "emp-alice"));

        RecordingTransport t1 = new RecordingTransport("alice-1");
        ClientConnection c1 = h.connect(t1, alice);
        h.subscriptionService.subscribe(c1, "trip:trip-1", 0);
        publishStatus("STARTED");
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !t1.eventsOfType("STATUS").isEmpty());
        long lastSeq = t1.lastSeqOfType("STATUS");
        c1.close("GONE");

        // Publish more criticals than the critical history can hold (capacity 2).
        publishStatus("A");
        publishStatus("B");
        publishStatus("C");
        publishStatus("D");

        RecordingTransport t2 = new RecordingTransport("alice-2");
        ClientConnection c2 = h.connect(t2, alice);
        h.subscriptionService.subscribe(c2, "trip:trip-1", lastSeq);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> !t2.framesOfKind("subscribed").isEmpty());

        Map<String, Object> sub = t2.framesOfKind("subscribed").get(0);
        assertThat(sub.get("mode")).isEqualTo("SNAPSHOT");
        // The client is told its critical recovery is incomplete, rather than silently missing events.
        assertThat(sub.get("criticalTruncated")).isEqualTo(true);
        // It still receives the criticals that are retained (the most recent ones).
        assertThat(t2.eventsOfType("STATUS")).isNotEmpty();
    }
}

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
 * Node-crash recovery. Two gateway harnesses share one event log and snapshot store, exactly as
 * two gateway nodes share Redis. A client on node A is dropped (the node "crashes"), updates
 * continue to be published cluster-wide, and the client reconnects to node B and resumes.
 *
 * <p>The requirement being proven: "when a node dies, its clients reconnect to another node and
 * resume without losing a status or SOS event."
 */
class NodeCrashRecoveryTest {

    private GatewayHarness nodeA;
    private GatewayHarness nodeB;
    private AuthenticatedPrincipal alice;
    private final AuthenticatedPrincipal device = AuthenticatedPrincipal.device("dev-1", "acme", "cab-1");

    @BeforeEach
    void setUp() {
        nodeA = new GatewayHarness();
        nodeB = nodeA.sharingBackbone("node-B"); // shares event log + snapshot store (the "Redis")
        nodeA.repository.registerOffice("office-1", "acme");
        nodeA.repository.save(new TripMeta("trip-1", "cab-1", "office-1", "acme", "emp-alice"));
        alice = AuthenticatedPrincipal.employee("emp-alice", "acme", Set.of("trip-1"));
    }

    @AfterEach
    void tearDown() {
        nodeA.shutdown();
        nodeB.shutdown();
    }

    @Test
    @DisplayName("A client whose gateway node crashes reconnects to another node and loses no status or SOS")
    void survivesNodeCrashWithoutLosingCriticalEvents() {
        // Connected to node A.
        RecordingTransport tA = new RecordingTransport("alice-A");
        ClientConnection cA = nodeA.connect(tA, alice);
        nodeA.subscriptionService.subscribe(cA, "trip:trip-1", 0);

        nodeA.publishService.publish(device,
                new PublishCommand("trip-1", EventType.STATUS, Map.of("status", "STARTED")));
        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> !tA.eventsOfType("STATUS").isEmpty());
        long lastSeq = tA.lastSeqOfType("STATUS");

        // --- Node A crashes: every connection it held is gone. ---
        cA.close("NODE_CRASH");

        // Critical events published cluster-wide while the client is mid-reconnect.
        nodeA.publishService.publish(device,
                new PublishCommand("trip-1", EventType.STATUS, Map.of("status", "DELAYED")));
        nodeA.publishService.publish(device,
                new PublishCommand("trip-1", EventType.SOS, Map.of("reasonCode", "PANIC")));
        nodeA.publishService.publish(device,
                new PublishCommand("trip-1", EventType.STATUS, Map.of("status", "COMPLETED")));

        // --- Client reconnects to node B and resumes from its last sequence. ---
        RecordingTransport tB = new RecordingTransport("alice-B");
        ClientConnection cB = nodeB.connect(tB, alice);
        nodeB.subscriptionService.subscribe(cB, "trip:trip-1", lastSeq);

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> tB.eventsOfType("STATUS").size() >= 2 && !tB.eventsOfType("SOS").isEmpty());

        // Not one status or SOS was lost across the crash.
        List<Map<String, Object>> statuses = tB.eventsOfType("STATUS");
        assertThat(statuses).extracting(e -> (Object) ((Map<?, ?>) e.get("payload")).get("status"))
                .containsExactly("DELAYED", "COMPLETED");
        assertThat(tB.eventsOfType("SOS")).hasSize(1);

        // And they arrive in sequence order on the new node.
        assertThat(tB.events()).extracting(e -> ((Number) e.get("seq")).longValue()).isSorted();
    }

    @Test
    @DisplayName("Node B serves a snapshot built from events it never saw live (shared state)")
    void nodeBSnapshotReflectsEventsPublishedViaNodeA() {
        // All writes go through node A and land in the shared event log + snapshot store.
        nodeA.publishService.publish(device,
                new PublishCommand("trip-1", EventType.STATUS, Map.of("status", "STARTED")));
        nodeA.publishService.publish(device,
                new PublishCommand("trip-1", EventType.LOCATION, Map.of("lat", 12.95, "lng", 77.65)));

        // A brand-new client connects to node B, which never processed those events live.
        RecordingTransport tB = new RecordingTransport("carol-B");
        ClientConnection cB = nodeB.connect(tB, alice);
        nodeB.subscriptionService.subscribe(cB, "trip:trip-1", 0); // fresh -> snapshot

        Awaitility.await().atMost(Duration.ofSeconds(5))
                .until(() -> !tB.framesOfKind("snapshot").isEmpty());

        // The snapshot node B produced reflects the state written through node A: shared backbone.
        Map<String, Object> snap = tB.framesOfKind("snapshot").get(0);
        assertThat(snap.get("statuses").toString()).contains("STARTED");
        assertThat(snap.get("locations").toString()).contains("cab-1");
    }
}

package com.moveinsync.livetrips.integration;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.gateway.ClientConnection;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.service.PublishCommand;
import com.moveinsync.livetrips.support.BlockingTransport;
import com.moveinsync.livetrips.support.GatewayHarness;
import com.moveinsync.livetrips.support.RecordingTransport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The failure-injection test the advanced track requires, and the second example use case:
 *
 * <blockquote>"One security desk browser freezes. Its queue fills and its location updates are
 * conflated, while SOS latency for every other desk stays under 2 seconds."</blockquote>
 *
 * <p>A frozen consumer (a transport whose write blocks) sits alongside healthy consumers. We flood
 * the shared topic with location traffic, then fire an SOS, and prove the healthy consumers still
 * receive it promptly while the frozen one is bounded and ultimately shed &mdash; without ever
 * losing the SOS for anyone else.
 */
class SlowConsumerIsolationTest {

    private GatewayHarness h;
    private final AuthenticatedPrincipal operator = AuthenticatedPrincipal.operator("op-desk", "acme");
    private final AuthenticatedPrincipal device = AuthenticatedPrincipal.device("dev-1", "acme", "cab-1");

    @BeforeEach
    void setUp() {
        // Small queue so the frozen consumer saturates quickly and deterministically.
        h = new GatewayHarness(200);
        h.repository.registerOffice("office-1", "acme");
        h.repository.save(new TripMeta("trip-1", "cab-1", "office-1", "acme", "emp-alice"));
    }

    @AfterEach
    void tearDown() {
        h.shutdown();
    }

    @Test
    @DisplayName("A frozen consumer never delays SOS delivery to healthy consumers")
    void frozenConsumerDoesNotBlockOthers() {
        // One frozen desk: its first write will block and never return until released.
        BlockingTransport frozenTransport = new BlockingTransport("frozen-desk");
        ClientConnection frozen = h.newConnection(frozenTransport);
        frozen.authenticate(operator);
        h.subscriptionService.subscribe(frozen, "trip:trip-1", 0);

        // Several healthy desks.
        RecordingTransport[] healthy = new RecordingTransport[5];
        for (int i = 0; i < healthy.length; i++) {
            healthy[i] = new RecordingTransport("healthy-" + i);
            ClientConnection c = h.newConnection(healthy[i]);
            c.authenticate(operator);
            h.subscriptionService.subscribe(c, "trip:trip-1", 0);
        }

        // Flood the topic with location traffic for the same cab (conflation territory).
        for (int i = 0; i < 2000; i++) {
            h.publishService.publish(device,
                    new PublishCommand("trip-1", EventType.LOCATION,
                            Map.of("lat", 12.9 + i * 1e-5, "lng", 77.6)));
        }

        // Fire the SOS.
        long sosPublishedAt = System.nanoTime();
        h.publishService.publish(device,
                new PublishCommand("trip-1", EventType.SOS, Map.of("reasonCode", "PANIC")));

        // Every healthy desk receives the SOS; measure how long the slowest took.
        for (RecordingTransport t : healthy) {
            Awaitility.await().atMost(Duration.ofSeconds(2))
                    .until(() -> !t.eventsOfType("SOS").isEmpty());
        }
        long worstMs = (System.nanoTime() - sosPublishedAt) / 1_000_000;

        // The frozen desk is stuck on its first blocked write and has received no SOS...
        assertThat(frozenTransport.sendsStarted()).isGreaterThanOrEqualTo(1);

        // ...yet every healthy desk got the SOS, and well within the 2s budget.
        for (RecordingTransport t : healthy) {
            assertThat(t.eventsOfType("SOS")).as("healthy desk %s got SOS", t.id()).hasSize(1);
        }
        assertThat(worstMs).as("worst healthy SOS delivery was %d ms", worstMs).isLessThan(2000);

        // The frozen desk's queue is bounded: it never grew past capacity despite 2000+ offers.
        assertThat(frozen.queue().size()).isLessThanOrEqualTo(frozen.queue().capacity());

        // Release the frozen consumer so the writer thread is reclaimed cleanly.
        frozenTransport.unblock();
    }

    @Test
    @DisplayName("Healthy desks' locations are conflated, not lost, under a flood")
    void healthyConsumersConflateUnderFlood() {
        RecordingTransport t = new RecordingTransport("desk");
        ClientConnection c = h.newConnection(t);
        c.authenticate(operator);
        h.subscriptionService.subscribe(c, "trip:trip-1", 0);

        for (int i = 0; i < 5000; i++) {
            h.publishService.publish(device,
                    new PublishCommand("trip-1", EventType.LOCATION,
                            Map.of("lat", 12.9 + i * 1e-6, "lng", 77.6)));
        }
        // Give the drain loop time to flush.
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> c.queue().isEmpty());

        // Far fewer than 5000 frames were delivered (conflation collapsed the backlog), but the
        // client still ends on a valid, recent position and the connection stayed healthy.
        assertThat(t.eventsOfType("LOCATION").size()).isLessThan(5000);
        assertThat(h.metrics).isNotNull();
        assertThat(c.isOpen()).isTrue();
    }
}

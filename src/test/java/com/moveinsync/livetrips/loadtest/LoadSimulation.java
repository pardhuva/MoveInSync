package com.moveinsync.livetrips.loadtest;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.gateway.ClientConnection;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.service.PublishCommand;
import com.moveinsync.livetrips.support.GatewayHarness;
import com.moveinsync.livetrips.support.LatencyCollector;
import com.moveinsync.livetrips.support.LatencyTransport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * The reproducible load test the advanced track asks for: "simulate at least 10,000 connections and
 * 2,000 publishing cabs, report SOS p99 latency, and kill one gateway node during the test."
 *
 * <p>This is NOT run by the normal {@code mvn test} (the class name has no {@code Test} suffix, so
 * surefire's default pattern skips it). Run it explicitly:
 *
 * <pre>
 *   mvn -Dtest=LoadSimulation test
 *   mvn -Dtest=LoadSimulation -Dload.connections=10000 -Dload.cabs=2000 -Dload.durationSeconds=20 test
 * </pre>
 *
 * <h2>What it measures, honestly</h2>
 * Two gateway harnesses share one event log and snapshot store, exactly as two gateway nodes share
 * Redis. Connections are in-process ({@link LatencyTransport}): there is no real TCP, so the
 * reported latency is the application pipeline latency (sequencing, fan-out, bounded queue,
 * conflation, serialization), not end-to-end network latency. This isolates the thing under test
 * (the fan-out layer) from the machine's socket limits, and is the figure the README reports with
 * that caveat stated.
 */
@Tag("load")
public class LoadSimulation {

    @Test
    void run() throws Exception {
        int connections = intProp("load.connections", 10_000);
        int cabs = intProp("load.cabs", 2_000);
        int durationSeconds = intProp("load.durationSeconds", 20);
        int locationHz = intProp("load.locationHz", 2);
        int queueCapacity = intProp("load.queueCapacity", 500);
        int offices = intProp("load.offices", 50);
        int publisherThreads = intProp("load.publisherThreads", 16);

        System.out.printf("%n=== LOAD SIMULATION ===%n");
        System.out.printf("connections=%d cabs=%d duration=%ds locationHz=%d queueCap=%d offices=%d%n",
                connections, cabs, durationSeconds, locationHz, queueCapacity, offices);
        printEnvironment();

        // --- Two gateway nodes sharing one backbone (the "Redis"). ---
        GatewayHarness nodeA = new GatewayHarness(queueCapacity);
        GatewayHarness nodeB = nodeA.sharingBackbone("node-B");

        // --- Register trips (one active trip per cab). ---
        for (int i = 0; i < cabs; i++) {
            String office = "office-" + (i % offices);
            nodeA.repository.registerOffice(office, "acme");
            nodeA.repository.save(new TripMeta("trip-" + i, "cab-" + i, office, "acme", "emp-" + i));
        }

        // --- Operators watch trips; an operator may watch any trip in its tenant, which keeps
        //     per-connection auth setup trivial while still exercising the full auth path. ---
        AuthenticatedPrincipal operator = AuthenticatedPrincipal.operator("op", "acme");
        // One authenticated device per cab, each permitted to publish only its own cab -- exactly
        // as 2,000 real in-vehicle units would be provisioned.
        AuthenticatedPrincipal[] devices = new AuthenticatedPrincipal[cabs];
        for (int i = 0; i < cabs; i++) {
            devices[i] = AuthenticatedPrincipal.device("dev-" + i, "acme", "cab-" + i);
        }

        LatencyCollector sosLatency = new LatencyCollector();
        LongAdder delivered = new LongAdder();
        // SOS re-delivered as part of a resume. Counted separately: their age includes the client's
        // offline time, so they must not be scored against the live 2s SOS target.
        LongAdder recoveredSos = new LongAdder();

        // --- Create connections, round-robin across the two nodes, each watching one trip. ---
        List<ConnState> onNodeA = new ArrayList<>();
        List<GatewayHarness> nodes = List.of(nodeA, nodeB);
        long connectStart = System.currentTimeMillis();
        for (int i = 0; i < connections; i++) {
            GatewayHarness node = nodes.get(i % 2);
            String tripTopic = "trip:trip-" + (i % cabs);
            LatencyTransport transport = new LatencyTransport("c-" + i, sosLatency, delivered, recoveredSos);
            ClientConnection c = node.newConnection(transport);
            c.authenticate(operator);
            node.subscriptionService.subscribe(c, tripTopic, 0);
            if (node == nodeA) {
                onNodeA.add(new ConnState(c, tripTopic, transport));
            }
        }
        System.out.printf("connected %d sockets in %d ms%n", connections,
                System.currentTimeMillis() - connectStart);

        // --- Publishers: each thread drives a slice of cabs at locationHz for the duration. ---
        AtomicBoolean running = new AtomicBoolean(true);
        LongAdder publishedLocations = new LongAdder();
        LongAdder publishedSos = new LongAdder();
        ExecutorService publishers = Executors.newFixedThreadPool(publisherThreads);
        CountDownLatch publishersDone = new CountDownLatch(publisherThreads);

        long intervalMs = Math.max(1, 1000 / locationHz);
        for (int t = 0; t < publisherThreads; t++) {
            final int threadIdx = t;
            publishers.submit(() -> {
                try {
                    while (running.get()) {
                        long tick = System.currentTimeMillis();
                        for (int cab = threadIdx; cab < cabs; cab += publisherThreads) {
                            GatewayHarness pub = nodeA.isAlive() ? nodeA : nodeB;
                            try {
                                pub.publishService.publish(devices[cab], new PublishCommand(
                                        "trip-" + cab, EventType.LOCATION,
                                        Map.of("lat", 12.9 + cab * 1e-6, "lng", 77.6)));
                                publishedLocations.increment();
                            } catch (RuntimeException ignored) {
                                // A node mid-shutdown can reject; the next tick uses the survivor.
                            }
                        }
                        long elapsed = System.currentTimeMillis() - tick;
                        if (elapsed < intervalMs) {
                            Thread.sleep(intervalMs - elapsed);
                        }
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    publishersDone.countDown();
                }
            });
        }

        // --- SOS injector: fire a batch of SOS periodically so we build a p99 distribution. ---
        ExecutorService sosInjector = Executors.newSingleThreadExecutor();
        sosInjector.submit(() -> {
            try {
                while (running.get()) {
                    for (int k = 0; k < 50; k++) {
                        int trip = ThreadLocalRandom.current().nextInt(cabs);
                        GatewayHarness pub = nodeA.isAlive() ? nodeA : nodeB;
                        try {
                            pub.publishService.publish(devices[trip], new PublishCommand(
                                    "trip-" + trip, EventType.SOS, Map.of("reasonCode", "PANIC")));
                            publishedSos.increment();
                        } catch (RuntimeException ignored) {
                            // node shutting down
                        }
                    }
                    Thread.sleep(1000);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        });

        // --- Let it warm up, then KILL NODE A halfway and reconnect its clients to node B. ---
        boolean killNode = !"false".equals(System.getProperty("load.killNode"));
        Thread.sleep(durationSeconds * 1000L / 2);
        if (!killNode) {
            System.out.printf("%n>>> node kill DISABLED (load.killNode=false) <<<%n");
            Thread.sleep(durationSeconds * 1000L / 2);
            running.set(false);
            publishersDone.await(10, TimeUnit.SECONDS);
            publishers.shutdownNow();
            sosInjector.shutdownNow();
            Thread.sleep(1500);
            System.out.println(buildReport(connections, cabs, durationSeconds, queueCapacity,
                    publishedLocations.sum(), publishedSos.sum(), delivered.sum(), durationSeconds,
                    sosLatency, 0, 0, recoveredSos.sum()));
            nodeA.kill();
            nodeB.kill();
            return;
        }
        System.out.printf("%n>>> KILLING node-A at T+%ds (held %d connections) <<<%n",
                durationSeconds / 2, onNodeA.size());
        long killStart = System.currentTimeMillis();
        long sosBeforeKill = sosLatency.count();

        nodeA.kill(); // node dies: all its connections drop

        // Its clients reconnect to node B and resume from their last applied sequence.
        int reconnected = 0;
        for (ConnState cs : onNodeA) {
            long lastSeq = cs.connection().lastDeliveredSeq(cs.topic());
            LatencyTransport transport = new LatencyTransport(cs.transport().id() + "-r", sosLatency, delivered, recoveredSos);
            ClientConnection c = nodeB.newConnection(transport);
            c.authenticate(operator);
            nodeB.subscriptionService.subscribe(c, cs.topic(), lastSeq);
            reconnected++;
        }
        System.out.printf(">>> reconnected %d clients to node-B in %d ms <<<%n",
                reconnected, System.currentTimeMillis() - killStart);

        // --- Run out the second half entirely on node B. ---
        Thread.sleep(durationSeconds * 1000L / 2);
        running.set(false);
        publishersDone.await(10, TimeUnit.SECONDS);
        publishers.shutdownNow();
        sosInjector.shutdownNow();

        // Let the drain loops flush outstanding frames.
        Thread.sleep(1500);

        long sosAfterKill = sosLatency.count() - sosBeforeKill;
        long totalDelivered = delivered.sum();

        // --- Report ---
        String report = buildReport(connections, cabs, durationSeconds, queueCapacity,
                publishedLocations.sum(), publishedSos.sum(), totalDelivered, durationSeconds,
                sosLatency, reconnected, sosAfterKill, recoveredSos.sum());
        System.out.println(report);
        writeReport(report);

        nodeB.kill();

        // Minimal sanity assertions so the run fails loudly if the pipeline stalled.
        if (totalDelivered <= 0) {
            throw new AssertionError("no events were delivered");
        }
        if (sosLatency.count() <= 0) {
            throw new AssertionError("no SOS latency samples collected");
        }
    }

    private record ConnState(ClientConnection connection, String topic, LatencyTransport transport) {
    }

    private static String buildReport(int connections, int cabs, int durationSeconds, int queueCap,
                                      long publishedLoc, long publishedSos, long totalDelivered,
                                      double wallSeconds, LatencyCollector sos,
                                      int reconnected, long sosAfterKill, long recoveredSos) {
        StringBuilder b = new StringBuilder();
        b.append("\n================ LOAD TEST RESULT ================\n");
        b.append(String.format("Run at            : %s%n", Instant.now()));
        b.append(String.format("Connections       : %d%n", connections));
        b.append(String.format("Publishing cabs   : %d%n", cabs));
        b.append(String.format("Duration          : %d s%n", durationSeconds));
        b.append(String.format("Per-conn queue cap: %d events%n", queueCap));
        b.append("--------------------------------------------------\n");
        b.append(String.format("Locations published: %,d%n", publishedLoc));
        b.append(String.format("SOS published      : %,d%n", publishedSos));
        b.append(String.format("Frames delivered   : %,d%n", totalDelivered));
        b.append(String.format("Delivery throughput: %,.0f frames/s%n", totalDelivered / wallSeconds));
        b.append("--------------------------------------------------\n");
        b.append(String.format("SOS samples        : %,d%n", sos.count()));
        b.append(String.format("SOS latency p50    : %d ms%n", sos.percentile(50)));
        b.append(String.format("SOS latency p99    : %d ms%n", sos.percentile(99)));
        b.append(String.format("SOS latency max    : %d ms%n", sos.max()));
        b.append(String.format("SOS latency mean   : %.1f ms%n", sos.mean()));
        b.append(String.format("SOS p99 < 2000 ms  : %s%n", sos.percentile(99) < 2000 ? "YES" : "NO"));
        b.append("--------------------------------------------------\n");
        b.append(String.format("Node-A killed, reconnected to B: %d clients%n", reconnected));
        b.append(String.format("SOS delivered after kill       : %,d%n", sosAfterKill));
        b.append(String.format("SOS recovered via resume       : %,d (excluded from live latency)%n", recoveredSos));
        b.append("==================================================\n");
        b.append("NOTE: in-process transport -> this is application pipeline latency\n");
        b.append("      (fan-out + bounded queue + conflation + serialization),\n");
        b.append("      not end-to-end TCP/network latency. Environment above.\n");
        return b.toString();
    }

    private static void writeReport(String report) {
        try {
            Path dir = Path.of("benchmarks");
            Files.createDirectories(dir);
            Path out = dir.resolve("load-result-latest.txt");
            Files.writeString(out, report);
            System.out.println("Report written to " + out.toAbsolutePath());
        } catch (IOException e) {
            System.out.println("Could not write report file: " + e.getMessage());
        }
    }

    private static void printEnvironment() {
        Runtime rt = Runtime.getRuntime();
        System.out.printf("JVM=%s %s | OS=%s %s | CPUs=%d | maxHeap=%dMB%n",
                System.getProperty("java.vendor"), System.getProperty("java.version"),
                System.getProperty("os.name"), System.getProperty("os.arch"),
                rt.availableProcessors(), rt.maxMemory() / (1024 * 1024));
    }

    private static int intProp(String key, int def) {
        String v = System.getProperty(key);
        return v == null ? def : Integer.parseInt(v);
    }
}

package com.moveinsync.livetrips.distribution;

import com.moveinsync.livetrips.core.InMemoryEventLog;
import com.moveinsync.livetrips.model.TripEvent;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.moveinsync.livetrips.support.Events.rawLocation;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue 4 regression: live dispatch must be strictly ordered per topic under concurrent publishing,
 * and the bounded dispatch queue must degrade safely (drop + metric + recoverable via replay)
 * rather than blocking or losing events irrecoverably.
 */
class OrderedDispatcherTest {

    /** Records the order in which events were actually distributed. */
    private static final class RecordingDistributor implements EventDistributor {
        final List<Long> seen = new CopyOnWriteArrayList<>();
        volatile CountDownLatch gate; // when set, publish blocks until released

        @Override public void start(EventListener listener) { }
        @Override public void publish(TripEvent event) {
            CountDownLatch g = gate;
            if (g != null) {
                try {
                    g.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            seen.add(event.sequence());
        }
        @Override public void onLocalSubscribe(String topic) { }
        @Override public void onLocalUnsubscribe(String topic) { }
        @Override public String nodeId() { return "test"; }
        @Override public String describe() { return "recording"; }
    }

    private OrderedDispatcher dispatcher;

    @AfterEach
    void tearDown() {
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("Concurrent publishers to one topic are distributed in strict sequence order")
    void strictOrderUnderConcurrentPublishing() throws Exception {
        RecordingDistributor distributor = new RecordingDistributor();
        InMemoryEventLog log = new InMemoryEventLog(100_000);
        dispatcher = new OrderedDispatcher(distributor, 8, 100_000, () -> { });

        String topic = "trip:hot";
        int threads = 8;
        int perThread = 500;
        int total = threads * perThread;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        dispatcher.dispatchOrdered(topic, () -> log.append(rawLocation(topic, "cab-1")));
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
                .until(() -> distributor.seen.size() == total);

        // The distributed order is exactly 1..total ascending: no inversion despite 8 publishers.
        assertThat(distributor.seen).hasSize(total);
        assertThat(distributor.seen).isSorted();
        assertThat(distributor.seen.get(0)).isEqualTo(1L);
        assertThat(distributor.seen.get(total - 1)).isEqualTo((long) total);
    }

    @Test
    @DisplayName("Queue saturation drops live dispatch, counts it, stays unblocked, and stays recoverable from the log")
    void saturationDropsAreCountedAndRecoverable() {
        RecordingDistributor distributor = new RecordingDistributor();
        distributor.gate = new CountDownLatch(1); // block the partition thread
        InMemoryEventLog log = new InMemoryEventLog(10_000);
        AtomicInteger drops = new AtomicInteger();
        // One partition, queue of 1: the first task occupies the thread, the second fills the queue,
        // everything after that must be dropped rather than blocking the publisher.
        dispatcher = new OrderedDispatcher(distributor, 1, 1, drops::incrementAndGet);

        String topic = "trip:saturated";
        int published = 50;
        long t0 = System.nanoTime();
        for (int i = 0; i < published; i++) {
            dispatcher.dispatchOrdered(topic, () -> log.append(rawLocation(topic, "cab-1")));
        }
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        // 1. The publisher was never blocked by the saturated partition.
        assertThat(elapsedMs).as("publishing %d events took %d ms (must not block)", published, elapsedMs)
                .isLessThan(5_000);
        // 2. Saturation is visible: drops were counted (exposed as livetrips.dispatch.dropped).
        assertThat(drops.get()).isGreaterThan(0);
        // 3. Nothing was lost irrecoverably: every event is in the replay log, in order, so a client
        //    that sees the sequence gap recovers all of them by resuming.
        var replay = log.replayFrom(topic, 0);
        assertThat(replay.events()).hasSize(published);
        assertThat(replay.events()).extracting(TripEvent::sequence).isSorted();

        // 4. The partition is not permanently blocked: once the delegate unblocks it keeps working.
        distributor.gate.countDown();
        dispatcher.dispatchOrdered(topic, () -> log.append(rawLocation(topic, "cab-1")));
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !distributor.seen.isEmpty());
        assertThat(distributor.seen).isNotEmpty();
    }

    @Test
    @DisplayName("A saturated partition does not block topics on other partitions")
    void saturationIsIsolatedPerPartition() {
        RecordingDistributor blocked = new RecordingDistributor();
        blocked.gate = new CountDownLatch(1);
        InMemoryEventLog log = new InMemoryEventLog(10_000);
        dispatcher = new OrderedDispatcher(blocked, 8, 1, () -> { });

        // Saturate one topic's partition.
        String hot = "trip:hot";
        for (int i = 0; i < 20; i++) {
            dispatcher.dispatchOrdered(hot, () -> log.append(rawLocation(hot, "cab-1")));
        }

        // A topic on a different partition must still be accepted without blocking.
        String other = findTopicOnDifferentPartition(hot);
        long t0 = System.nanoTime();
        dispatcher.dispatchOrdered(other, () -> log.append(rawLocation(other, "cab-2")));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        assertThat(elapsedMs).isLessThan(2_000);

        blocked.gate.countDown();
    }

    /** Find a topic key that hashes to a different partition than {@code topic}. */
    private String findTopicOnDifferentPartition(String topic) {
        int p = dispatcher.partitionCount();
        int target = Math.floorMod(topic.hashCode(), p);
        for (int i = 0; i < 10_000; i++) {
            String candidate = "trip:other-" + i;
            if (Math.floorMod(candidate.hashCode(), p) != target) {
                return candidate;
            }
        }
        throw new IllegalStateException("no topic on a different partition found");
    }
}

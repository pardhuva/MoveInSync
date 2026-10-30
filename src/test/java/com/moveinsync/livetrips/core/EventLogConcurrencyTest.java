package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.moveinsync.livetrips.support.Events.rawLocation;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The concurrency test the advanced track requires.
 *
 * <p>Ordering, dedup and resume all rest on the sequence number being assigned atomically with the
 * append. This test hammers a single topic from many threads and proves the invariant that the
 * whole system depends on: the sequences assigned are exactly 1..N, with no gaps and no duplicates,
 * and the replay log is sorted by sequence.
 */
class EventLogConcurrencyTest {

    @Test
    @DisplayName("Concurrent publishers to one topic get gap-free, duplicate-free, ordered sequences")
    void concurrentAppendsAreTotallyOrdered() throws Exception {
        int threads = 16;
        int perThread = 5000;
        int total = threads * perThread;
        String topic = "trip:hot";

        // Replay capacity >= total so nothing is evicted and we can inspect the whole log.
        InMemoryEventLog logStore = new InMemoryEventLog(total + 10);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Callable<List<Long>>> tasks = IntStream.range(0, threads)
                .mapToObj(t -> (Callable<List<Long>>) () -> {
                    List<Long> assigned = new ArrayList<>(perThread);
                    for (int i = 0; i < perThread; i++) {
                        TripEvent stored = logStore.append(rawLocation(topic, "cab-" + (i % 100)));
                        assigned.add(stored.sequence());
                    }
                    return assigned;
                })
                .collect(Collectors.toList());

        List<Long> all = new ArrayList<>(total);
        for (Future<List<Long>> f : pool.invokeAll(tasks)) {
            all.addAll(f.get());
        }
        pool.shutdown();

        // Every sequence 1..total appears exactly once.
        assertThat(all).hasSize(total);
        assertThat(all.stream().distinct().count()).isEqualTo(total);
        assertThat(all.stream().min(Long::compareTo)).contains(1L);
        assertThat(all.stream().max(Long::compareTo)).contains((long) total);

        // The replay log itself is sorted by sequence (per-key ordering preserved on the wire).
        ReplayResult replay = logStore.replayFrom(topic, 0);
        List<Long> loggedSeqs = replay.events().stream().map(TripEvent::sequence).toList();
        assertThat(loggedSeqs).isSorted();
        assertThat(loggedSeqs).hasSize(total);
    }
}

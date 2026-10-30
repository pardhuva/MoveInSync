package com.moveinsync.livetrips.distribution;

import com.moveinsync.livetrips.model.TripEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Preserves strict per-topic ordering of live dispatch under concurrent publishing, without ever
 * holding a lock across the distribution fan-out.
 *
 * <h2>The problem it solves</h2>
 * Sequence assignment ({@code EventLog.append}) is atomic, but if {@code distributor.publish} is
 * called afterwards outside any ordering, two threads publishing to one topic can assign seq 5 and 6
 * yet distribute 6 before 5 &mdash; inverting the live wire order.
 *
 * <h2>How it works</h2>
 * Topics are partitioned across {@code partitions} single-thread executors (a topic maps to one by
 * hash), so the same topic is always distributed by the same thread, in submission order. The
 * atomic unit is "assign the sequence + enqueue the distribute task" ({@link #dispatchOrdered}),
 * done under a per-partition lock. That lock spans the (in-memory, for the single-node event log)
 * sequence assignment and the in-memory enqueue only; the actual fan-out ({@code delegate.publish})
 * runs later on the partition thread, never under the lock.
 *
 * <h2>Scope &mdash; single-node vs Redis</h2>
 * In single-node (in-process) mode the event log append is in-memory, so no lock is held across any
 * network call and strict per-topic live ordering is guaranteed (and tested). In Redis mode the
 * append is a sub-millisecond Redis {@code INCR}; the per-partition lock briefly spans it, which is
 * bounded and only contends when two producers publish to the <i>same</i> topic concurrently (rare,
 * since a trip has one cab). More importantly, this orders only <b>same-node</b> dispatch: two
 * gateway nodes publishing to one topic are not mutually ordered by Redis pub/sub, so cross-node
 * live order is reconciled by the per-topic sequence at the client. Strict cross-node live ordering
 * is <b>not</b> provided here.
 *
 * <h2>Saturation</h2>
 * Each partition has a bounded queue. If a partition's distribute falls behind and its queue fills,
 * further tasks are <b>dropped</b> (the {@code onDrop} callback fires) rather than blocking the
 * publisher or growing without limit. A dropped live dispatch is recoverable: the event is already
 * in the replay log (append happened first), so a client sees the sequence gap and resumes. Dropping
 * never permanently blocks a partition &mdash; it keeps processing subsequent tasks.
 */
public class OrderedDispatcher {

    private final EventDistributor delegate;
    private final ThreadPoolExecutor[] partitions;
    private final Object[] locks;
    private final Runnable onDrop;

    public OrderedDispatcher(EventDistributor delegate, int partitionCount, int queueCapacity,
                             Runnable onDrop) {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be >= 1");
        }
        this.delegate = delegate;
        this.onDrop = onDrop == null ? () -> { } : onDrop;
        this.partitions = new ThreadPoolExecutor[partitionCount];
        this.locks = new Object[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            locks[i] = new Object();
            AtomicLong counter = new AtomicLong();
            int idx = i;
            // Single thread per partition => serial, in-submission-order processing for its topics.
            partitions[i] = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(queueCapacity),
                    r -> {
                        Thread t = new Thread(r, "ordered-dispatch-" + idx + "-" + counter.incrementAndGet());
                        t.setDaemon(true);
                        return t;
                    },
                    // Abort on a full queue; the caller catches it and counts a recoverable drop.
                    new ThreadPoolExecutor.AbortPolicy());
            partitions[i].allowCoreThreadTimeOut(true);
        }
    }

    private int indexFor(String topic) {
        return Math.floorMod(topic.hashCode(), partitions.length);
    }

    /**
     * Assign the sequence (via {@code append}) and enqueue the distribution, atomically per topic,
     * so distribution happens in strict sequence order for that topic. Returns the sequenced event.
     * The distribution itself runs later on the partition thread, never under the lock.
     */
    public TripEvent dispatchOrdered(String topic, Supplier<TripEvent> append) {
        int idx = indexFor(topic);
        synchronized (locks[idx]) {
            TripEvent sequenced = append.get();
            try {
                partitions[idx].execute(() -> delegate.publish(sequenced));
            } catch (RuntimeException rejected) {
                // Partition queue saturated: drop the live dispatch (recoverable via replay) rather
                // than block the publisher. The event is already in the log.
                onDrop.run();
            }
            return sequenced;
        }
    }

    /**
     * Distribute immediately, bypassing the ordered partitions. Used for SOS, which must skip ahead
     * of queued traffic everywhere in the pipeline; its true order is preserved by its sequence
     * number and the per-connection express lane.
     */
    public void dispatchImmediate(TripEvent event) {
        delegate.publish(event);
    }

    public void shutdown() {
        for (ThreadPoolExecutor p : partitions) {
            p.shutdownNow();
        }
    }

    public int partitionCount() {
        return partitions.length;
    }
}

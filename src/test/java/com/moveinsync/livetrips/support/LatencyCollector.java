package com.moveinsync.livetrips.support;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Thread-safe collector of latency samples (milliseconds) with percentile computation.
 * Used by the load simulation to report SOS p50/p99.
 */
public class LatencyCollector {

    private final ConcurrentLinkedQueue<Long> samples = new ConcurrentLinkedQueue<>();

    public void record(long millis) {
        samples.add(millis);
    }

    public int count() {
        return samples.size();
    }

    public List<Long> sortedSamples() {
        List<Long> copy = new ArrayList<>(samples);
        Collections.sort(copy);
        return copy;
    }

    /** Percentile in [0,100]; returns 0 if there are no samples. */
    public long percentile(double p) {
        List<Long> sorted = sortedSamples();
        if (sorted.isEmpty()) {
            return 0;
        }
        int idx = (int) Math.ceil((p / 100.0) * sorted.size()) - 1;
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return sorted.get(idx);
    }

    public long max() {
        return sortedSamples().stream().mapToLong(Long::longValue).max().orElse(0);
    }

    public double mean() {
        return sortedSamples().stream().mapToLong(Long::longValue).average().orElse(0);
    }
}

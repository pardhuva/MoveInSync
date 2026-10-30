package com.moveinsync.livetrips.support;

import com.moveinsync.livetrips.gateway.ClientTransport;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * A lightweight transport for load testing. It does not store frames (that would blow up memory at
 * 10k connections); it counts deliveries and, for SOS frames, records end-to-end latency into a
 * shared collector.
 *
 * <p>Latency is measured as {@code now - originTimestampMs}, i.e. from the moment the update was
 * accepted for publishing to the moment it is handed to this transport's {@code send}. In-process,
 * this is the application pipeline latency (sequencing, fan-out, queueing, serialization); it does
 * not include real TCP/network time. The load report states this explicitly.
 */
public class LatencyTransport implements ClientTransport {

    private static final String SOS_MARKER = "\"type\":\"SOS\"";
    private static final String EVENT_MARKER = "\"kind\":\"event\"";
    private static final String REPLAY_MARKER = "\"replay\":true";
    private static final String TS_KEY = "\"ts\":";

    private final String id;
    private final LatencyCollector sosLatencies;
    private final LongAdder delivered;
    private final LongAdder recoveredSos;
    private final AtomicBoolean open = new AtomicBoolean(true);

    public LatencyTransport(String id, LatencyCollector sosLatencies, LongAdder delivered) {
        this(id, sosLatencies, delivered, new LongAdder());
    }

    public LatencyTransport(String id, LatencyCollector sosLatencies, LongAdder delivered,
                            LongAdder recoveredSos) {
        this.id = id;
        this.sosLatencies = sosLatencies;
        this.delivered = delivered;
        this.recoveredSos = recoveredSos;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public void send(String payload) {
        if (!open.get()) {
            throw new IllegalStateException("closed");
        }
        delivered.increment();
        // Cheap check: only pay the parse cost for the rare SOS frame.
        if (payload.indexOf(SOS_MARKER) < 0) {
            return;
        }
        // Only a live SOS *event* counts toward the 2s live-delivery target. Deliberately excluded:
        //  - snapshot frames (kind != event), which embed current state rather than deliver an alert;
        //  - replayed events (resume), whose age includes however long the client was offline --
        //    counting those would measure the client's disconnect, not the system's delivery.
        // Recovered SOS are counted separately so recovery completeness is still visible.
        if (payload.indexOf(EVENT_MARKER) < 0) {
            return;
        }
        if (payload.indexOf(REPLAY_MARKER) >= 0) {
            recoveredSos.increment();
            return;
        }
        long ts = extractTs(payload);
        if (ts > 0) {
            sosLatencies.record(System.currentTimeMillis() - ts);
        }
    }

    public long recoveredSosCount() {
        return recoveredSos.sum();
    }

    private static long extractTs(String payload) {
        int i = payload.indexOf(TS_KEY);
        if (i < 0) {
            return -1;
        }
        int start = i + TS_KEY.length();
        int end = start;
        while (end < payload.length() && (Character.isDigit(payload.charAt(end)))) {
            end++;
        }
        try {
            return Long.parseLong(payload.substring(start, end));
        } catch (RuntimeException e) {
            return -1;
        }
    }

    @Override
    public void close(String reason) {
        open.set(false);
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }
}

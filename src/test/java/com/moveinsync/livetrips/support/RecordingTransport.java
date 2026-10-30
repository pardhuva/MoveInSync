package com.moveinsync.livetrips.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.livetrips.gateway.ClientTransport;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A fake WebSocket client that records every frame the server sends it. This is what lets the whole
 * delivery pipeline be asserted in-process: no browser, no network, no servlet container.
 */
public class RecordingTransport implements ClientTransport {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String id;
    private final List<String> frames = new CopyOnWriteArrayList<>();
    private final AtomicBoolean open = new AtomicBoolean(true);
    private volatile String closeReason;

    public RecordingTransport(String id) {
        this.id = id;
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
        frames.add(payload);
    }

    @Override
    public void close(String reason) {
        if (open.compareAndSet(true, false)) {
            this.closeReason = reason;
        }
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    public String closeReason() {
        return closeReason;
    }

    /** All frames, as raw JSON. */
    public List<String> frames() {
        return new ArrayList<>(frames);
    }

    /** Only the "event" frames, parsed to maps, in delivery order. */
    public List<Map<String, Object>> events() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String f : frames) {
            Map<String, Object> m = parse(f);
            if ("event".equals(m.get("kind"))) {
                out.add(m);
            }
        }
        return out;
    }

    /** Event frames of a given type (LOCATION/STATUS/SOS), in order. */
    public List<Map<String, Object>> eventsOfType(String type) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> e : events()) {
            if (type.equals(e.get("type"))) {
                out.add(e);
            }
        }
        return out;
    }

    public List<Map<String, Object>> framesOfKind(String kind) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String f : frames) {
            Map<String, Object> m = parse(f);
            if (kind.equals(m.get("kind"))) {
                out.add(m);
            }
        }
        return out;
    }

    public long lastSeqOfType(String type) {
        List<Map<String, Object>> evs = eventsOfType(type);
        if (evs.isEmpty()) {
            return 0;
        }
        return ((Number) evs.get(evs.size() - 1).get("seq")).longValue();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) {
        try {
            return MAPPER.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

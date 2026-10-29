package com.moveinsync.livetrips.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Explicit JSON codec for {@link TripEvent}.
 *
 * <p>Written by hand rather than relying on reflective binding so the wire format is a
 * deliberate, stable contract: the WebSocket clients, the Redis backbone and the tests all
 * speak exactly these field names, and adding a field to the domain object cannot silently
 * change the protocol.
 *
 * <p>Thread-safe ({@link ObjectMapper} is, once configured).
 */
public class TripEventCodec {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };

    private final ObjectMapper mapper;

    public TripEventCodec(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public TripEventCodec() {
        this(new ObjectMapper());
    }

    /** Field-by-field map form, used both for JSON and as the WebSocket frame body. */
    public Map<String, Object> toMap(TripEvent e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("eventId", e.eventId());
        m.put("topic", e.topic());
        m.put("seq", e.sequence());
        m.put("type", e.type().name());
        if (e.tripId() != null) m.put("tripId", e.tripId());
        if (e.cabId() != null) m.put("cabId", e.cabId());
        if (e.officeId() != null) m.put("officeId", e.officeId());
        if (!e.payload().isEmpty()) m.put("payload", e.payload());
        m.put("ts", e.originTimestampMs());
        return m;
    }

    @SuppressWarnings("unchecked")
    public TripEvent fromMap(Map<String, Object> m) {
        Object payload = m.get("payload");
        return TripEvent.builder()
                .eventId(asString(m.get("eventId")))
                .topic(asString(m.get("topic")))
                .sequence(asLong(m.get("seq")))
                .type(EventType.valueOf(asString(m.get("type"))))
                .tripId(asString(m.get("tripId")))
                .cabId(asString(m.get("cabId")))
                .officeId(asString(m.get("officeId")))
                .payload(payload instanceof Map ? (Map<String, Object>) payload : null)
                .originTimestampMs(asLong(m.get("ts")))
                .build();
    }

    public String encode(TripEvent e) {
        try {
            return mapper.writeValueAsString(toMap(e));
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to encode event", ex);
        }
    }

    public TripEvent decode(String json) {
        try {
            return fromMap(mapper.readValue(json, MAP_TYPE));
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to decode event", ex);
        }
    }

    private static String asString(Object o) {
        return o == null ? null : o.toString();
    }

    private static long asLong(Object o) {
        if (o == null) return 0L;
        if (o instanceof Number n) return n.longValue();
        return Long.parseLong(o.toString());
    }
}

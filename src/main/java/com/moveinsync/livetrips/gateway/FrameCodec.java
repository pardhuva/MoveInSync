package com.moveinsync.livetrips.gateway;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encodes server&rarr;client frames and decodes client&rarr;server frames.
 *
 * <h2>Wire protocol</h2>
 * All frames are JSON text. Every server frame carries a {@code "kind"} discriminator; every
 * client frame carries an {@code "action"}.
 *
 * <p><b>Client &rarr; server</b>
 * <pre>
 * {"action":"authenticate","token":"&lt;jwt&gt;"}
 * {"action":"subscribe","topic":"trip:T-1","lastSeq":42}   // lastSeq omitted or 0 = fresh subscribe
 * {"action":"unsubscribe","topic":"trip:T-1"}
 * {"action":"ping"}
 * </pre>
 *
 * <p><b>Server &rarr; client</b>
 * <pre>
 * {"kind":"welcome","nodeId":"gw-1"}
 * {"kind":"authenticated","userId":"u1","role":"EMPLOYEE"}
 * {"kind":"subscribed","topic":"trip:T-1","mode":"REPLAY|SNAPSHOT|LIVE","latestSeq":57}
 * {"kind":"event","topic":"trip:T-1","seq":58,"type":"SOS",...}
 * {"kind":"snapshot","topic":"trip:T-1","atSeq":57,"locations":{...},"statuses":{...},"sos":[...]}
 * {"kind":"error","code":"FORBIDDEN","message":"not permitted to subscribe to this topic"}
 * {"kind":"closing","reason":"SLOW_CONSUMER"}
 * {"kind":"pong"}
 * </pre>
 */
public class FrameCodec {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() { };

    private final ObjectMapper mapper;
    private final TripEventCodec eventCodec;

    public FrameCodec(ObjectMapper mapper, TripEventCodec eventCodec) {
        this.mapper = mapper;
        this.eventCodec = eventCodec;
    }

    public FrameCodec() {
        this(new ObjectMapper(), new TripEventCodec());
    }

    // ---------- client -> server ----------

    public Map<String, Object> decodeClientFrame(String json) {
        try {
            return mapper.readValue(json, MAP_TYPE);
        } catch (Exception ex) {
            throw new IllegalArgumentException("malformed frame");
        }
    }

    // ---------- server -> client ----------

    public String event(TripEvent e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kind", "event");
        m.putAll(eventCodec.toMap(e));
        return write(m);
    }

    /**
     * An event delivered as part of a resume (replay window or critical history), marked
     * {@code "replay": true}.
     *
     * <p>This distinction matters to clients and to measurement. A security desk must not re-sound
     * an alarm for an SOS it is merely recovering after a reconnect, and latency against the 2s SOS
     * target must be measured on live delivery &mdash; a replayed event's age includes however long
     * the client was disconnected, which the server cannot bound.
     */
    public String replayEvent(TripEvent e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kind", "event");
        m.putAll(eventCodec.toMap(e));
        m.put("replay", true);
        return write(m);
    }

    public String snapshot(TopicSnapshot snap) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("kind", "snapshot");
        m.put("topic", snap.topic());
        m.put("atSeq", snap.atSequence());

        Map<String, Object> locations = new LinkedHashMap<>();
        snap.latestLocationByCab().forEach((cab, ev) -> locations.put(cab, eventCodec.toMap(ev)));
        m.put("locations", locations);

        Map<String, Object> statuses = new LinkedHashMap<>();
        snap.latestStatusByTrip().forEach((trip, ev) -> statuses.put(trip, eventCodec.toMap(ev)));
        m.put("statuses", statuses);

        List<Object> sos = new ArrayList<>();
        snap.activeSosAlerts().forEach(ev -> sos.add(eventCodec.toMap(ev)));
        m.put("sos", sos);

        return write(m);
    }

    public String welcome(String nodeId) {
        return write(Map.of("kind", "welcome", "nodeId", nodeId));
    }

    public String authenticated(String userId, String role) {
        return write(Map.of("kind", "authenticated", "userId", userId, "role", role));
    }

    public String subscribed(String topic, String mode, long latestSeq) {
        return write(Map.of("kind", "subscribed", "topic", topic, "mode", mode, "latestSeq", latestSeq));
    }

    /**
     * Subscribe ack that also tells the client whether the critical (STATUS/SOS) history was
     * truncated for its resume horizon. When true, some intermediate critical events could not be
     * replayed and only the snapshot's latest state is authoritative for what is missing.
     */
    public String subscribed(String topic, String mode, long latestSeq, boolean criticalTruncated) {
        return write(Map.of("kind", "subscribed", "topic", topic, "mode", mode,
                "latestSeq", latestSeq, "criticalTruncated", criticalTruncated));
    }

    public String unsubscribed(String topic) {
        return write(Map.of("kind", "unsubscribed", "topic", topic));
    }

    /** Error frames carry a stable machine code and a message that never leaks internals or PII. */
    public String error(String code, String safeMessage) {
        return write(Map.of("kind", "error", "code", code, "message", safeMessage));
    }

    public String closing(String reason) {
        return write(Map.of("kind", "closing", "reason", reason));
    }

    public String pong() {
        return write(Map.of("kind", "pong"));
    }

    private String write(Map<String, Object> m) {
        try {
            return mapper.writeValueAsString(m);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to encode frame", ex);
        }
    }
}

package com.moveinsync.livetrips.support;

import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;

import java.util.Map;
import java.util.UUID;

/**
 * Tiny builders so tests read as behaviour, not as event construction. Every helper stamps a
 * distinct eventId and a caller-supplied sequence; the origin timestamp defaults to "now" but can
 * be set for latency assertions.
 */
public final class Events {

    private Events() {
    }

    public static TripEvent location(String topic, long seq, String cabId, double lat, double lng) {
        return TripEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .topic(topic).sequence(seq).type(EventType.LOCATION)
                .tripId("trip-1").cabId(cabId).officeId("office-1")
                .payload(Map.of("lat", lat, "lng", lng))
                .originTimestampMs(System.currentTimeMillis())
                .build();
    }

    public static TripEvent status(String topic, long seq, String tripId, String status) {
        return TripEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .topic(topic).sequence(seq).type(EventType.STATUS)
                .tripId(tripId).cabId("cab-1").officeId("office-1")
                .payload(Map.of("status", status))
                .originTimestampMs(System.currentTimeMillis())
                .build();
    }

    public static TripEvent sos(String topic, long seq, String tripId) {
        return TripEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .topic(topic).sequence(seq).type(EventType.SOS)
                .tripId(tripId).cabId("cab-1").officeId("office-1")
                .payload(Map.of("reasonCode", "PANIC"))
                .originTimestampMs(System.currentTimeMillis())
                .build();
    }

    /** An un-sequenced event (sequence 0), as produced before it enters the event log. */
    public static TripEvent rawLocation(String topic, String cabId) {
        return location(topic, 0, cabId, 12.9, 77.6);
    }
}

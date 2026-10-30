package com.moveinsync.livetrips.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures the actual JSON wire size of representative events, so the memory figures in the docs
 * are grounded in a measurement rather than a guess.
 *
 * <p>This measures the <b>serialized wire size</b> (the bytes put on the WebSocket). The in-heap
 * footprint of a {@link TripEvent} object is larger (object headers, boxed values, the payload map,
 * string overhead) and is stated separately in the docs as an estimate, not a measurement &mdash;
 * measuring true heap retention would need a tool such as JOL, which this project does not pull in.
 *
 * <p>The test prints the measured sizes and asserts only loose sanity bounds, so it documents
 * reality without becoming brittle to a field rename.
 */
class EventWireSizeTest {

    private final TripEventCodec codec = new TripEventCodec(new ObjectMapper());

    private TripEvent event(EventType type, Map<String, Object> payload) {
        return TripEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .topic("trip:trip-100").sequence(123_456L).type(type)
                .tripId("trip-100").cabId("cab-7").officeId("office-BLR-1")
                .payload(payload).originTimestampMs(1_793_348_490_767L)
                .build();
    }

    private int wireBytes(TripEvent e) {
        return codec.encode(e).getBytes(StandardCharsets.UTF_8).length;
    }

    @Test
    @DisplayName("Representative events have a measured JSON wire size on the order of ~200-350 bytes")
    void measureWireSizes() {
        int location = wireBytes(event(EventType.LOCATION, Map.of("lat", 12.9716, "lng", 77.5946, "speedKph", 42.5)));
        int status = wireBytes(event(EventType.STATUS, Map.of("status", "DELAYED", "delayMinutes", 5)));
        int sos = wireBytes(event(EventType.SOS, Map.of("reasonCode", "PANIC")));

        System.out.printf("%n=== MEASURED EVENT WIRE SIZE (JSON, UTF-8 bytes) ===%n");
        System.out.printf("LOCATION : %d bytes%n", location);
        System.out.printf("STATUS   : %d bytes%n", status);
        System.out.printf("SOS      : %d bytes%n", sos);
        System.out.printf("====================================================%n");

        // Loose sanity bounds: every representative event is a small frame, well under 1 KB.
        assertThat(location).isBetween(120, 600);
        assertThat(status).isBetween(120, 600);
        assertThat(sos).isBetween(100, 600);
    }
}

package com.moveinsync.livetrips.service;

import com.moveinsync.livetrips.model.EventType;

import java.util.Map;

/**
 * A request to publish one update, already separated from its HTTP representation so the service
 * layer has no dependency on the web layer.
 *
 * @param tripId  the trip the update belongs to; everything else (cab, office, tenant) is resolved
 *                from the trip record rather than trusted from the caller
 * @param type    LOCATION, STATUS or SOS
 * @param payload non-PII details (lat/lng, status code, SOS reason code)
 */
public record PublishCommand(String tripId, EventType type, Map<String, Object> payload) {

    public PublishCommand {
        if (tripId == null || tripId.isBlank()) {
            throw new IllegalArgumentException("tripId is required");
        }
        if (type == null) {
            throw new IllegalArgumentException("type is required");
        }
        if (type == EventType.SNAPSHOT) {
            // SNAPSHOT is server-generated; a producer may never inject one.
            throw new IllegalArgumentException("SNAPSHOT may not be published");
        }
        payload = payload == null ? Map.of() : Map.copyOf(payload);
    }
}

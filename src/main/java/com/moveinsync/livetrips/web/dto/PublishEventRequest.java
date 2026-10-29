package com.moveinsync.livetrips.web.dto;

import com.moveinsync.livetrips.model.EventType;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A cab's update, as it arrives over HTTP.
 *
 * <p>Fields are explicit rather than a free-form map so the payload is validated at the edge: a
 * latitude outside [-90, 90] or an oversized status string is rejected before it can reach the
 * event log and be fanned out to thousands of screens.
 *
 * <p>Note what is <i>not</i> here: no cab id, no office id, no tenant. Those are resolved from the
 * trip record server-side, so a producer cannot relabel its event as belonging to a different cab
 * or office.
 */
public record PublishEventRequest(

        @NotNull(message = "type is required")
        EventType type,

        @DecimalMin(value = "-90.0", message = "lat must be between -90 and 90")
        @DecimalMax(value = "90.0", message = "lat must be between -90 and 90")
        Double lat,

        @DecimalMin(value = "-180.0", message = "lng must be between -180 and 180")
        @DecimalMax(value = "180.0", message = "lng must be between -180 and 180")
        Double lng,

        /** Speed in km/h, optional telemetry. */
        @DecimalMin(value = "0.0", message = "speedKph must not be negative")
        Double speedKph,

        /** For STATUS: ASSIGNED, STARTED, DELAYED, COMPLETED, CANCELLED. */
        @Size(max = 32, message = "status must be at most 32 characters")
        String status,

        /** For SOS: a short reason code, never free text that might contain personal detail. */
        @Size(max = 32, message = "reasonCode must be at most 32 characters")
        String reasonCode,

        /** For STATUS=DELAYED: how late, in minutes. */
        Integer delayMinutes
) {

    /** Validation that depends on {@code type} and so cannot be expressed with field annotations. */
    public void validateForType() {
        switch (type) {
            case LOCATION -> {
                if (lat == null || lng == null) {
                    throw new IllegalArgumentException("lat and lng are required for LOCATION");
                }
            }
            case STATUS -> {
                if (status == null || status.isBlank()) {
                    throw new IllegalArgumentException("status is required for STATUS");
                }
            }
            case SOS -> { /* reasonCode is optional: an SOS must never be rejected for detail. */ }
            case SNAPSHOT -> throw new IllegalArgumentException("SNAPSHOT may not be published");
        }
    }

    /** The non-PII payload carried on the event. */
    public Map<String, Object> toPayload() {
        Map<String, Object> payload = new LinkedHashMap<>();
        if (lat != null) payload.put("lat", lat);
        if (lng != null) payload.put("lng", lng);
        if (speedKph != null) payload.put("speedKph", speedKph);
        if (status != null) payload.put("status", status.toUpperCase());
        if (reasonCode != null) payload.put("reasonCode", reasonCode);
        if (delayMinutes != null) payload.put("delayMinutes", delayMinutes);
        return payload;
    }
}

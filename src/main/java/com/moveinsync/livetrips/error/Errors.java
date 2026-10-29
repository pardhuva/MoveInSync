package com.moveinsync.livetrips.error;

import org.springframework.http.HttpStatus;

/**
 * Factory for the service's error cases. Centralising them keeps the client-facing wording
 * consistent and makes it easy to audit that no message leaks information.
 *
 * <p>Note the deliberate ambiguity in {@link #forbidden()}: a caller who is not permitted to see a
 * trip is told only that they are not permitted. They are never told whether the trip exists,
 * because that alone would let an attacker enumerate another tenant's trip ids.
 */
public final class Errors {

    private Errors() {
    }

    public static ApiException unauthenticated() {
        return new ApiException("UNAUTHENTICATED", HttpStatus.UNAUTHORIZED,
                "a valid bearer token is required");
    }

    public static ApiException invalidToken() {
        return new ApiException("INVALID_TOKEN", HttpStatus.UNAUTHORIZED,
                "token is invalid or expired");
    }

    public static ApiException forbidden() {
        return new ApiException("FORBIDDEN", HttpStatus.FORBIDDEN,
                "not permitted to access this resource");
    }

    public static ApiException badRequest(String safeMessage) {
        return new ApiException("BAD_REQUEST", HttpStatus.BAD_REQUEST, safeMessage);
    }

    public static ApiException unknownTrip() {
        // Same status and shape as forbidden-on-purpose cases would be ideal, but a caller
        // authorised for the tenant genuinely needs to distinguish "no such trip".
        return new ApiException("UNKNOWN_TRIP", HttpStatus.NOT_FOUND, "no such trip");
    }

    public static ApiException internal() {
        return new ApiException("INTERNAL_ERROR", HttpStatus.INTERNAL_SERVER_ERROR,
                "the request could not be completed");
    }
}

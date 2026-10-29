package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.model.TripMeta;

/**
 * Read-only view of the ownership metadata the authorizer needs. Implemented by the
 * repository (authoritative) and by the caching decorator, so
 * {@link SubscriptionAuthorizer} can be unit-tested against a trivial fake with no
 * database or Spring context.
 */
public interface TripMetadataSource {

    /** Ownership metadata for a trip, or null if the trip is unknown. */
    TripMeta findTrip(String tripId);

    /** Ownership metadata for the cab's currently active trip, or null if the cab is idle/unknown. */
    TripMeta findActiveTripForCab(String cabId);

    /** The tenant (client) that owns an office, or null if the office is unknown. */
    String findOfficeTenant(String officeId);
}

package com.moveinsync.livetrips.repository;

import com.moveinsync.livetrips.core.TripMetadataSource;
import com.moveinsync.livetrips.model.TripMeta;

import java.util.Collection;

/**
 * Repository layer for trip ownership metadata. In a production deployment this would be
 * backed by the relational store that owns trips; here it is an in-memory implementation
 * (see {@link InMemoryTripRepository}) because the case study is about the fan-out layer,
 * not about trip persistence.
 *
 * <p>Extends {@link TripMetadataSource} so it can be handed directly to the authorizer,
 * with or without a cache in front of it.
 */
public interface TripRepository extends TripMetadataSource {

    /** Register (or replace) a trip's ownership metadata. */
    void save(TripMeta meta);

    /** Mark a trip as no longer active, so its cab stops resolving to it. */
    void endTrip(String tripId);

    /** Register the tenant that owns an office. */
    void registerOffice(String officeId, String tenantId);

    Collection<TripMeta> findAll();

    int tripCount();
}

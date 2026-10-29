package com.moveinsync.livetrips.repository;

import com.moveinsync.livetrips.model.TripMeta;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory {@link TripRepository}. Authoritative source of trip ownership for this service.
 *
 * <p>Two indexes are maintained so both {@code trip:} and {@code cab:} topics resolve in O(1):
 * trip id &rarr; metadata, and cab id &rarr; active trip id.
 *
 * <p>Thread-safe via {@link ConcurrentHashMap}; writes (trip start/end) are rare compared to
 * reads (one per subscribe).
 */
@Repository
public class InMemoryTripRepository implements TripRepository {

    private final Map<String, TripMeta> tripsById = new ConcurrentHashMap<>();
    private final Map<String, String> activeTripByCab = new ConcurrentHashMap<>();
    private final Map<String, String> tenantByOffice = new ConcurrentHashMap<>();

    @Override
    public void save(TripMeta meta) {
        tripsById.put(meta.tripId(), meta);
        activeTripByCab.put(meta.cabId(), meta.tripId());
        tenantByOffice.putIfAbsent(meta.officeId(), meta.tenantId());
    }

    @Override
    public void endTrip(String tripId) {
        TripMeta meta = tripsById.get(tripId);
        if (meta != null) {
            // Only clear the cab index if it still points at this trip.
            activeTripByCab.remove(meta.cabId(), tripId);
        }
    }

    @Override
    public void registerOffice(String officeId, String tenantId) {
        tenantByOffice.put(officeId, tenantId);
    }

    @Override
    public TripMeta findTrip(String tripId) {
        return tripId == null ? null : tripsById.get(tripId);
    }

    @Override
    public TripMeta findActiveTripForCab(String cabId) {
        if (cabId == null) {
            return null;
        }
        String tripId = activeTripByCab.get(cabId);
        return tripId == null ? null : tripsById.get(tripId);
    }

    @Override
    public String findOfficeTenant(String officeId) {
        return officeId == null ? null : tenantByOffice.get(officeId);
    }

    @Override
    public Collection<TripMeta> findAll() {
        return Collections.unmodifiableCollection(tripsById.values());
    }

    @Override
    public int tripCount() {
        return tripsById.size();
    }
}

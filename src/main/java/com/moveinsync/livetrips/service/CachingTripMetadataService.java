package com.moveinsync.livetrips.service;

import com.moveinsync.livetrips.core.BoundedTtlCache;
import com.moveinsync.livetrips.core.TripMetadataSource;
import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.repository.TripRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Caching decorator over {@link TripRepository} for authorisation metadata, which is read
 * once per subscribe and is therefore the hottest read in the system.
 *
 * <h2>What can go stale, for how long, and how it is bounded</h2>
 * <ul>
 *   <li><b>What:</b> trip ownership (which employee owns a trip, which office and tenant a trip
 *       belongs to) and office&rarr;tenant mapping.</li>
 *   <li><b>How long:</b> at most the cache TTL ({@code livetrips.cache.ttl-ms}, default 30s).
 *       After that the entry is treated as absent and re-read from the repository.</li>
 *   <li><b>How bounded:</b> TTL bounds staleness in time; LRU with
 *       {@code livetrips.cache.max-entries} bounds it in space. On a write that changes
 *       ownership we also {@link #invalidateTrip(String) invalidate} explicitly, so the
 *       common case is immediate consistency and the TTL is only the backstop for changes
 *       made by another node.</li>
 * </ul>
 *
 * <h2>Why this is safe for a security check</h2>
 * A stale cache entry could in principle let a user keep access for up to the TTL after their
 * access is revoked. That is an explicit, documented trade-off: a 30-second window on
 * <i>revocation</i> in exchange for removing a database read from every subscribe. It can never
 * <i>grant</i> access to a different tenant, because a cache entry is only ever populated from
 * the authoritative record for that same key. For deployments that cannot accept the revocation
 * window, set {@code livetrips.cache.ttl-ms=0} to disable caching.
 */
@Service
public class CachingTripMetadataService implements TripMetadataSource {

    private final TripRepository repository;
    private final BoundedTtlCache<String, TripMeta> tripCache;
    private final BoundedTtlCache<String, TripMeta> cabCache;
    private final BoundedTtlCache<String, String> officeCache;
    private final boolean enabled;

    public CachingTripMetadataService(
            TripRepository repository,
            @Value("${livetrips.cache.max-entries:50000}") int maxEntries,
            @Value("${livetrips.cache.ttl-ms:30000}") long ttlMs) {
        this.repository = repository;
        this.enabled = ttlMs > 0;
        this.tripCache = new BoundedTtlCache<>(maxEntries, ttlMs);
        this.cabCache = new BoundedTtlCache<>(maxEntries, ttlMs);
        this.officeCache = new BoundedTtlCache<>(Math.max(1, maxEntries / 10), ttlMs);
    }

    @Override
    public TripMeta findTrip(String tripId) {
        if (!enabled || tripId == null) {
            return repository.findTrip(tripId);
        }
        TripMeta cached = tripCache.get(tripId);
        if (cached != null) {
            return cached;
        }
        TripMeta fresh = repository.findTrip(tripId);
        if (fresh != null) {
            tripCache.put(tripId, fresh);
        }
        // Negative results are intentionally not cached: an unknown trip is usually a trip that
        // is about to be created, and caching the miss would delay legitimate access.
        return fresh;
    }

    @Override
    public TripMeta findActiveTripForCab(String cabId) {
        if (!enabled || cabId == null) {
            return repository.findActiveTripForCab(cabId);
        }
        TripMeta cached = cabCache.get(cabId);
        if (cached != null) {
            return cached;
        }
        TripMeta fresh = repository.findActiveTripForCab(cabId);
        if (fresh != null) {
            cabCache.put(cabId, fresh);
        }
        return fresh;
    }

    @Override
    public String findOfficeTenant(String officeId) {
        if (!enabled || officeId == null) {
            return repository.findOfficeTenant(officeId);
        }
        String cached = officeCache.get(officeId);
        if (cached != null) {
            return cached;
        }
        String fresh = repository.findOfficeTenant(officeId);
        if (fresh != null) {
            officeCache.put(officeId, fresh);
        }
        return fresh;
    }

    /** Called whenever a trip's ownership changes, to avoid waiting out the TTL. */
    public void invalidateTrip(String tripId) {
        TripMeta meta = repository.findTrip(tripId);
        tripCache.invalidate(tripId);
        if (meta != null) {
            cabCache.invalidate(meta.cabId());
        }
    }

    public long stalenessBoundMillis() {
        return enabled ? tripCache.stalenessBoundMillis() : 0L;
    }

    public long cacheHits() { return tripCache.hits() + cabCache.hits() + officeCache.hits(); }
    public long cacheMisses() { return tripCache.misses() + cabCache.misses() + officeCache.misses(); }
    public long cacheEvictions() { return tripCache.evictions() + cabCache.evictions() + officeCache.evictions(); }
    public int cachedTrips() { return tripCache.size(); }
}

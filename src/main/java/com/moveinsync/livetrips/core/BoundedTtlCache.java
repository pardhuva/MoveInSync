package com.moveinsync.livetrips.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * A small bounded cache with LRU eviction and a per-entry TTL.
 *
 * <p>Used for authorisation metadata, which is read on every subscribe. Two independent
 * bounds, both deliberate:
 *
 * <ul>
 *   <li><b>Size bound (LRU).</b> At most {@code maxEntries}; the least-recently-used entry is
 *       evicted. This caps memory regardless of how many distinct trips exist.</li>
 *   <li><b>Time bound (TTL).</b> An entry older than {@code ttlMillis} is treated as absent and
 *       re-read from the authoritative source. <b>This is the staleness bound:</b> a change to
 *       trip ownership can be served from cache for at most {@code ttlMillis} after it happens.</li>
 * </ul>
 *
 * <p>Complexity: O(1) get and put (amortised). Space: O(maxEntries).
 *
 * <p>The clock is injectable so TTL behaviour is unit-testable without sleeping.
 */
public class BoundedTtlCache<K, V> {

    private final long ttlMillis;
    private final LongSupplier clock;
    private final LinkedHashMap<K, CacheEntry<V>> map;

    private long hits = 0;
    private long misses = 0;
    private long evictions = 0;
    private long expirations = 0;

    public BoundedTtlCache(int maxEntries, long ttlMillis) {
        this(maxEntries, ttlMillis, System::currentTimeMillis);
    }

    public BoundedTtlCache(int maxEntries, long ttlMillis, LongSupplier clock) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be >= 1");
        }
        this.ttlMillis = ttlMillis;
        this.clock = clock;
        this.map = new LinkedHashMap<>(Math.min(maxEntries, 256), 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, CacheEntry<V>> eldest) {
                boolean evict = size() > maxEntries;
                if (evict) {
                    evictions++;
                }
                return evict;
            }
        };
    }

    /** Returns the cached value, or null if absent or expired. */
    public synchronized V get(K key) {
        CacheEntry<V> e = map.get(key);
        if (e == null) {
            misses++;
            return null;
        }
        if (clock.getAsLong() - e.storedAt() > ttlMillis) {
            map.remove(key);
            expirations++;
            misses++;
            return null;
        }
        hits++;
        return e.value();
    }

    public synchronized void put(K key, V value) {
        map.put(key, new CacheEntry<>(value, clock.getAsLong()));
    }

    public synchronized void invalidate(K key) {
        map.remove(key);
    }

    public synchronized void clear() {
        map.clear();
    }

    public synchronized int size() { return map.size(); }
    public synchronized long hits() { return hits; }
    public synchronized long misses() { return misses; }
    public synchronized long evictions() { return evictions; }
    public synchronized long expirations() { return expirations; }

    /** The maximum time a change at the source can go unseen by readers of this cache. */
    public long stalenessBoundMillis() { return ttlMillis; }

    private record CacheEntry<V>(V value, long storedAt) { }
}

package com.moveinsync.livetrips.core;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cache behind the authorization hot path. Its two bounds are the staleness and memory
 * guarantees the case study asks to be stated: TTL bounds how long a stale entry can be served,
 * LRU bounds how many entries exist. A manual clock makes the TTL testable without sleeping.
 */
class BoundedTtlCacheTest {

    @Test
    @DisplayName("Entries expire once the TTL elapses (staleness is bounded in time)")
    void entriesExpireAfterTtl() {
        AtomicLong now = new AtomicLong(0);
        BoundedTtlCache<String, String> cache = new BoundedTtlCache<>(100, 1000, now::get);
        cache.put("k", "v");

        now.set(999);
        assertThat(cache.get("k")).isEqualTo("v"); // still fresh

        now.set(1001);
        assertThat(cache.get("k")).isNull();        // expired: forces a re-read of authoritative data
        assertThat(cache.expirations()).isEqualTo(1);
    }

    @Test
    @DisplayName("Least-recently-used entries are evicted once capacity is exceeded (memory bound)")
    void evictsLeastRecentlyUsed() {
        BoundedTtlCache<Integer, Integer> cache = new BoundedTtlCache<>(3, Long.MAX_VALUE);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.get(1);          // touch 1 so 2 becomes least-recently-used
        cache.put(4, 4);       // evicts 2

        assertThat(cache.get(2)).isNull();
        assertThat(cache.get(1)).isEqualTo(1);
        assertThat(cache.get(3)).isEqualTo(3);
        assertThat(cache.get(4)).isEqualTo(4);
        assertThat(cache.evictions()).isEqualTo(1);
    }

    @Test
    @DisplayName("Invalidation makes an ownership change visible immediately, before the TTL")
    void invalidationIsImmediate() {
        BoundedTtlCache<String, String> cache = new BoundedTtlCache<>(100, Long.MAX_VALUE);
        cache.put("trip-1", "acme");
        cache.invalidate("trip-1");
        assertThat(cache.get("trip-1")).isNull();
    }
}

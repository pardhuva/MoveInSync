package com.moveinsync.livetrips.redis;

import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.core.CriticalHistory;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.SnapshotStore;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Multi-node {@link SnapshotStore}. Together with {@link RedisEventLog} this is what lets a
 * client whose gateway node crashed reconnect to a different node and be made whole: the latest
 * state of every topic is in Redis, not in the dead node's heap.
 *
 * <h2>Keys (all hashes, so a write is O(1) and bounded by the topic's cab/trip count)</h2>
 * <ul>
 *   <li>{@code lt:snap:loc:<topic>} &mdash; cabId &rarr; latest LOCATION event</li>
 *   <li>{@code lt:snap:st:<topic>} &mdash; tripId &rarr; latest STATUS event</li>
 *   <li>{@code lt:snap:sos:<topic>} &mdash; eventId &rarr; unresolved SOS event</li>
 * </ul>
 *
 * <p>Each write refreshes a TTL so state for finished trips is reclaimed automatically.
 *
 * <h2>Sequence-monotonic writes</h2>
 * LOCATION and STATUS keep the latest value per key. Because {@code record} runs off any ordering
 * lock (and, across nodes, from more than one writer), a plain {@code HSET} could let an older event
 * overwrite a newer one. So those writes go through a Lua compare-and-set that overwrites only when
 * the new sequence is greater than the stored one &mdash; making {@code record} commutative. Values
 * are stored as {@code "<seq>|<json>"} so the script can compare the sequence without parsing JSON.
 * SOS is keyed by its unique {@code eventId}, so it needs no comparison.
 *
 * <p><b>Unverified against real Redis.</b> This class is covered only by the conditionally-enabled
 * {@code RedisEventLogIntegrationTest} family, which is skipped when no Redis is reachable.
 *
 * <h2>Complexity</h2>
 * {@code record}: O(1), one round trip. {@code snapshot}: three {@code HGETALL}s, O(cabs + trips
 * + open SOS) &mdash; paid only when a client falls outside the replay window.
 */
public class RedisSnapshotStore implements SnapshotStore {

    private static final String LOC_PREFIX = "lt:snap:loc:";
    private static final String STATUS_PREFIX = "lt:snap:st:";
    private static final String SOS_PREFIX = "lt:snap:sos:";
    /** Ordered STATUS/SOS history (a sorted set, score = sequence) and the highest-evicted marker. */
    private static final String CRIT_PREFIX = "lt:snap:crit:";
    private static final String CRIT_MIN_PREFIX = "lt:snap:critmin:";

    /**
     * Add a critical event to the ordered history, trim to capacity, and track the highest evicted
     * sequence (so {@code truncated} can be reported). KEYS[1]=sorted set, KEYS[2]=critmin;
     * ARGV[1]=seq, ARGV[2]="<seq>|<json>" member, ARGV[3]=capacity, ARGV[4]=ttl ms.
     */
    private static final String CRIT_ADD = """
            redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
            local over = redis.call('ZCARD', KEYS[1]) - tonumber(ARGV[3])
            while over > 0 do
                local popped = redis.call('ZPOPMIN', KEYS[1])
                local score = tonumber(popped[2])
                local curmin = tonumber(redis.call('GET', KEYS[2]) or '0')
                if score and score > curmin then redis.call('SET', KEYS[2], score) end
                over = over - 1
            end
            redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
            redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[4]))
            return 1
            """;

    /**
     * Overwrite field only if the new sequence is greater than the stored one, then refresh TTL.
     * KEYS[1]=hash key; ARGV[1]=field, ARGV[2]=new seq, ARGV[3]="<seq>|<json>" value, ARGV[4]=ttl ms.
     */
    private static final String MONOTONIC_WRITE = """
            local cur = redis.call('HGET', KEYS[1], ARGV[1])
            if cur then
                local bar = string.find(cur, '|')
                if bar then
                    local curseq = tonumber(string.sub(cur, 1, bar - 1))
                    if curseq and curseq >= tonumber(ARGV[2]) then
                        return 0
                    end
                end
            end
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
            redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]))
            return 1
            """;

    private final StringRedisTemplate redis;
    private final TripEventCodec codec;
    private final EventLog eventLog;
    private final Duration ttl;
    private final int criticalCapacity;
    private final RedisScript<Long> monotonicWrite;
    private final RedisScript<Long> critAdd;

    public RedisSnapshotStore(StringRedisTemplate redis, TripEventCodec codec,
                              EventLog eventLog, Duration ttl) {
        this(redis, codec, eventLog, ttl,
                com.moveinsync.livetrips.core.InMemorySnapshotStore.DEFAULT_CRITICAL_CAPACITY);
    }

    public RedisSnapshotStore(StringRedisTemplate redis, TripEventCodec codec,
                              EventLog eventLog, Duration ttl, int criticalCapacity) {
        this.redis = redis;
        this.codec = codec;
        this.eventLog = eventLog;
        this.ttl = ttl;
        this.criticalCapacity = criticalCapacity;
        this.monotonicWrite = new DefaultRedisScript<>(MONOTONIC_WRITE, Long.class);
        this.critAdd = new DefaultRedisScript<>(CRIT_ADD, Long.class);
    }

    @Override
    public void record(TripEvent event) {
        switch (event.type()) {
            case LOCATION -> {
                if (event.cabId() != null) {
                    writeMonotonic(LOC_PREFIX + event.topic(), event.cabId(), event);
                }
            }
            case STATUS -> {
                if (event.tripId() != null) {
                    writeMonotonic(STATUS_PREFIX + event.topic(), event.tripId(), event);
                }
            }
            // SOS is keyed by unique eventId: a plain set (idempotent, no overwrite race).
            case SOS -> writePlain(SOS_PREFIX + event.topic(), event.eventId(), event);
            case SNAPSHOT -> { /* derived state; never folded back in */ }
        }
        // STATUS and SOS also go into the ordered, bounded critical history for snapshot-path replay.
        if (event.type() == EventType.STATUS || event.type() == EventType.SOS) {
            redis.execute(critAdd,
                    List.of(CRIT_PREFIX + event.topic(), CRIT_MIN_PREFIX + event.topic()),
                    String.valueOf(event.sequence()), encodeValue(event),
                    String.valueOf(criticalCapacity), String.valueOf(ttl.toMillis()));
        }
    }

    @Override
    public CriticalHistory criticalEventsAfter(String topic, long lastSeq) {
        // Sequences are integers, so (lastSeq, +inf) is exactly [lastSeq + 1, +inf) inclusive.
        Set<String> raw = redis.opsForZSet().rangeByScore(
                CRIT_PREFIX + topic, (double) (lastSeq + 1), Double.POSITIVE_INFINITY);
        List<TripEvent> events = new ArrayList<>();
        if (raw != null) {
            for (String member : raw) {
                try {
                    events.add(decodeValue(member));
                } catch (RuntimeException ignored) {
                    // Skip a single corrupt member rather than failing the whole resume.
                }
            }
        }
        String min = redis.opsForValue().get(CRIT_MIN_PREFIX + topic);
        boolean truncated = min != null && Long.parseLong(min) > lastSeq;
        return CriticalHistory.of(events, truncated);
    }

    /** Value format shared by all hashes: "<seq>|<json>", so the Lua script can read the sequence. */
    private String encodeValue(TripEvent event) {
        return event.sequence() + "|" + codec.encode(event);
    }

    private void writeMonotonic(String key, String field, TripEvent event) {
        redis.execute(monotonicWrite, List.of(key),
                field, String.valueOf(event.sequence()), encodeValue(event),
                String.valueOf(ttl.toMillis()));
    }

    private void writePlain(String key, String field, TripEvent event) {
        redis.opsForHash().put(key, field, encodeValue(event));
        redis.expire(key, ttl);
    }

    @Override
    public TopicSnapshot snapshot(String topic) {
        Map<String, TripEvent> locations = readHash(LOC_PREFIX + topic);
        Map<String, TripEvent> statuses = readHash(STATUS_PREFIX + topic);
        List<TripEvent> sos = new ArrayList<>(readHash(SOS_PREFIX + topic).values());
        return new TopicSnapshot(topic, eventLog.currentSequence(topic), locations, statuses, sos);
    }

    private Map<String, TripEvent> readHash(String key) {
        Map<Object, Object> raw = redis.opsForHash().entries(key);
        Map<String, TripEvent> out = new LinkedHashMap<>();
        for (Map.Entry<Object, Object> e : raw.entrySet()) {
            try {
                out.put(e.getKey().toString(), decodeValue(e.getValue().toString()));
            } catch (RuntimeException ignored) {
                // A single corrupt field must not fail the whole snapshot.
            }
        }
        return out;
    }

    /** Strip the "<seq>|" prefix written by {@link #encodeValue} before decoding the JSON. */
    private TripEvent decodeValue(String raw) {
        int bar = raw.indexOf('|');
        return codec.decode(bar >= 0 ? raw.substring(bar + 1) : raw);
    }

    @Override
    public void clearSos(String topic, String eventId) {
        redis.opsForHash().delete(SOS_PREFIX + topic, eventId);
    }
}

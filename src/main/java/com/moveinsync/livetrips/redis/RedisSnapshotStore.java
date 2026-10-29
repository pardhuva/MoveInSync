package com.moveinsync.livetrips.redis;

import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.SnapshotStore;
import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.data.redis.core.StringRedisTemplate;

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
 * <h2>Complexity</h2>
 * {@code record}: O(1), one round trip. {@code snapshot}: three {@code HGETALL}s, O(cabs + trips
 * + open SOS) &mdash; paid only when a client falls outside the replay window.
 */
public class RedisSnapshotStore implements SnapshotStore {

    private static final String LOC_PREFIX = "lt:snap:loc:";
    private static final String STATUS_PREFIX = "lt:snap:st:";
    private static final String SOS_PREFIX = "lt:snap:sos:";

    private final StringRedisTemplate redis;
    private final TripEventCodec codec;
    private final EventLog eventLog;
    private final Duration ttl;

    public RedisSnapshotStore(StringRedisTemplate redis, TripEventCodec codec,
                              EventLog eventLog, Duration ttl) {
        this.redis = redis;
        this.codec = codec;
        this.eventLog = eventLog;
        this.ttl = ttl;
    }

    @Override
    public void record(TripEvent event) {
        switch (event.type()) {
            case LOCATION -> {
                if (event.cabId() != null) {
                    write(LOC_PREFIX + event.topic(), event.cabId(), event);
                }
            }
            case STATUS -> {
                if (event.tripId() != null) {
                    write(STATUS_PREFIX + event.topic(), event.tripId(), event);
                }
            }
            case SOS -> write(SOS_PREFIX + event.topic(), event.eventId(), event);
            case SNAPSHOT -> { /* derived state; never folded back in */ }
        }
    }

    private void write(String key, String field, TripEvent event) {
        redis.opsForHash().put(key, field, codec.encode(event));
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
                out.put(e.getKey().toString(), codec.decode(e.getValue().toString()));
            } catch (RuntimeException ignored) {
                // A single corrupt field must not fail the whole snapshot.
            }
        }
        return out;
    }

    @Override
    public void clearSos(String topic, String eventId) {
        redis.opsForHash().delete(SOS_PREFIX + topic, eventId);
    }
}

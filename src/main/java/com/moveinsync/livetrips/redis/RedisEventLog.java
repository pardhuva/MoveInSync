package com.moveinsync.livetrips.redis;

import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.ReplayResult;
import com.moveinsync.livetrips.model.TripEvent;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.ArrayList;
import java.util.List;

/**
 * Multi-node {@link EventLog} backed by Redis. This is what makes a node crash survivable:
 * the sequence counter and the replay window live outside the gateway process, so a client
 * that reconnects to a <i>different</i> node resumes from exactly where it left off.
 *
 * <h2>Keys</h2>
 * <ul>
 *   <li>{@code lt:seq:<topic>} &mdash; the per-topic sequence counter (a Redis string, {@code INCR}).</li>
 *   <li>{@code lt:log:<topic>} &mdash; the bounded replay window (a Redis list, trimmed to capacity).</li>
 * </ul>
 *
 * <h2>Atomicity</h2>
 * {@code INCR} + {@code RPUSH} + {@code LTRIM} run inside a single Lua script. Redis executes a
 * script atomically on its single-threaded command loop, so no two publishers can interleave and
 * leave the list unsorted by sequence. The sequence is also assigned by Redis, which means there
 * is exactly one sequencer for a topic across the whole cluster &mdash; no coordination between
 * gateway nodes is needed.
 *
 * <h2>Storage format</h2>
 * Each list element is {@code "<seq>|<json>"}. Keeping the sequence outside the JSON lets the
 * script stamp it without rewriting the document, and lets a reader recover the sequence without
 * parsing JSON.
 *
 * <h2>Complexity</h2>
 * {@code append}: O(1) Redis work, one network round trip. {@code replayFrom}: O(capacity) to read
 * and filter the window, one round trip; paid only on resume.
 */
public class RedisEventLog implements EventLog {

    private static final String SEQ_PREFIX = "lt:seq:";
    private static final String LOG_PREFIX = "lt:log:";

    /**
     * Assign the next sequence and append to the trimmed window, atomically.
     * KEYS[1]=seq key, KEYS[2]=log key; ARGV[1]=event json, ARGV[2]=capacity, ARGV[3]=ttl millis.
     */
    private static final String APPEND_SCRIPT = """
            local seq = redis.call('INCR', KEYS[1])
            redis.call('RPUSH', KEYS[2], seq .. '|' .. ARGV[1])
            redis.call('LTRIM', KEYS[2], -tonumber(ARGV[2]), -1)
            redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]))
            redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]))
            return seq
            """;

    private final StringRedisTemplate redis;
    private final TripEventCodec codec;
    private final int capacity;
    private final long ttlMillis;
    private final RedisScript<Long> appendScript;

    public RedisEventLog(StringRedisTemplate redis, TripEventCodec codec, int capacity, long ttlMillis) {
        this.redis = redis;
        this.codec = codec;
        this.capacity = capacity;
        this.ttlMillis = ttlMillis;
        this.appendScript = new DefaultRedisScript<>(APPEND_SCRIPT, Long.class);
    }

    @Override
    public TripEvent append(TripEvent event) {
        String json = codec.encode(event);
        Long seq = redis.execute(appendScript,
                List.of(SEQ_PREFIX + event.topic(), LOG_PREFIX + event.topic()),
                json, String.valueOf(capacity), String.valueOf(ttlMillis));
        if (seq == null) {
            throw new IllegalStateException("Redis did not return a sequence for topic " + event.topic());
        }
        return event.withSequence(seq);
    }

    @Override
    public ReplayResult replayFrom(String topic, long lastSequence) {
        long latest = currentSequence(topic);

        // The client claims to have seen more than the log has. Either the topic's state was
        // reclaimed or this is a stale token: a snapshot is the only safe answer.
        if (lastSequence > latest) {
            return ReplayResult.snapshotRequired(latest);
        }
        if (lastSequence == latest) {
            return ReplayResult.replay(List.of(), latest);
        }

        List<String> raw = redis.opsForList().range(LOG_PREFIX + topic, 0, -1);
        if (raw == null || raw.isEmpty()) {
            // Nothing retained but the client is behind: it must be re-seeded from a snapshot.
            return lastSequence < latest
                    ? ReplayResult.snapshotRequired(latest)
                    : ReplayResult.replay(List.of(), latest);
        }

        long oldestRetained = parseSequence(raw.get(0));
        if (lastSequence + 1 < oldestRetained) {
            return ReplayResult.snapshotRequired(latest);
        }

        List<TripEvent> missed = new ArrayList<>();
        for (String element : raw) {
            long seq = parseSequence(element);
            if (seq > lastSequence) {
                missed.add(decode(element, seq));
            }
        }
        return ReplayResult.replay(missed, latest);
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public long currentSequence(String topic) {
        String v = redis.opsForValue().get(SEQ_PREFIX + topic);
        return v == null ? 0L : Long.parseLong(v);
    }

    @Override
    public void deleteTopic(String topic) {
        redis.delete(List.of(SEQ_PREFIX + topic, LOG_PREFIX + topic));
    }

    private static long parseSequence(String element) {
        int bar = element.indexOf('|');
        if (bar <= 0) {
            throw new IllegalStateException("Malformed replay-log element");
        }
        return Long.parseLong(element.substring(0, bar));
    }

    private TripEvent decode(String element, long seq) {
        return codec.decode(element.substring(element.indexOf('|') + 1)).withSequence(seq);
    }
}

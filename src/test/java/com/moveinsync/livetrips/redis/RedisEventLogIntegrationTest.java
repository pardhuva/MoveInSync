package com.moveinsync.livetrips.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.core.ReplayResult;
import com.moveinsync.livetrips.model.TripEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.net.Socket;
import java.util.List;

import static com.moveinsync.livetrips.support.Events.rawLocation;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the real Redis-backed {@link RedisEventLog} against a live Redis.
 *
 * <p>This test is <b>conditionally enabled</b>: it runs only when a Redis is reachable at
 * {@code REDIS_HOST:REDIS_PORT} (default localhost:6379), and is cleanly skipped otherwise. So it
 * runs inside Docker Compose / CI (where Redis is up) and is skipped on a machine without Redis,
 * rather than failing there. It is what verifies the atomic Lua sequencing and the Redis replay
 * window for real; the in-process equivalents are covered by the always-on unit tests.
 */
@EnabledIf("redisAvailable")
class RedisEventLogIntegrationTest {

    private static final String HOST = System.getenv().getOrDefault("REDIS_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static RedisEventLog log;

    /** Condition method referenced by {@link EnabledIf}: true only if Redis accepts a connection. */
    @SuppressWarnings("unused")
    static boolean redisAvailable() {
        try (Socket s = new Socket(HOST, PORT)) {
            return s.isConnected();
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory(HOST, PORT);
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        redis.afterPropertiesSet();
        TripEventCodec codec = new TripEventCodec(new ObjectMapper());
        // Small window so the snapshot-required path is reachable quickly.
        log = new RedisEventLog(redis, codec, 5, 60_000);
    }

    @AfterAll
    static void disconnect() {
        if (factory != null) {
            factory.destroy();
        }
    }

    private String freshTopic() {
        return "trip:it-" + System.nanoTime();
    }

    @Test
    @DisplayName("append assigns gap-free increasing sequences via the atomic Lua script")
    void appendAssignsSequences() {
        String topic = freshTopic();
        try {
            for (long expected = 1; expected <= 10; expected++) {
                TripEvent stored = log.append(rawLocation(topic, "cab-1"));
                assertThat(stored.sequence()).isEqualTo(expected);
            }
            assertThat(log.currentSequence(topic)).isEqualTo(10L);
        } finally {
            log.deleteTopic(topic);
        }
    }

    @Test
    @DisplayName("replayFrom returns missed events within the window, and demands a snapshot outside it")
    void replayAndSnapshotBoundary() {
        String topic = freshTopic();
        try {
            for (int i = 0; i < 20; i++) {
                log.append(rawLocation(topic, "cab-1"));
            }
            // Window is 5, so only 16..20 are retained.
            ReplayResult inWindow = log.replayFrom(topic, 17);
            assertThat(inWindow.kind()).isEqualTo(ReplayResult.Kind.REPLAY);
            assertThat(inWindow.events()).extracting(TripEvent::sequence).containsExactly(18L, 19L, 20L);

            ReplayResult outOfWindow = log.replayFrom(topic, 3);
            assertThat(outOfWindow.isSnapshotRequired()).isTrue();

            ReplayResult caughtUp = log.replayFrom(topic, 20);
            assertThat(caughtUp.events()).isEmpty();
        } finally {
            log.deleteTopic(topic);
        }
    }

    @Test
    @DisplayName("concurrent appends to one topic stay gap-free and duplicate-free in Redis")
    void concurrentAppends() throws Exception {
        String topic = freshTopic();
        int threads = 8;
        int perThread = 200;
        try {
            List<Thread> workers = new java.util.ArrayList<>();
            java.util.concurrent.ConcurrentLinkedQueue<Long> seqs = new java.util.concurrent.ConcurrentLinkedQueue<>();
            for (int t = 0; t < threads; t++) {
                Thread w = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        seqs.add(log.append(rawLocation(topic, "cab-1")).sequence());
                    }
                });
                workers.add(w);
                w.start();
            }
            for (Thread w : workers) {
                w.join();
            }
            assertThat(seqs).hasSize(threads * perThread);
            assertThat(seqs.stream().distinct().count()).isEqualTo((long) threads * perThread);
            assertThat(log.currentSequence(topic)).isEqualTo((long) threads * perThread);
        } finally {
            log.deleteTopic(topic);
        }
    }
}

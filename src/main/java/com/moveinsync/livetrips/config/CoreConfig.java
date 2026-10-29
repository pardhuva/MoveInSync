package com.moveinsync.livetrips.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.livetrips.auth.JwtService;
import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.InMemoryEventLog;
import com.moveinsync.livetrips.core.InMemorySnapshotStore;
import com.moveinsync.livetrips.core.PublishAuthorizer;
import com.moveinsync.livetrips.core.SnapshotStore;
import com.moveinsync.livetrips.core.SubscriptionAuthorizer;
import com.moveinsync.livetrips.core.TripMetadataSource;
import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.distribution.InProcessEventDistributor;
import com.moveinsync.livetrips.gateway.FrameCodec;
import com.moveinsync.livetrips.service.CachingTripMetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wires the core, transport-agnostic beans.
 *
 * <p>The single-node implementations of {@link EventLog}, {@link SnapshotStore} and
 * {@link EventDistributor} are declared here under {@code @Profile("!redis")}; the Redis-backed
 * equivalents live in {@link RedisDistributionConfig} under {@code @Profile("redis")}. Choosing by
 * profile rather than by conditional-on-missing-bean keeps it obvious which implementation is
 * active in a given run.
 */
@Configuration
@EnableConfigurationProperties(LiveTripsProperties.class)
public class CoreConfig {

    private static final Logger log = LoggerFactory.getLogger(CoreConfig.class);

    @Bean
    public TripEventCodec tripEventCodec(ObjectMapper mapper) {
        return new TripEventCodec(mapper);
    }

    @Bean
    public FrameCodec frameCodec(ObjectMapper mapper, TripEventCodec eventCodec) {
        return new FrameCodec(mapper, eventCodec);
    }

    @Bean
    public JwtService jwtService(LiveTripsProperties props) {
        return new JwtService(props.getAuth().getJwtSecret(), props.getAuth().getTokenTtl());
    }

    /**
     * The authorizers deliberately depend on the <i>caching</i> metadata source by concrete type
     * rather than on {@link TripMetadataSource} by interface. Three beans implement that interface
     * (the repository, the caching decorator, and the decorator's own declaration), so injecting by
     * interface would be ambiguous. Naming the decorator here also makes the read path explicit:
     * swap it for {@code TripRepository} to take caching out of the authorisation path entirely.
     */
    @Bean
    public SubscriptionAuthorizer subscriptionAuthorizer(CachingTripMetadataService metadata) {
        return new SubscriptionAuthorizer(metadata);
    }

    @Bean
    public PublishAuthorizer publishAuthorizer(CachingTripMetadataService metadata) {
        return new PublishAuthorizer(metadata);
    }

    @Bean
    @Profile("!redis")
    public EventLog eventLog(LiveTripsProperties props) {
        log.info("Using in-process event log (single node), replay capacity {} events per topic",
                props.getReplay().getCapacity());
        return new InMemoryEventLog(props.getReplay().getCapacity());
    }

    @Bean
    @Profile("!redis")
    public SnapshotStore snapshotStore() {
        return new InMemorySnapshotStore();
    }

    @Bean
    @Profile("!redis")
    public EventDistributor eventDistributor(LiveTripsProperties props) {
        log.info("Using in-process event distributor (single node, no broker)");
        return new InProcessEventDistributor(props.getNodeId());
    }

    /**
     * The shared pool that drains per-connection queues to sockets.
     *
     * <p>Sized for concurrent <i>writers</i>, not for total connections: a connection only
     * occupies a thread while it actually has bytes to write. The queue in front of the pool is
     * bounded and the rejection policy is caller-runs, so under extreme load the offering thread
     * does the write itself rather than the pool growing without limit or work being silently
     * discarded.
     */
    @Bean(destroyMethod = "shutdown")
    public ThreadPoolExecutor writerPool(LiveTripsProperties props) {
        int threads = props.getGateway().getWriterThreads();
        AtomicLong counter = new AtomicLong();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                threads, threads,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10_000),
                r -> {
                    Thread t = new Thread(r, "ws-writer-" + counter.incrementAndGet());
                    t.setDaemon(true);
                    return t;
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        pool.allowCoreThreadTimeOut(true);
        log.info("WebSocket writer pool: {} threads", threads);
        return pool;
    }
}

package com.moveinsync.livetrips.config;

import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.SnapshotStore;
import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.redis.RedisEventDistributor;
import com.moveinsync.livetrips.redis.RedisEventLog;
import com.moveinsync.livetrips.redis.RedisSnapshotStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Multi-node mode. Active only under the {@code redis} profile
 * ({@code SPRING_PROFILES_ACTIVE=redis}), which is what {@code docker compose up} sets.
 *
 * <p>Replaces three beans with their shared-state equivalents. That substitution is the whole of
 * "scale out": nothing in the gateway, service or core layers changes, because they depend on
 * {@link EventLog}, {@link SnapshotStore} and {@link EventDistributor} as interfaces.
 *
 * <h2>Two listener containers, on purpose</h2>
 * SOS messages are consumed by their own container, which means their own Redis connection and
 * their own dispatch threads. If location traffic and SOS shared a container, a backlog of
 * location messages would sit in front of an SOS in the same consumer queue &mdash; precisely the
 * head-of-line blocking the priority-lane requirement forbids. Separating them makes the SOS path
 * independent at this hop.
 */
@Configuration
@Profile("redis")
public class RedisDistributionConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisDistributionConfig.class);

    @Bean
    public EventLog eventLog(StringRedisTemplate redis, TripEventCodec codec, LiveTripsProperties props) {
        log.info("Using Redis event log (multi-node), replay capacity {} events per topic, topic TTL {}",
                props.getReplay().getCapacity(), props.getReplay().getTopicTtl());
        return new RedisEventLog(redis, codec,
                props.getReplay().getCapacity(),
                props.getReplay().getTopicTtl().toMillis());
    }

    @Bean
    public SnapshotStore snapshotStore(StringRedisTemplate redis, TripEventCodec codec,
                                       EventLog eventLog, LiveTripsProperties props) {
        return new RedisSnapshotStore(redis, codec, eventLog, props.getReplay().getTopicTtl());
    }

    /** Container for ordinary (location, status) topic channels. */
    @Bean(name = "normalListenerContainer", destroyMethod = "destroy")
    public RedisMessageListenerContainer normalListenerContainer(RedisConnectionFactory factory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.setTaskExecutor(new SimpleAsyncTaskExecutor("redis-sub-"));
        container.afterPropertiesSet();
        container.start();
        return container;
    }

    /** Dedicated container (and therefore connection and threads) for the SOS priority lane. */
    @Bean(name = "sosListenerContainer", destroyMethod = "destroy")
    public RedisMessageListenerContainer sosListenerContainer(RedisConnectionFactory factory) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("redis-sos-");
        container.setTaskExecutor(executor);
        container.afterPropertiesSet();
        container.start();
        return container;
    }

    @Bean
    public EventDistributor eventDistributor(
            StringRedisTemplate redis,
            // Two beans share this type, so both are qualified explicitly rather than relying on
            // Spring's parameter-name fallback.
            @Qualifier("normalListenerContainer") RedisMessageListenerContainer normalListenerContainer,
            @Qualifier("sosListenerContainer") RedisMessageListenerContainer sosListenerContainer,
            TripEventCodec codec,
            LiveTripsProperties props) {
        log.info("Using Redis pub/sub event distributor (multi-node) as node {}", props.getNodeId());
        return new RedisEventDistributor(redis, normalListenerContainer, sosListenerContainer,
                codec, props.getNodeId());
    }
}

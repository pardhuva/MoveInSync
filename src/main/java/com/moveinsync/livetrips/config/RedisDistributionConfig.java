
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

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
        return new RedisSnapshotStore(redis, codec, eventLog, props.getReplay().getTopicTtl(),
                props.getReplay().getCriticalCapacity());
    }

    private static ThreadPoolTaskExecutor boundedDispatchPool(String threadPrefix, int threads,
                                                              int queueCapacity) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(threads);
        executor.setMaxPoolSize(threads);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(threadPrefix);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(5);
        executor.initialize();
        return executor;
    }

    @Bean(name = "redisNormalExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor redisNormalExecutor() {
        return boundedDispatchPool("redis-sub-", 8, 10_000);
    }

    @Bean(name = "redisSosExecutor", destroyMethod = "shutdown")
    public ThreadPoolTaskExecutor redisSosExecutor() {
        return boundedDispatchPool("redis-sos-", 4, 5_000);
    }

    @Bean(name = "normalListenerContainer", destroyMethod = "destroy")
    public RedisMessageListenerContainer normalListenerContainer(
            RedisConnectionFactory factory,
            @Qualifier("redisNormalExecutor") ThreadPoolTaskExecutor redisNormalExecutor) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.setTaskExecutor(redisNormalExecutor);
        return container;
    }

    @Bean(name = "sosListenerContainer", destroyMethod = "destroy")
    public RedisMessageListenerContainer sosListenerContainer(
            RedisConnectionFactory factory,
            @Qualifier("redisSosExecutor") ThreadPoolTaskExecutor redisSosExecutor) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.setTaskExecutor(redisSosExecutor);
        return container;
    }

    @Bean
    public EventDistributor eventDistributor(
            StringRedisTemplate redis,
            @Qualifier("normalListenerContainer") RedisMessageListenerContainer normalListenerContainer,
            @Qualifier("sosListenerContainer") RedisMessageListenerContainer sosListenerContainer,
            TripEventCodec codec,
            LiveTripsProperties props) {
        log.info("Using Redis pub/sub event distributor (multi-node) as node {}", props.getNodeId());
        return new RedisEventDistributor(redis, normalListenerContainer, sosListenerContainer,
                codec, props.getNodeId());
    }
}

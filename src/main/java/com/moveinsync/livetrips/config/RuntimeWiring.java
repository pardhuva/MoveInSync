package com.moveinsync.livetrips.config;

import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.distribution.InProcessEventDistributor;
import com.moveinsync.livetrips.gateway.ConnectionRegistry;
import com.moveinsync.livetrips.metrics.LiveTripsMetrics;
import com.moveinsync.livetrips.redis.RedisEventDistributor;
import com.moveinsync.livetrips.service.DeliveryService;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Connects the distribution backbone to local delivery, and keeps the periodic gauges fresh.
 *
 * <p>The backbone is deliberately not wired in a constructor: {@link DeliveryService} needs the
 * {@link ConnectionRegistry}, and the distributor needs the delivery callback, so wiring the
 * callback after both beans exist avoids a circular dependency and makes the startup order
 * explicit.
 */
@Component
@EnableScheduling
public class RuntimeWiring {

    private static final Logger log = LoggerFactory.getLogger(RuntimeWiring.class);

    private final EventDistributor distributor;
    private final DeliveryService deliveryService;
    private final ConnectionRegistry registry;
    private final LiveTripsMetrics metrics;

    public RuntimeWiring(EventDistributor distributor,
                         DeliveryService deliveryService,
                         ConnectionRegistry registry,
                         LiveTripsMetrics metrics) {
        this.distributor = distributor;
        this.deliveryService = deliveryService;
        this.registry = registry;
        this.metrics = metrics;
    }

    @PostConstruct
    public void wire() {
        distributor.start(deliveryService);
        log.info("Event distribution started on node {} using {}",
                distributor.nodeId(), distributor.describe());
    }

    /**
     * Queue depth and interest counts are O(connections) to compute, so they are sampled on a
     * schedule rather than maintained on the hot path. A one-second interval is well inside any
     * realistic scrape interval and costs a single pass over the connection map.
     */
    @Scheduled(fixedDelay = 1000)
    public void refreshGauges() {
        metrics.refreshGauges(registry, interestedTopicCount());
    }

    private long interestedTopicCount() {
        if (distributor instanceof InProcessEventDistributor inProcess) {
            return inProcess.interestedTopicCount();
        }
        if (distributor instanceof RedisEventDistributor redis) {
            return redis.subscribedTopicCount();
        }
        return registry.topicCount();
    }
}

package com.moveinsync.livetrips.metrics;

import com.moveinsync.livetrips.core.AuthorizationDecision;
import com.moveinsync.livetrips.gateway.ConnectionRegistry;
import com.moveinsync.livetrips.model.EventType;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * All instrumentation in one place, exported at {@code /actuator/prometheus}.
 *
 * <h2>What an on-call engineer actually needs at 3 AM</h2>
 * <table border="1">
 *   <caption>Key signals</caption>
 *   <tr><th>Question</th><th>Metric</th></tr>
 *   <tr><td>Are SOS alerts arriving in time?</td>
 *       <td>{@code livetrips.delivery.latency{type=SOS}} p99 &mdash; the 2-second SLO</td></tr>
 *   <tr><td>Is anyone falling behind?</td>
 *       <td>{@code livetrips.queue.depth} (gauge), {@code livetrips.events.conflated}</td></tr>
 *   <tr><td>Are we dropping clients?</td>
 *       <td>{@code livetrips.slow.consumer.disconnects}, {@code livetrips.connections.closed{reason}}</td></tr>
 *   <tr><td>Is something probing us?</td>
 *       <td>{@code livetrips.subscribe.denied{reason}}, {@code livetrips.auth.failures}</td></tr>
 *   <tr><td>Are clients resyncing abnormally often?</td>
 *       <td>{@code livetrips.resume{mode=SNAPSHOT}} vs {@code mode=REPLAY}</td></tr>
 *   <tr><td>Is the socket layer healthy?</td>
 *       <td>{@code livetrips.write.failures}, {@code livetrips.connections.active}</td></tr>
 * </table>
 *
 * <p>No metric carries a user id, trip id, cab id or location as a tag: unbounded tag values
 * would both explode cardinality and leak who is where.
 */
@Component
public class LiveTripsMetrics {

    private final MeterRegistry registry;

    private final Map<EventType, Counter> published = new EnumMap<>(EventType.class);
    private final Map<EventType, Counter> delivered = new EnumMap<>(EventType.class);
    private final Map<EventType, Timer> deliveryLatency = new EnumMap<>(EventType.class);

    private final Counter conflated;
    private final Counter droppedLocations;
    private final Counter slowConsumerDisconnects;
    private final Counter writeFailures;
    private final Counter authFailures;
    private final Counter subscribeOk;
    private final Counter publishRejected;
    private final Counter connectionsOpened;
    private final Counter criticalHistoryTruncated;
    private final Counter sosExpressDeliveries;
    private final Counter orderedDispatchDropped;

    private final Map<String, Counter> subscribeDenied = new ConcurrentHashMap<>();
    private final Map<String, Counter> connectionsClosed = new ConcurrentHashMap<>();
    private final Map<String, Counter> resumeByMode = new ConcurrentHashMap<>();

    private final AtomicLong activeConnections = new AtomicLong();
    private final AtomicLong queueDepth = new AtomicLong();
    private final AtomicLong interestedTopics = new AtomicLong();

    public LiveTripsMetrics(MeterRegistry registry) {
        this.registry = registry;

        for (EventType type : EventType.values()) {
            published.put(type, Counter.builder("livetrips.events.published")
                    .tag("type", type.name()).register(registry));
            delivered.put(type, Counter.builder("livetrips.events.delivered")
                    .tag("type", type.name()).register(registry));
            deliveryLatency.put(type, Timer.builder("livetrips.delivery.latency")
                    .tag("type", type.name())
                    .publishPercentiles(0.5, 0.95, 0.99)
                    .publishPercentileHistogram()
                    .maximumExpectedValue(Duration.ofSeconds(10))
                    .register(registry));
        }

        this.conflated = Counter.builder("livetrips.events.conflated")
                .description("Location updates replaced by a newer one because the consumer lagged")
                .register(registry);
        this.droppedLocations = Counter.builder("livetrips.events.dropped.location")
                .description("Location updates discarded under back-pressure (never status or SOS)")
                .register(registry);
        this.slowConsumerDisconnects = Counter.builder("livetrips.slow.consumer.disconnects")
                .description("Connections closed because a critical event could not be queued")
                .register(registry);
        this.writeFailures = Counter.builder("livetrips.write.failures").register(registry);
        this.authFailures = Counter.builder("livetrips.auth.failures").register(registry);
        this.subscribeOk = Counter.builder("livetrips.subscribe.ok").register(registry);
        this.publishRejected = Counter.builder("livetrips.publish.rejected").register(registry);
        this.connectionsOpened = Counter.builder("livetrips.connections.opened").register(registry);
        this.criticalHistoryTruncated = Counter.builder("livetrips.resume.critical.truncated")
                .description("Resumes where some STATUS/SOS were older than the critical-history horizon")
                .register(registry);
        this.sosExpressDeliveries = Counter.builder("livetrips.sos.express.deliveries")
                .description("SOS frames flushed via the dedicated express writer path")
                .register(registry);
        this.orderedDispatchDropped = Counter.builder("livetrips.dispatch.dropped")
                .description("Live dispatches dropped under ordered-dispatcher saturation (recoverable via resume)")
                .register(registry);

        Gauge.builder("livetrips.connections.active", activeConnections, AtomicLong::get)
                .description("Currently connected subscribers on this node").register(registry);
        Gauge.builder("livetrips.queue.depth", queueDepth, AtomicLong::get)
                .description("Total queued-but-unsent events across all connections").register(registry);
        Gauge.builder("livetrips.topics.interested", interestedTopics, AtomicLong::get)
                .description("Distinct topics with at least one local subscriber").register(registry);
    }

    // ---------- publish / delivery ----------

    public void published(EventType type) {
        published.get(type).increment();
    }

    public void publishRejected() {
        publishRejected.increment();
    }

    /**
     * Record one event written to one socket, with the latency from the producer's timestamp.
     * This is the measurement behind the SOS p99 claim.
     */
    public void deliveredToClient(EventType type, long originTimestampMs) {
        delivered.get(type).increment();
        long latency = System.currentTimeMillis() - originTimestampMs;
        if (latency >= 0) {
            deliveryLatency.get(type).record(latency, TimeUnit.MILLISECONDS);
        }
    }

    public void conflated() {
        conflated.increment();
    }

    public void droppedLocation() {
        droppedLocations.increment();
    }

    // ---------- connections ----------

    public void connectionOpened() {
        connectionsOpened.increment();
        activeConnections.incrementAndGet();
    }

    public void connectionClosed(String reason) {
        activeConnections.decrementAndGet();
        connectionsClosed.computeIfAbsent(reason == null ? "UNKNOWN" : reason,
                r -> Counter.builder("livetrips.connections.closed").tag("reason", r).register(registry))
                .increment();
    }

    public void slowConsumerDisconnect() {
        slowConsumerDisconnects.increment();
    }

    public void writeFailure() {
        writeFailures.increment();
    }

    // ---------- auth / subscribe ----------

    public void authFailure() {
        authFailures.increment();
    }

    public void subscribeAllowed() {
        subscribeOk.increment();
    }

    public void subscribeDenied(AuthorizationDecision.DenyReason reason) {
        subscribeDenied.computeIfAbsent(reason.name(),
                r -> Counter.builder("livetrips.subscribe.denied").tag("reason", r).register(registry))
                .increment();
    }

    /** mode is REPLAY, SNAPSHOT or LIVE. A rising SNAPSHOT rate means the replay window is too small. */
    public void resume(String mode) {
        resumeByMode.computeIfAbsent(mode,
                m -> Counter.builder("livetrips.resume").tag("mode", m).register(registry))
                .increment();
    }

    /** A resume where the critical (STATUS/SOS) history had overflowed for the client's horizon. */
    public void criticalHistoryTruncated() {
        criticalHistoryTruncated.increment();
    }

    /** An SOS frame flushed through the dedicated express writer path. */
    public void sosExpressDelivery() {
        sosExpressDeliveries.increment();
    }

    /** A live dispatch dropped because the ordered-dispatcher queue was saturated (recoverable). */
    public void orderedDispatchDropped() {
        orderedDispatchDropped.increment();
    }

    // ---------- periodic gauges ----------

    /** Called on a schedule to refresh gauges that are expensive to maintain per event. */
    public void refreshGauges(ConnectionRegistry registry, long interestedTopicCount) {
        queueDepth.set(registry.totalQueueDepth());
        interestedTopics.set(interestedTopicCount);
        activeConnections.set(registry.connectionCount());
    }

    /** Exposed for the load-test harness and tests to read the measured distribution. */
    public Timer deliveryLatencyTimer(EventType type) {
        return deliveryLatency.get(type);
    }
}

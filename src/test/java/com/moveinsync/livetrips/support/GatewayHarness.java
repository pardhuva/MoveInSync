package com.moveinsync.livetrips.support;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.InMemoryEventLog;
import com.moveinsync.livetrips.core.InMemorySnapshotStore;
import com.moveinsync.livetrips.core.OutboundQueue;
import com.moveinsync.livetrips.core.PublishAuthorizer;
import com.moveinsync.livetrips.core.SnapshotStore;
import com.moveinsync.livetrips.core.SubscriptionAuthorizer;
import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.distribution.OrderedDispatcher;
import com.moveinsync.livetrips.gateway.ClientConnection;
import com.moveinsync.livetrips.gateway.ConnectionRegistry;
import com.moveinsync.livetrips.gateway.FrameCodec;
import com.moveinsync.livetrips.metrics.LiveTripsMetrics;
import com.moveinsync.livetrips.repository.InMemoryTripRepository;
import com.moveinsync.livetrips.repository.TripRepository;
import com.moveinsync.livetrips.service.CachingTripMetadataService;
import com.moveinsync.livetrips.service.DeliveryService;
import com.moveinsync.livetrips.service.PublishService;
import com.moveinsync.livetrips.service.SubscriptionService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Wires a full gateway stack in-process so integration tests exercise the real publish -> event
 * log -> distributor -> delivery -> queue -> transport path, with no Spring, Redis or network.
 *
 * <p>A test can also build two harnesses that share one {@link EventLog} and {@link SnapshotStore}
 * via {@link #sharingBackbone}, which is exactly the shape of two gateway nodes sharing Redis: the
 * basis of the node-crash recovery test.
 */
public class GatewayHarness {

    public final TripRepository repository;
    public final CachingTripMetadataService metadata;
    public final EventLog eventLog;
    public final SnapshotStore snapshotStore;
    public final EventDistributor distributor;
    public final ConnectionRegistry registry;
    public final DeliveryService deliveryService;
    public final SubscriptionService subscriptionService;
    public final PublishService publishService;
    public final OrderedDispatcher orderedDispatcher;
    public final FrameCodec codec = new FrameCodec();
    public final LiveTripsMetrics metrics;
    private final ExecutorService writerPool;
    private final ExecutorService sosWriterPool;
    private final int queueCapacity;
    private final MultiNodeBus bus;
    private volatile boolean alive = true;

    public GatewayHarness() {
        this(new InMemoryEventLog(500), new InMemorySnapshotStore(), new InMemoryTripRepository(),
                "node-A", 1000, new MultiNodeBus());
    }

    public GatewayHarness(int queueCapacity) {
        this(new InMemoryEventLog(500), new InMemorySnapshotStore(), new InMemoryTripRepository(),
                "node-A", queueCapacity, new MultiNodeBus());
    }

    /** Harness with a specific (e.g. tiny) replay window, to force the snapshot-required path. */
    public static GatewayHarness withReplayWindow(int replayWindow, int queueCapacity) {
        return new GatewayHarness(new InMemoryEventLog(replayWindow), new InMemorySnapshotStore(),
                new InMemoryTripRepository(), "node-A", queueCapacity, new MultiNodeBus());
    }

    /**
     * Harness with a tiny replay window AND a tiny critical-history capacity, to exercise both the
     * snapshot path and critical-history overflow (truncation).
     */
    public static GatewayHarness withReplayAndCriticalWindow(int replayWindow, int criticalCapacity,
                                                             int queueCapacity) {
        return new GatewayHarness(new InMemoryEventLog(replayWindow),
                new InMemorySnapshotStore(criticalCapacity),
                new InMemoryTripRepository(), "node-A", queueCapacity, new MultiNodeBus());
    }

    private GatewayHarness(EventLog eventLog, SnapshotStore snapshotStore, TripRepository repository,
                           String nodeId, int queueCapacity, MultiNodeBus bus) {
        this.eventLog = eventLog;
        this.snapshotStore = snapshotStore;
        this.repository = repository;
        this.queueCapacity = queueCapacity;
        this.metadata = new CachingTripMetadataService(repository, 10_000, 30_000);
        this.metrics = new LiveTripsMetrics(new SimpleMeterRegistry());
        this.registry = new ConnectionRegistry();
        this.bus = bus;
        this.distributor = bus;
        this.deliveryService = new DeliveryService(registry, metrics);
        this.subscriptionService = new SubscriptionService(registry,
                new SubscriptionAuthorizer(metadata), eventLog, snapshotStore, distributor,
                codec, metrics, 50);
        // Ordered live dispatch, as in production. Small partition count keeps the harness light;
        // a topic still always maps to one partition, which is what preserves per-topic order.
        this.orderedDispatcher = new OrderedDispatcher(distributor,
                Integer.getInteger("harness.dispatchPartitions", 16), 50_000,
                metrics::orderedDispatchDropped);
        this.publishService = new PublishService(eventLog, snapshotStore, orderedDispatcher, repository,
                new PublishAuthorizer(metadata), metrics, true);
        this.writerPool = Executors.newFixedThreadPool(8, r -> {
            Thread t = new Thread(r, "harness-writer");
            t.setDaemon(true);
            return t;
        });
        this.sosWriterPool = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "harness-sos-writer");
            t.setDaemon(true);
            return t;
        });
        // Register this node's delivery callback on the shared bus: a publish on any node
        // that shares this bus is broadcast here and filtered by this node's registry.
        bus.start(deliveryService);
    }

    /** A second gateway "node" that shares this one's event log and snapshot store (as Redis would). */
    public GatewayHarness sharingBackbone(String nodeId) {
        return new GatewayHarness(this.eventLog, this.snapshotStore, this.repository, nodeId,
                queueCapacity, this.bus);
    }

    /** Open a connection backed by the given transport and attach the registry cleanup. */
    public ClientConnection connect(RecordingTransport transport, AuthenticatedPrincipal principal) {
        ClientConnection c = newConnection(transport);
        c.authenticate(principal);
        return c;
    }

    public ClientConnection newConnection(com.moveinsync.livetrips.gateway.ClientTransport transport) {
        ClientConnection c = new ClientConnection(transport, new OutboundQueue(queueCapacity),
                codec, writerPool,
                Boolean.getBoolean("harness.sharedSosPool") ? writerPool : sosWriterPool,
                deliveryService);
        c.onClose(subscriptionService::disconnect);
        registry.register(c);
        return c;
    }

    public Executor writerPool() {
        return writerPool;
    }

    public boolean isAlive() {
        return alive;
    }

    /**
     * Simulate a node crash: mark the node dead, drop every connection it holds (so clients must
     * reconnect elsewhere) and stop its writer pool. The shared event log and snapshot store are
     * untouched, which is what lets reconnecting clients resume losslessly on another node.
     */
    public void kill() {
        alive = false;
        for (ClientConnection c : registry.allConnections()) {
            c.close("NODE_CRASH");
        }
        writerPool.shutdownNow();
        sosWriterPool.shutdownNow();
        orderedDispatcher.shutdown();
    }

    public void shutdown() {
        orderedDispatcher.shutdown();
        sosWriterPool.shutdownNow();
        writerPool.shutdownNow();
    }
}

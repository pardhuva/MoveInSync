package com.moveinsync.livetrips.service;

import com.moveinsync.livetrips.core.OutboundQueue;
import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.gateway.ClientConnection;
import com.moveinsync.livetrips.gateway.ConnectionObserver;
import com.moveinsync.livetrips.gateway.ConnectionRegistry;
import com.moveinsync.livetrips.metrics.LiveTripsMetrics;
import com.moveinsync.livetrips.model.TripEvent;
import org.springframework.stereotype.Service;

/**
 * Local fan-out: takes an event that arrived from the distribution backbone and offers it to every
 * connection on <i>this</i> node that subscribes to its topic.
 *
 * <h2>The isolation property</h2>
 * This method is the one place where one client could hurt another, so it is written to make that
 * impossible: it only ever calls {@link ClientConnection#offer}, which is non-blocking, and it
 * never touches a socket. A client that is frozen, on a lossy network, or 10,000 events behind
 * absorbs the consequence inside its own bounded queue &mdash; conflation, or in the worst case its
 * own disconnection &mdash; while the loop moves on to the next subscriber in constant time.
 *
 * <h2>Complexity</h2>
 * O(subscribers on this node for the topic) per event, with a single hash lookup to find them.
 * This is the irreducible cost of fan-out: delivering one event to N watchers is N offers. It is
 * also where the system stops scaling vertically &mdash; see the complexity notes in the docs.
 */
@Service
public class DeliveryService implements EventDistributor.EventListener, ConnectionObserver {

    private final ConnectionRegistry registry;
    private final LiveTripsMetrics metrics;

    public DeliveryService(ConnectionRegistry registry, LiveTripsMetrics metrics) {
        this.registry = registry;
        this.metrics = metrics;
    }

    @Override
    public void onEvent(TripEvent event) {
        for (ClientConnection connection : registry.subscribers(event.topic())) {
            OutboundQueue.OfferOutcome outcome = connection.offer(event);
            switch (outcome) {
                case CONFLATED -> metrics.conflated();
                case DROPPED_LOCATION -> metrics.droppedLocation();
                case REJECTED_OVERFLOW -> metrics.slowConsumerDisconnect();
                case ENQUEUED -> { /* the normal path */ }
            }
        }
    }

    // ---- ConnectionObserver: instrumentation of what actually reached a socket ----

    @Override
    public void onDelivered(TripEvent event) {
        metrics.deliveredToClient(event.type(), event.originTimestampMs());
    }

    @Override
    public void onWriteFailure() {
        metrics.writeFailure();
    }

    @Override
    public void onSosExpressDelivery() {
        metrics.sosExpressDelivery();
    }
}

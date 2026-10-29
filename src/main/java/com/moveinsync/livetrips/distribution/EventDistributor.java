package com.moveinsync.livetrips.distribution;

import com.moveinsync.livetrips.model.TripEvent;

/**
 * Carries a published event to every gateway node that holds a subscriber for its topic.
 *
 * <h2>The question this answers</h2>
 * <i>"Connections are spread across several nodes. Explain how an event reaches the node that
 * holds a given subscriber."</i>
 *
 * <p>The design uses <b>subscription-driven routing</b>: a node declares interest in a topic
 * ({@link #onLocalSubscribe}) the moment its first local client subscribes to it, and drops that
 * interest when its last local client goes away. The backbone then delivers each published event
 * only to the nodes that declared interest. There is no central "which node holds whom" table to
 * keep consistent &mdash; interest is the routing table, and it is maintained by the same events
 * that create and destroy connections.
 *
 * <p>Two implementations:
 * <ul>
 *   <li>{@link InProcessEventDistributor} &mdash; single node; the "backbone" is a direct method call.
 *       Default, and what the unit/integration tests use.</li>
 *   <li>{@code RedisEventDistributor} &mdash; multi-node; Redis Pub/Sub channels are the backbone and
 *       Redis's own subscription registry is the routing table. Active under the {@code redis} profile.</li>
 * </ul>
 */
public interface EventDistributor {

    /** Receives events that this node must deliver to its local subscribers. */
    interface EventListener {
        void onEvent(TripEvent event);
    }

    /** Wire up the local delivery callback. Called once at startup before any publish. */
    void start(EventListener listener);

    /** Distribute an already-sequenced event to all interested nodes, including this one. */
    void publish(TripEvent event);

    /** This node now holds at least one subscriber for {@code topic}; start receiving its events. */
    void onLocalSubscribe(String topic);

    /** This node no longer holds any subscriber for {@code topic}; stop receiving its events. */
    void onLocalUnsubscribe(String topic);

    /** Stable identifier for this gateway node, used in logs and metrics. */
    String nodeId();

    /** Human-readable description of the backbone, for the health endpoint. */
    String describe();
}

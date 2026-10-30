package com.moveinsync.livetrips.support;

import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.model.TripEvent;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An in-process stand-in for the Redis pub/sub backbone, used by multi-node tests and the load
 * simulation. Every gateway node registers its delivery callback here; a publish on any node is
 * broadcast to all registered nodes, exactly as a Redis PUBLISH reaches every subscribed node.
 *
 * <p>Each node's delivery service still filters by its own connection registry, so a node only
 * writes to sockets it actually holds. This is what gives the load test honest cross-node fan-out
 * without standing up Redis.
 */
public class MultiNodeBus implements EventDistributor {

    private final List<EventListener> listeners = new CopyOnWriteArrayList<>();

    @Override
    public void start(EventListener listener) {
        listeners.add(listener);
    }

    @Override
    public void publish(TripEvent event) {
        for (EventListener l : listeners) {
            l.onEvent(event);
        }
    }

    @Override
    public void onLocalSubscribe(String topic) {
        // Interest filtering is unnecessary in-process: delivery filters by registry membership.
    }

    @Override
    public void onLocalUnsubscribe(String topic) {
    }

    @Override
    public String nodeId() {
        return "multi-node-bus";
    }

    @Override
    public String describe() {
        return "in-process multi-node bus (" + listeners.size() + " nodes)";
    }
}

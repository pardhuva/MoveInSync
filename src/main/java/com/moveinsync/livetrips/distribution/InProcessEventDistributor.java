package com.moveinsync.livetrips.distribution;

import com.moveinsync.livetrips.model.TripEvent;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-node {@link EventDistributor}: publishing hands the event straight to the local
 * delivery callback.
 *
 * <p>This is the default so the service runs, and every guarantee can be tested, with no broker
 * and no Docker. The multi-node behaviour it stands in for is exercised by
 * {@code TwoNodeFailoverTest}, which wires two gateway instances over a shared distributor and
 * shared replay buffer to reproduce a node crash.
 *
 * <p>Interest tracking is kept even though it is not needed for routing here, so the two
 * implementations behave identically from the caller's point of view and the "interested topics"
 * metric means the same thing in both modes.
 */
public class InProcessEventDistributor implements EventDistributor {

    private final String nodeId;
    private final Set<String> interestedTopics = ConcurrentHashMap.newKeySet();
    private volatile EventListener listener;

    public InProcessEventDistributor(String nodeId) {
        this.nodeId = nodeId;
    }

    @Override
    public void start(EventListener listener) {
        this.listener = listener;
    }

    @Override
    public void publish(TripEvent event) {
        EventListener l = this.listener;
        if (l != null) {
            l.onEvent(event);
        }
    }

    @Override
    public void onLocalSubscribe(String topic) {
        interestedTopics.add(topic);
    }

    @Override
    public void onLocalUnsubscribe(String topic) {
        interestedTopics.remove(topic);
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public String describe() {
        return "in-process (single node, no broker)";
    }

    public int interestedTopicCount() {
        return interestedTopics.size();
    }
}

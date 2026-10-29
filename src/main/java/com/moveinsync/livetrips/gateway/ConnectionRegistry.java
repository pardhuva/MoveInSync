package com.moveinsync.livetrips.gateway;

import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This node's view of who is connected and what they watch.
 *
 * <p>Two indexes:
 * <ul>
 *   <li><b>by connection id</b> &mdash; for lifecycle (close, lookup).</li>
 *   <li><b>by topic</b> &mdash; the fan-out index. Delivering an event is one hash lookup plus an
 *       iteration over that topic's local subscribers, which is the irreducible cost of fan-out.</li>
 * </ul>
 *
 * <p>{@link #subscribe} and {@link #unsubscribe} report whether the node's interest in a topic
 * just began or ended. That boolean is what drives {@code onLocalSubscribe}/{@code onLocalUnsubscribe}
 * on the distribution backbone, so a node is subscribed to a topic's channel exactly while it has
 * at least one local client for it. The detection is done inside
 * {@link ConcurrentHashMap#compute} so concurrent subscribes cannot both believe they were first.
 *
 * <p>Complexity: register/unregister/subscribe/unsubscribe are O(1); {@link #subscribers} is O(1)
 * to look up and O(n) to iterate.
 */
@Component
public class ConnectionRegistry {

    private final Map<String, ClientConnection> byId = new ConcurrentHashMap<>();
    private final Map<String, Set<ClientConnection>> byTopic = new ConcurrentHashMap<>();

    public void register(ClientConnection connection) {
        byId.put(connection.id(), connection);
    }

    public ClientConnection get(String connectionId) {
        return byId.get(connectionId);
    }

    /**
     * Remove a connection and all of its subscriptions.
     *
     * @return the topics for which this node no longer holds any subscriber, so the caller can
     *         drop its interest on the backbone
     */
    public Set<String> unregister(ClientConnection connection) {
        byId.remove(connection.id());
        Set<String> nowEmpty = new HashSet<>();
        for (String topic : connection.topics()) {
            if (unsubscribe(connection, topic)) {
                nowEmpty.add(topic);
            }
        }
        return nowEmpty;
    }

    /** @return true if this node had no subscriber for {@code topic} before this call. */
    public boolean subscribe(ClientConnection connection, String topic) {
        boolean[] first = {false};
        byTopic.compute(topic, (key, set) -> {
            if (set == null) {
                set = ConcurrentHashMap.newKeySet();
                first[0] = true;
            }
            set.add(connection);
            return set;
        });
        connection.addTopic(topic);
        return first[0];
    }

    /** @return true if this removed the node's last subscriber for {@code topic}. */
    public boolean unsubscribe(ClientConnection connection, String topic) {
        boolean[] last = {false};
        byTopic.compute(topic, (key, set) -> {
            if (set == null) {
                return null;
            }
            set.remove(connection);
            if (set.isEmpty()) {
                last[0] = true;
                return null; // drop the empty set so the map does not leak topics
            }
            return set;
        });
        connection.removeTopic(topic);
        return last[0];
    }

    /** Local subscribers of a topic. Never null; safe to iterate while others mutate. */
    public Collection<ClientConnection> subscribers(String topic) {
        Set<ClientConnection> set = byTopic.get(topic);
        return set == null ? Collections.emptySet() : set;
    }

    public int connectionCount() {
        return byId.size();
    }

    /** Number of distinct topics with at least one local subscriber. */
    public int topicCount() {
        return byTopic.size();
    }

    public Collection<ClientConnection> allConnections() {
        return Collections.unmodifiableCollection(byId.values());
    }

    /** Total queued-but-unsent events across all connections &mdash; the on-call "queue depth" gauge. */
    public long totalQueueDepth() {
        long sum = 0;
        for (ClientConnection c : byId.values()) {
            sum += c.queue().size();
        }
        return sum;
    }
}

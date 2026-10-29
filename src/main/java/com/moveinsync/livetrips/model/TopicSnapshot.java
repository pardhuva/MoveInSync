package com.moveinsync.livetrips.model;

import java.util.List;
import java.util.Map;

/**
 * The current state of a topic, sent to a client that cannot be served from the bounded
 * replay buffer (it was away too long) or that is subscribing for the first time.
 *
 * <p>A snapshot is "where everything is right now": the latest known position of every cab
 * in the topic and the latest status of every trip in it, stamped with the sequence number
 * it is current as of. The client applies the snapshot, sets its last-seen sequence to
 * {@link #atSequence()}, and then continues with live events &mdash; so no event is processed
 * twice and none is missed.
 */
public final class TopicSnapshot {

    private final String topic;
    private final long atSequence;
    private final Map<String, TripEvent> latestLocationByCab;
    private final Map<String, TripEvent> latestStatusByTrip;
    private final List<TripEvent> activeSosAlerts;

    public TopicSnapshot(String topic,
                         long atSequence,
                         Map<String, TripEvent> latestLocationByCab,
                         Map<String, TripEvent> latestStatusByTrip,
                         List<TripEvent> activeSosAlerts) {
        this.topic = topic;
        this.atSequence = atSequence;
        this.latestLocationByCab = Map.copyOf(latestLocationByCab);
        this.latestStatusByTrip = Map.copyOf(latestStatusByTrip);
        this.activeSosAlerts = List.copyOf(activeSosAlerts);
    }

    public String topic() { return topic; }

    /** The client's last-seen sequence after applying this snapshot. */
    public long atSequence() { return atSequence; }

    public Map<String, TripEvent> latestLocationByCab() { return latestLocationByCab; }
    public Map<String, TripEvent> latestStatusByTrip() { return latestStatusByTrip; }

    /**
     * SOS alerts still considered active for this topic. Included so a reconnecting client
     * can never miss an in-flight emergency, even if the originating event was evicted
     * from the replay window.
     */
    public List<TripEvent> activeSosAlerts() { return activeSosAlerts; }
}
